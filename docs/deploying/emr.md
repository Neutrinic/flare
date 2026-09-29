# Amazon EMR

Verified on EMR 7.14.0 (Spark 3.5.8, Scala 2.12, Java 17), one primary and two core nodes, in both
cluster and client mode, with export to logs and to Grafana Cloud. EMR's Spark build does not
affect how task spans parent to their stage.

EMR 7.x ships Spark 3.5, so use `flare-spark-3-5_2.12`. There is no Spark 4 on EMR yet.

## Cluster mode

Steps and `spark-submit --deploy-mode cluster` run the driver inside YARN, so the
[YARN recipe](yarn.md) works unchanged: `--files` with the JAR URLs and relative `./` paths.
Nothing is installed on the nodes. You still need to [append to the classpath](#append-to-the-classpath).

## Client mode

Notebooks, EMR Studio and `spark-shell` on the primary node run the driver outside YARN, where
`--files` never reaches it. Use a **bootstrap action** that fetches both JARs to a fixed path on
every node before YARN starts:

```bash
#!/bin/bash
set -euo pipefail
--8<-- "urls.sh"

sudo mkdir -p /opt/flare
sudo curl -fsSL -o /opt/flare/opentelemetry-javaagent.jar "$AGENT"
sudo curl -fsSL -o /opt/flare/flare-spark.jar "$FLARE"
sudo chmod 644 /opt/flare/*.jar
```

Then use absolute `/opt/flare/...` paths in the job.

## Append to the classpath

EMR sets `spark.{driver,executor}.extraClassPath` itself: `hadoop-aws`, the AWS SDKs, EMRFS and an
OpenLineage listener. A bare `--conf spark.executor.extraClassPath=/opt/flare/flare-spark.jar`
**replaces** all of that and breaks `s3://` access. Read the current value and append:

```bash
CONF=/etc/spark/conf/spark-defaults.conf
get() { awk -v k="$1" '$1==k { sub(/^[^ \t]+[ \t]+/, ""); print; exit }' "$CONF"; }

spark-submit \
  --conf "spark.driver.extraClassPath=$(get spark.driver.extraClassPath):/opt/flare/flare-spark.jar" \
  --conf "spark.executor.extraClassPath=$(get spark.executor.extraClassPath):/opt/flare/flare-spark.jar" \
  ...
```

`extraJavaOptions` is safe to set directly. It is empty on EMR 7.14; EMR's own flags are in
`spark.{driver,executor}.defaultJavaOptions`, which Spark prepends on its own.

## Exporting to a hosted backend

Put the exporter credentials in an agent configuration file written by the bootstrap action, not in
Spark config: see [Credentials](../configuration/exporting.md#credentials). On EMR the natural
source is SSM Parameter Store. The instance role (`EMR_EC2_DefaultRole` by default) needs
`ssm:GetParameter` on that one parameter; the default `aws/ssm` key needs no extra KMS grant.

```bash
# In the bootstrap action. The parameter holds the whole header value, for Grafana Cloud
# Authorization=Basic%20<base64 of instanceId:token>. Never echo it: bootstrap output goes to S3.
V=$(aws ssm get-parameter --region <region> --name /flare/otlp-headers --with-decryption \
      --query Parameter.Value --output text)
{
  echo "otel.exporter.otlp.endpoint=https://otlp-gateway-<region>.grafana.net/otlp"
  echo "otel.exporter.otlp.protocol=http/protobuf"
  echo "otel.exporter.otlp.headers=$V"
} | sudo tee /opt/flare/otel.properties >/dev/null
sudo chmod 644 /opt/flare/otel.properties
unset V
```

`0644` is what was run, on a cluster with no other users. On a cluster where other people can log
in to the nodes, the header in that file is readable by all of them: restrict it to the group the
Spark JVMs run as, for example `sudo chown root:hadoop` and `sudo chmod 640` on EMR, and check that
jobs still export. That variant was not run here. A collector on the node that holds the
credential avoids the file entirely.

Checked leak-free: no trace of the token, the encoded header or even the `otel.exporter.otlp.headers`
key in any of 154 files, including both Spark event logs, every container and step log, and the
bootstrap output.

Validate the stored value's shape in the bootstrap and fail if it is wrong, without printing it. A
malformed credential then stops the cluster at bootstrap instead of producing a run that silently
exports nothing.

## Notes

- **Instance role.** The default `EMR_EC2_DefaultRole` is broad (`s3:*` and more on every
  resource), and every node, including code from the downloaded JARs, runs as it. Use a scoped
  instance role in production.
- **The driver's own AWS calls.** In client mode the agent traced the driver fetching the job JAR
  from S3 and calling the instance metadata service. Flare drops these by default; see
  [Noise](../configuration/noise.md).
