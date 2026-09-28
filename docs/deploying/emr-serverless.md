# EMR Serverless

Verified on EMR 7.14.0 (Spark 3.5.8, Java 17) with a driver and two executors.

EMR Serverless has no bootstrap actions and no init scripts, so a **custom image** is the only way
to get the agent onto the machine before the JVM starts.

## Image

```dockerfile
FROM public.ecr.aws/emr-serverless/spark/emr-7.14.0:latest
USER root
COPY opentelemetry-javaagent.jar flare-spark.jar /opt/flare/
RUN chmod 755 /opt/flare && chmod 644 /opt/flare/*.jar \
 && cp /opt/flare/flare-spark.jar /usr/lib/spark/jars/flare-spark.jar
USER hadoop:hadoop
```

- `/usr/lib/spark/jars` is already on the driver and executor classpath. The image's
  `spark-defaults.conf` sets a long `extraClassPath` (Spark Connect, Hadoop AWS, EMRFS, the AWS SDK),
  so putting Flare there avoids repeating and appending to it.
- The image must run as `hadoop`.

Push it to ECR and give EMR Serverless pull access with a repository policy allowing
`ecr:BatchGetImage`, `ecr:DescribeImages` and `ecr:GetDownloadUrlForLayer` for the principal
`emr-serverless.amazonaws.com`.

## Application and job

```bash
aws emr-serverless create-application --name my-app --type SPARK --release-label emr-7.14.0 \
  --image-configuration imageUri=<account>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>
```

Set the JVM options through the `spark-defaults` classification rather than
`sparkSubmitParameters`, which avoids quoting values that contain spaces:

```json
{
  "applicationConfiguration": [{
    "classification": "spark-defaults",
    "properties": {
      "spark.plugins": "io.flare.spark.plugin.FlareSparkPlugin",
      "spark.driver.extraJavaOptions": "-javaagent:/opt/flare/opentelemetry-javaagent.jar -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar -Dotel.service.name=my-app-driver",
      "spark.executor.extraJavaOptions": "-javaagent:/opt/flare/opentelemetry-javaagent.jar -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar -Dotel.service.name=my-app-executor"
    }
  }]
}
```

`extraJavaOptions` is empty in the image (EMR's own flags are in `defaultJavaOptions`), so setting
it replaces nothing.

## Credentials and networking

- There is no init script to write an agent configuration file at start-up. Point the agent at a
  collector in your VPC that holds the backend credential, or bake a non-secret configuration file
  into the image. Do not put a token in `spark-defaults`: it is visible in the job's configuration.
- The verified run exported to the job's logs in S3, so it says nothing about reaching an external
  collector. An application attached to your VPC, with a collector in that VPC, is the safe
  assumption.
