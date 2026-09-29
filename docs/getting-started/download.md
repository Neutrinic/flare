# Downloading the JARs

Flare needs two JARs on every node: the OpenTelemetry Java agent and the Flare JAR for your Spark
and Scala version ([Choosing a JAR](choosing-a-jar.md)). Both are on Maven Central.

## URLs

The examples below use Spark 3.5 on Scala 2.12, `flare-spark-3-5_2.12`. For another coordinate,
replace `3-5_2.12` in both places it appears in the Flare URL.

```bash
--8<-- "urls.sh"
```

The agent is pinned to the version Flare is built and tested against. Use that version rather than
the newest agent: the agent's extension API can change between releases.

## Linux

On cluster nodes, as root, which is how init scripts, bootstrap actions and image builds run:

```bash
--8<-- "urls.sh"

mkdir -p /opt/flare
curl -fsSL -o /opt/flare/opentelemetry-javaagent.jar "$AGENT"
curl -fsSL -o /opt/flare/flare-spark.jar "$FLARE"
```

`wget -q -O <file> <url>` works the same way where `curl` is not installed.

## macOS

For a local Spark, in a directory you can write to without `sudo`:

```bash
--8<-- "urls.sh"

mkdir -p ~/flare
curl -fsSL -o ~/flare/opentelemetry-javaagent.jar "$AGENT"
curl -fsSL -o ~/flare/flare-spark.jar "$FLARE"
```

Use `~/flare` in place of `/opt/flare` in the other examples.

## Windows

For a local Spark on Windows, in PowerShell:

```powershell
--8<-- "urls.ps1"

New-Item -ItemType Directory -Force C:\flare | Out-Null
Invoke-WebRequest -Uri $agent -OutFile C:\flare\opentelemetry-javaagent.jar
Invoke-WebRequest -Uri $flare -OutFile C:\flare\flare-spark.jar
```

## From GitHub

Every release also attaches each Flare JAR:

```bash
--8<-- "github-release.sh"
```

## Checking the download

Maven Central serves a SHA-1 checksum next to every file, and a PGP signature (`.asc`) next to
each Flare file. On Linux:

```bash
echo "$(curl -fsSL "$FLARE.sha1")  /opt/flare/flare-spark.jar" | sha1sum -c
echo "$(curl -fsSL "$AGENT.sha1")  /opt/flare/opentelemetry-javaagent.jar" | sha1sum -c
```

On macOS, which has `shasum` rather than `sha1sum`:

```bash
echo "$(curl -fsSL "$FLARE.sha1")  $HOME/flare/flare-spark.jar" | shasum -a 1 -c
echo "$(curl -fsSL "$AGENT.sha1")  $HOME/flare/opentelemetry-javaagent.jar" | shasum -a 1 -c
```

In PowerShell, compare `(Get-FileHash C:\flare\flare-spark.jar -Algorithm SHA1).Hash` with
`(Invoke-WebRequest "$flare.sha1").Content`.

## Pinning a version

Always name an exact version. Maven Central has no "latest" URL, and a cluster should not change
its agent or Flare version when a node restarts. To upgrade, change the version in your init script
or image, and read [Upgrading](../upgrading.md) first.

## Clusters without internet access

Download the two JARs once and put them where your nodes can reach them: an internal bucket
(`s3://`, `gs://`, `abfss://`), a shared filesystem, or your container image. A Maven proxy, such as
Nexus or Artifactory, serves the same URL layout as Maven Central, so the URLs above work with its
host in place of `repo1.maven.org`.
