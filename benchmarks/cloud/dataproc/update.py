"""Submitted as a Dataproc PySpark job to refresh the harness on the master from the bucket."""
import subprocess
import sys

bucket = sys.argv[1]
print(subprocess.run(f"gsutil -q cp 'gs://{bucket}/bench/scripts/*' /opt/flare/bench/ && chmod +x /opt/flare/bench/*.sh",
                     shell=True).returncode)
