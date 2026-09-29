"""Submitted as a Dataproc PySpark job to start run.sh on the master, detached, and return at once.
It never creates a SparkContext, so it is only a Python process on the master.

    gcloud dataproc jobs submit pyspark launch.py --cluster C --region R -- <run.sh arguments>
"""
import subprocess
import sys

subprocess.Popen(["bash", "/opt/flare/bench/run.sh"] + sys.argv[1:], start_new_session=True,
                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
print("started run.sh", sys.argv[1:])
