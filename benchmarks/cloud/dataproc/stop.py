"""Submitted as a Dataproc PySpark job to stop a running run.sh and its matrix on the master."""
import subprocess

for pattern in ["/opt/flare/bench/run.sh", "run_matrix.py", "otlp_sink.py", "tpch_generate.py"]:
    print(pattern, subprocess.run(["pkill", "-f", pattern]).returncode)
