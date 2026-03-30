# examples/ckan/run_all.py
import subprocess

test_files = [
    "1.ingest.py",
    "2.process.py",
    "3.query.py",
    "4.find.py",
    "5.inspect.py"
]

for tf in test_files:
    print(f"\nRunning {tf} ...")
    subprocess.run(["python3", tf], check=True)