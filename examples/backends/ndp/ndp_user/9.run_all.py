# examples/ndp/ndp_developer/9.run_all.py
import subprocess

test_files = [
    "1.load_basic.py",
    "2.list_summary.py",
    "3.display.py",
    "4.query.py",
    "5.find.py",
    "6.process.py",
    "7.validate.py",
    "8.close.py"
]

for tf in test_files:
    print(f"\nRunning {tf} ...")
    subprocess.run(["python3", tf, "--verbose"], check=True)