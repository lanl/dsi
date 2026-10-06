#!/usr/bin/env python3
"""
Install the Pelican OSDF client (the standalone binary, NOT `pip install pelican`).
Run once:  python installPelican.py
"""

import os
import sys
import glob
import shutil
import platform
import subprocess

VERSION = "7.19.0"  # check https://github.com/PelicanPlatform/pelican/releases for newest
BIN_DIR = os.path.expanduser("~/bin")
DEST    = os.path.join(BIN_DIR, "pelican")


def pick_asset():
    """Choose the right release asset for this OS/arch."""
    system = platform.system()
    machine = platform.machine().lower()
    if system == "Linux":
        return "pelican_Linux_x86_64.tar.gz"
    if system == "Darwin":
        if machine in ("arm64", "aarch64"):
            return "pelican_Darwin_arm64.tar.gz"
        return "pelican_Darwin_x86_64.tar.gz"
    if system == "Windows":
        return "pelican_Windows_x86_64.zip"
    sys.exit(f"Unsupported OS: {system}. Install pelican manually.")


def main():
    os.makedirs(BIN_DIR, exist_ok=True)
    asset = pick_asset()
    url = (f"https://github.com/PelicanPlatform/pelican/releases/"
           f"download/v{VERSION}/{asset}")
    tarball = "/tmp/pelican.tar.gz"
    if ".zip" in asset:
        tarball = "C:/tmp/pelican.zip"

    print(f"Downloading {url}")
    # curl follows redirects (-L) and handles TLS without Python's cert issues.
    subprocess.run(["curl", "-L", "-o", tarball, url], check=True)

    print("Extracting...")
    if ".zip" in tarball:
        subprocess.run(["tar", "-xzf", tarball, "-C", "C:/tmp"], check=True)
    else:
        subprocess.run(["tar", "-xzf", tarball, "-C", "/tmp"], check=True)

    if ".zip" in asset:
        matches = glob.glob("C:/tmp/pelican-*/pelican.exe")
        osDEST = DEST+".exe"
    else:
        matches = glob.glob("/tmp/pelican-*/pelican")
        osDEST = DEST
    if not matches:
        sys.exit("Could not find the pelican binary after extraction.")
    
    shutil.copy(matches[0], osDEST)
    os.chmod(osDEST, 0o755)
    print(f"Installed to {osDEST}")

    # --- Verify: must print a version and list "get" as a valid command ---
    ver = subprocess.run([osDEST, "--version"], capture_output=True, text=True)
    print("\n--version:\n" + (ver.stdout or ver.stderr))

    help_ = subprocess.run([osDEST, "object", "get", "--help"],
                           capture_output=True, text=True)
    ok = help_.returncode == 0 or "object" in (help_.stdout + help_.stderr)
    if ok:
        print("\u2713 Verified: real Pelican OSDF client installed.")
    else:
        sys.exit("\u2717 Wrong pelican detected (site generator?). "
                 "Run `pip uninstall pelican` and re-run this script.")

    print(f"\nNOTE: add this to your ~/.bashrc to keep it on PATH:\n"
          f'  export PATH="$HOME/bin:$PATH"')


if __name__ == "__main__":
    main()