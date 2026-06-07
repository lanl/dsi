import argparse
import shlex
import shutil
import subprocess
import sys
from pathlib import Path


KEEPALIVE_OPTS = [
    "-o", "ServerAliveInterval=60",
    "-o", "ServerAliveCountMax=30",
]


def have_command(name: str) -> bool:
    return shutil.which(name) is not None


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    print("Running:", " ".join(cmd))
    return subprocess.run(cmd, check=check)


def has_kerberos_ticket() -> bool:
    if not have_command("klist"):
        raise RuntimeError("klist not found")

    result = subprocess.run(
        ["klist", "-l"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def ensure_kerberos_ticket():
    if has_kerberos_ticket():
        return

    print("No valid Kerberos ticket found. Running kinit -f...")
    run(["kinit", "-f"])


def ssh_k(user: str, host: str):
    cmd = [
        "ssh",
        *KEEPALIVE_OPTS,
        "-l", user,
        host,
    ]
    run(cmd)


def scp_k_copy_to(local_path: str, user: str, host: str, remote_path: str):
    local = Path(local_path)
    if not local.exists():
        raise FileNotFoundError(f"Local path does not exist: {local}")

    cmd = [
        "scp",
        *KEEPALIVE_OPTS,
        str(local),
        f"{user}@{host}:{remote_path}",
    ]
    run(cmd)


def scp_k_copy_from(user: str, host: str, remote_path: str, local_path: str):
    local = Path(local_path)

    # If destination is a directory, ensure it exists
    if local.suffix == "" and not local.exists():
        local.mkdir(parents=True, exist_ok=True)

    cmd = [
        "scp",
        *KEEPALIVE_OPTS,
        f"{user}@{host}:{remote_path}",
        str(local),
    ]
    run(cmd)


def ssh_k_remote_size_bytes(
    user: str,
    host: str,
    remote_path: str,
    skip_kinit: bool = False,
    timeout: int | None = None,
) -> int:
    """
    Get remote file size in bytes using SSH with Kerberos authentication.

    This runs interactively, so SSH can prompt for Kerberos authentication if needed.
    Uses the same keepalive options as other SSH commands in this module.

    Args:
        user: username for external HPC
        host: login host for external HPC
        remote_path: The path to the file on the remote host (e.g. "/data/file.bin")
        skip_kinit: Skip Kerberos ticket check if True
        timeout: Optional timeout in seconds for the entire operation

    Returns:
        The size of the remote file in bytes.

    Raises:
        FileNotFoundError: If the remote file does not exist
        RuntimeError: If the SSH command fails or output cannot be parsed
    """
    if not skip_kinit:
        ensure_kerberos_ticket()

    # Safely quote the remote path
    quoted_path = shlex.quote(remote_path)

    # Try GNU stat first, then BSD stat, then wc fallback
    remote_cmd = (
        f"stat -c %s {quoted_path} 2>/dev/null || "
        f"stat -f %z {quoted_path} 2>/dev/null || "
        f"wc -c < {quoted_path}"
    )

    cmd = [
        "ssh",
        *KEEPALIVE_OPTS,
        "-l", user,
        host,
        remote_cmd,
    ]

    print("Running:", " ".join(cmd))
    p = subprocess.run(
        cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )

    if p.returncode != 0:
        err = (p.stderr or "") + "\n" + (p.stdout or "")
        if "No such file" in err or "not found" in err:
            raise FileNotFoundError(
                f"Remote file not found: {user}@{host}:{remote_path}\n{err.strip()}"
            )
        raise RuntimeError(
            "SSH size check failed\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout:\n{p.stdout}\n"
            f"stderr:\n{p.stderr}"
        )

    try:
        return int(p.stdout.strip())
    except ValueError:
        raise RuntimeError(
            f"Could not parse size from SSH output.\nOutput:\n{p.stdout}\nError:\n{p.stderr}"
        )


def execute_command(
    command: str,
    user: str,
    host: str,
    skip_kinit: bool = False,
    local_path: str | None = None,
    remote_path: str | None = None,
) -> int:
    """
    Execute the requested command (ssh, upload, or download).

    Args:
        command: The command to execute ('ssh', 'upload', or 'download')
        user: username for external HPC
        host: login host for external HPC
        skip_kinit: Skip Kerberos ticket check if True
        local_path: Local file path (for upload/download)
        remote_path: Remote file path (for upload/download)

    Returns:
        Exit code (0 for success)
    """
    if not skip_kinit:
        ensure_kerberos_ticket()

    if command == "ssh":
        ssh_k(user, host)

    elif command == "upload":
        if not local_path or not remote_path:
            raise ValueError("upload requires local_path and remote_path")
        scp_k_copy_to(local_path, user, host, remote_path)

    elif command == "download":
        if not local_path or not remote_path:
            raise ValueError("download requires local_path and remote_path")
        scp_k_copy_from(user, host, remote_path, local_path)

    else:
        raise ValueError(f"Unknown command: {command}")

    return 0


def main():
    """Parse command-line arguments and execute the requested command."""
    parser = argparse.ArgumentParser(description="Current HPC → External HPC SSH/SCP helper (Kerberos-aware)")

    parser.add_argument("--user", required=True, help="username for external HPC")
    parser.add_argument("--host", required=True, help="login host for external HPC")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # SSH command
    subparsers.add_parser("ssh", help="SSH into external HPC")

    # Upload
    up = subparsers.add_parser("upload", help="Copy file to external HPC")
    up.add_argument("local_path", help="Local file path")
    up.add_argument("remote_path", help="Remote destination path")

    # Download
    down = subparsers.add_parser("download", help="Copy file from external HPC")
    down.add_argument("remote_path", help="Remote file path")
    down.add_argument("local_path", help="Local destination path")

    parser.add_argument(
        "--skip-kinit",
        action="store_true",
        help="Skip Kerberos check",
    )

    args = parser.parse_args()

    # Extract optional arguments based on command
    local_path = getattr(args, "local_path", None)
    remote_path = getattr(args, "remote_path", None)

    return execute_command(
        command=args.command,
        user=args.user,
        host=args.host,
        skip_kinit=args.skip_kinit,
        local_path=local_path,
        remote_path=remote_path,
    )


if __name__ == "__main__":
    sys.exit(main())