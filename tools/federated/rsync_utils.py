from __future__ import annotations
import subprocess
import re
from pathlib import Path

def rsync_download_interactive(
    remote: str,                 # e.g. "user@host"
    remote_path: str,            # e.g. "/data/file.bin"
    local_path: str | Path,      # file or directory destination
    port: int | None = None,
    identity_file: str | None = None,
    progress: bool = True,
    timeout: int | None = None,  # None = no timeout
) -> Path:
    """
    Runs rsync interactively attached to the terminal, so SSH can prompt for
    password/MFA if required.

    Args:
        remote: The remote host, optionally with username (e.g. "user@host")
        remote_path: The path to the file on the remote host (e.g. "/data/file.bin")
        local_path: The local destination path (file or directory)
        port: Optional SSH port if not the default 22
        identity_file: Optional path to SSH private key for authentication
        progress: Whether to show progress output from rsync
        timeout: Optional timeout in seconds for the entire operation

    Returns:
        The local path of the downloaded file.
    """
    
    local_path = Path(local_path).expanduser()
    # Ensure destination directory exists
    if local_path.suffix:
        local_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        local_path.mkdir(parents=True, exist_ok=True)

    ssh_cmd = ["ssh"]
    if port is not None:
        ssh_cmd += ["-p", str(port)]
    if identity_file is not None:
        ssh_cmd += ["-i", str(Path(identity_file).expanduser())]

    cmd = ["rsync", "-a", "--partial"]
    if progress:
        cmd += ["--info=progress2"]
    cmd += ["-e", " ".join(ssh_cmd), f"{remote}:{remote_path}", str(local_path)]

    # Attach to the current terminal (no capture_output); prompts will work
    subprocess.run(cmd, check=True, timeout=timeout)

    return local_path



def rsync_remote_size_bytes_interactive(
    remote: str,
    remote_path: str,
    port: int | None = None,
    identity_file: str | None = None,
    timeout: int | None = None,
) -> int:
    """
    Uses rsync in dry-run mode to get the size of a remote file in bytes.
    This runs interactively, so SSH can prompt for password/MFA if required.

    Args:
        remote: The remote host, optionally with username (e.g. "user@host")    
        remote_path: The path to the file on the remote host (e.g. "/data/file.bin")
        port: Optional SSH port if not the default 22
        identity_file: Optional path to SSH private key for authentication
        timeout: Optional timeout in seconds for the entire operation
    Returns:
        The size of the remote file in bytes.
    """
    ssh_cmd = ["ssh"]
    if port is not None:
        ssh_cmd += ["-p", str(port)]
    if identity_file is not None:
        ssh_cmd += ["-i", str(Path(identity_file).expanduser())]

    cmd = [
        "rsync", "-n", "--stats",
        "-e", " ".join(ssh_cmd),
        f"{remote}:{remote_path}",
        "/dev/null",
    ]

    p = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)

    if p.returncode != 0:
        err = (p.stderr or "") + "\n" + (p.stdout or "")
        # Common rsync/ssh “missing file” patterns
        if re.search(r"(No such file or directory|link_stat|cannot stat|failed: No such file)", err, re.IGNORECASE):
            raise FileNotFoundError(f"Remote file not found: {remote}:{remote_path}\n{err.strip()}")
        raise RuntimeError(
            "rsync size check failed\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout:\n{p.stdout}\n"
            f"stderr:\n{p.stderr}"
        )

    m = re.search(r"Total file size:\s*([0-9,]+)\s*bytes", p.stdout)
    if not m:
        raise RuntimeError(f"Could not parse size from rsync output.\nOutput:\n{p.stdout}")

    return int(m.group(1).replace(",", ""))
