from __future__ import annotations
import re
import requests
from pathlib import Path
from urllib.parse import urlparse, unquote


def github_to_api_contents(url: str) -> str | None:
    """
    Convert GitHub URLs to the contents API:
      https://github.com/<owner>/<repo>/blob/<ref>/<path>
      https://github.com/<owner>/<repo>/tree/<ref>/<path>
    ->
      https://api.github.com/repos/<owner>/<repo>/contents/<path>?ref=<ref>

    Args:
        url: The GitHub URL to convert.
    """
    m = re.match(r"^https://github\.com/([^/]+)/([^/]+)/(?:blob|tree)/([^/]+)/(.*)$", url)
    if not m:
        return None
    owner, repo, ref, path = m.groups()
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={ref}"



def github_blob_to_raw(url: str) -> str:
    """
    Convert GitHub web URLs to raw file URLs when needed.
    Supports:
      - https://github.com/<owner>/<repo>/blob/<ref>/<path>
      - https://raw.githubusercontent.com/<owner>/<repo>/<ref>/<path> (already raw)

    Args:
        url: The GitHub URL to convert.
    """
    if url.startswith("https://raw.githubusercontent.com/"):
        return url
    if url.startswith("https://github.com/") and "/blob/" in url:
        return url.replace("https://github.com/", "https://raw.githubusercontent.com/").replace("/blob/", "/")
    return url  # assume user already provided a direct download URL


def get_github_remote_file_size(url: str, timeout: int = 20, github_token: str | None = None) -> int:
    """
    Get the size of a remote file on GitHub in bytes without downloading it.
    Tries multiple methods for robustness:

    Args:
        url: The GitHub URL of the file (can be a blob/tree URL or raw URL)
        timeout: Timeout in seconds for network requests
        github_token: Optional GitHub token for authenticated requests (can help with rate limits)

    Returns:
        The size of the remote file in bytes.
    """
    # 1) Prefer GitHub API if it looks like a GitHub blob/tree link
    api_url = github_to_api_contents(url)
    if api_url:
        headers = {"Accept": "application/vnd.github+json"}
        if github_token:
            headers["Authorization"] = f"Bearer {github_token}"
        r = requests.get(api_url, headers=headers, timeout=timeout)
        if r.ok:
            js = r.json()
            if isinstance(js, dict) and js.get("type") == "file" and "size" in js:
                return int(js["size"])
        # If API fails, try raw URL next

    # 2) If it's a GitHub blob URL, switch to raw for HTTP size checks
    raw_url = github_blob_to_raw(url) or url

    # 3) HEAD
    r = requests.head(raw_url, allow_redirects=True, timeout=timeout)
    cl = r.headers.get("Content-Length")
    if r.ok and cl and cl.isdigit():
        return int(cl)

    # 4) Range GET fallback
    r = requests.get(raw_url, headers={"Range": "bytes=0-0"}, allow_redirects=True, timeout=timeout)
    cr = (r.headers.get("Content-Range") or "").strip()
    if "/" in cr:
        total = cr.split("/")[-1].strip()
        if total.isdigit():
            return int(total)

    cl = r.headers.get("Content-Length")
    if cl and cl.isdigit():
        return int(cl)

    return 0  # Could not determine size, return 0 as a fallback



def download_github_file(url: str, out_path: str | Path, github_token: str | None = None, timeout: int = 30) -> Path:
    """
    Download a file from GitHub, supporting both blob/tree URLs and raw URLs.
    If the output path is a directory, the filename will be inferred from the URL.

    Args:
        url: The GitHub URL of the file to download (can be a blob/tree URL or raw URL)
        out_path: The local output path (file or directory)
        github_token: Optional GitHub token for authenticated requests
        timeout: Timeout in seconds for network requests    

    Returns:
        The path to the downloaded file.        
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    raw_url = github_blob_to_raw(url)
    headers = {}
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"

    with requests.get(raw_url, headers=headers, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    return out_path



def github_to_raw(url: str) -> str:
    """
    Convert GitHub web URLs to raw file URLs:
      - https://github.com/<owner>/<repo>/blob/<ref>/<path>
      - https://github.com/<owner>/<repo>/tree/<ref>/<path>   (user sometimes links files this way)
    -> https://raw.githubusercontent.com/<owner>/<repo>/<ref>/<path>

    Args:        
        url: The GitHub URL to convert.

    Returns:
        str: The raw.githubusercontent.com URL if it was a recognized GitHub URL, otherwise returns the original URL.

    """
    if url.startswith("https://raw.githubusercontent.com/"):
        return url

    m = re.match(r"^https://github\.com/([^/]+)/([^/]+)/(?:blob|tree)/([^/]+)/(.*)$", url)
    if not m:
        return url  # assume already a direct download URL
    owner, repo, ref, path = m.groups()
    return f"https://raw.githubusercontent.com/{owner}/{repo}/{ref}/{path}"



def filename_from_url(url: str) -> str:
    """
    Extract the filename from a URL.

    Args:
        url: The URL to extract the filename from.

    Returns:
        The filename extracted from the URL, or "downloaded_file" if it cannot be determined
    """
    path = urlparse(url).path
    return Path(unquote(path)).name or "downloaded_file"



def download_github_file(url: str, out_path: str | Path, github_token: str | None = None, timeout: int = 30) -> Path:
    """
    Download a file from GitHub, supporting both blob/tree URLs and raw URLs.
    If the output path is a directory, the filename will be inferred from the URL.

    Args:
        url: The GitHub URL of the file to download (can be a blob/tree URL or raw URL)
        out_path: The local output path (file or directory)
        github_token: Optional GitHub token for authenticated requests
        timeout: Timeout in seconds for network requests

    Returns:
        The path to the downloaded file.
    """
    out_path = Path(out_path)
    raw_url = github_to_raw(url)

    # If user passed a directory, append filename
    if out_path.exists() and out_path.is_dir():
        out_file = out_path / filename_from_url(raw_url)
    elif str(out_path).endswith(("/", "\\")) or out_path.suffix == "":
        # Treat paths with no suffix as directories to be safe
        out_path.mkdir(parents=True, exist_ok=True)
        out_file = out_path / filename_from_url(raw_url)
    else:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_file = out_path

    headers = {}
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"

    with requests.get(raw_url, headers=headers, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(out_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    return out_file
