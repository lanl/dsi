from __future__ import annotations

import json
import re
import ssl
import time
from pathlib import Path
from urllib.parse import urlparse, unquote
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


# Disable SSL certificate verification
SSL_CONTEXT = ssl._create_unverified_context()

# Retry only transient HTTP errors
RETRYABLE_HTTP_CODES = {500, 502, 503, 504}


def _open(
    url: str,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    timeout: float = 20.0,
    retries: int = 3,
):
    """
    Open a URL with timeout and retry support.

    Args:
        url: URL to open.
        method: HTTP method.
        headers: Optional request headers.
        timeout: Timeout in seconds for each attempt.
        retries: Number of attempts.

    Returns:
        A urllib response object.

    Raises:
        HTTPError, URLError: If all attempts fail or a non-retryable error occurs.
    """
    req = Request(url, headers=headers or {}, method=method)

    for attempt in range(retries):
        try:
            return urlopen(req, context=SSL_CONTEXT, timeout=timeout)

        except HTTPError as exc:
            if exc.code not in RETRYABLE_HTTP_CODES or attempt == retries - 1:
                raise

        except URLError:
            if attempt == retries - 1:
                raise

        time.sleep(2 ** attempt)  # exponential backoff


def github_to_api_contents(url: str) -> str | None:
    """
    Convert GitHub URLs to the contents API:
      https://github.com/<owner>/<repo>/blob/<ref>/<path>
      https://github.com/<owner>/<repo>/tree/<ref>/<path>
    ->
      https://api.github.com/repos/<owner>/<repo>/contents/<path>?ref=<ref>

    Args:
        url: The GitHub URL to convert.

    Returns:
        The corresponding GitHub contents API URL, or None if the URL is not recognized.
    """
    m = re.match(r"^https://github\.com/([^/]+)/([^/]+)/(?:blob|tree)/([^/]+)/(.*)$", url)
    if not m:
        return None
    owner, repo, ref, path = m.groups()
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={ref}"


def github_blob_to_raw(url: str) -> str:
    """
    Convert GitHub web blob URLs to raw file URLs when needed.

    Supports:
      - https://github.com/<owner>/<repo>/blob/<ref>/<path>
      - https://raw.githubusercontent.com/<owner>/<repo>/<ref>/<path>

    Args:
        url: The GitHub URL to convert.

    Returns:
        A raw GitHub URL if conversion is possible, otherwise the original URL.
    """
    if url.startswith("https://raw.githubusercontent.com/"):
        return url
    if url.startswith("https://github.com/") and "/blob/" in url:
        return (
            url.replace("https://github.com/", "https://raw.githubusercontent.com/")
            .replace("/blob/", "/")
        )
    return url


def get_github_remote_file_size(
    url: str,
    timeout: float = 20.0,
    retries: int = 3,
    github_token: str | None = None,
) -> int:
    """
    Get the size of a remote GitHub file in bytes without downloading it.

    Tries, in order:
      1. GitHub contents API
      2. HEAD request on raw URL
      3. Range GET fallback on raw URL

    Args:
        url: GitHub file URL (blob/tree/raw/direct).
        timeout: Timeout in seconds for each request attempt.
        retries: Number of attempts for each network operation.
        github_token: Optional GitHub token.

    Returns:
        The file size in bytes, or 0 if it cannot be determined.
    """
    api_url = github_to_api_contents(url)
    if api_url:
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "python-urllib",
        }
        if github_token:
            headers["Authorization"] = f"Bearer {github_token}"

        try:
            with _open(
                api_url,
                method="GET",
                headers=headers,
                timeout=timeout,
                retries=retries,
            ) as response:
                if 200 <= response.status < 300:
                    payload = json.loads(response.read().decode("utf-8"))
                    if (
                        isinstance(payload, dict)
                        and payload.get("type") == "file"
                        and "size" in payload
                    ):
                        return int(payload["size"])
        except (URLError, HTTPError, ValueError, json.JSONDecodeError):
            pass

    raw_url = github_blob_to_raw(url)
    headers = {"User-Agent": "python-urllib"}
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"

    try:
        with _open(
            raw_url,
            method="HEAD",
            headers=headers,
            timeout=timeout,
            retries=retries,
        ) as response:
            cl = response.headers.get("Content-Length")
            if 200 <= response.status < 300 and cl and cl.isdigit():
                return int(cl)
    except (URLError, HTTPError):
        pass

    range_headers = dict(headers)
    range_headers["Range"] = "bytes=0-0"

    try:
        with _open(
            raw_url,
            method="GET",
            headers=range_headers,
            timeout=timeout,
            retries=retries,
        ) as response:
            content_range = (response.headers.get("Content-Range") or "").strip()
            if "/" in content_range:
                total = content_range.split("/")[-1].strip()
                if total.isdigit():
                    return int(total)

            cl = response.headers.get("Content-Length")
            if cl and cl.isdigit():
                return int(cl)
    except (URLError, HTTPError):
        pass

    return 0


def github_to_raw(url: str) -> str:
    """
    Convert GitHub web URLs to raw file URLs.

    Supports:
      - https://github.com/<owner>/<repo>/blob/<ref>/<path>
      - https://github.com/<owner>/<repo>/tree/<ref>/<path>

    Args:
        url: The GitHub URL to convert.

    Returns:
        The raw.githubusercontent.com URL if recognized, otherwise the original URL.
    """
    if url.startswith("https://raw.githubusercontent.com/"):
        return url

    m = re.match(r"^https://github\.com/([^/]+)/([^/]+)/(?:blob|tree)/([^/]+)/(.*)$", url)
    if not m:
        return url
    owner, repo, ref, path = m.groups()
    return f"https://raw.githubusercontent.com/{owner}/{repo}/{ref}/{path}"


def filename_from_url(url: str) -> str:
    """
    Extract the filename from a URL.

    Args:
        url: The URL to inspect.

    Returns:
        The filename, or 'downloaded_file' if none is found.
    """
    path = urlparse(url).path
    return Path(unquote(path)).name or "downloaded_file"


def download_github_file(
    url: str,
    out_path: str | Path,
    github_token: str | None = None,
    timeout: float = 30.0,
    retries: int = 3,
) -> Path:
    """
    Download a file from GitHub, supporting blob/tree/raw URLs.

    If out_path is a directory, the filename is inferred from the URL.

    Args:
        url: GitHub file URL.
        out_path: Output file path or directory.
        github_token: Optional GitHub token.
        timeout: Timeout in seconds for each request attempt.
        retries: Number of attempts.
    Returns:
        Path to the downloaded file.
    """
    out_path = Path(out_path)
    raw_url = github_to_raw(url)

    if out_path.exists() and out_path.is_dir():
        out_file = out_path / filename_from_url(raw_url)
    elif str(out_path).endswith(("/", "\\")) or out_path.suffix == "":
        out_path.mkdir(parents=True, exist_ok=True)
        out_file = out_path / filename_from_url(raw_url)
    else:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_file = out_path

    headers = {"User-Agent": "python-urllib"}
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"

    with _open(
        raw_url,
        method="GET",
        headers=headers,
        timeout=timeout,
        retries=retries,
    ) as response:
        if not (200 <= response.status < 300):
            raise HTTPError(raw_url, response.status, "Download failed", response.headers, None)

        with open(out_file, "wb") as f:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)

    return out_file