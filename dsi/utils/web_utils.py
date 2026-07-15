from pathlib import Path
from urllib.request import Request, urlopen
from urllib.parse import urlsplit, unquote
from urllib.error import URLError, HTTPError
import ssl
import time
import os


SSL_CONTEXT = ssl._create_unverified_context()


def _filename_from_url(url: str) -> str:
    path = urlsplit(url).path
    name = Path(unquote(path)).name
    return name or "downloaded_file"


def _should_retry_http_error(exc: HTTPError) -> bool:
    return exc.code in {500, 502, 503, 504}


def get_url_file_size(url: str, timeout: float = 10.0, retries: int = 3) -> int:
    """Get the size of a file at a given URL without downloading it.

    Args:
        url: The URL of the file.
        timeout: Timeout in seconds for each request attempt.
        retries: Number of attempts.

    Returns:
        The size of the file in bytes, or 0 if it cannot be determined.
    """
    req = Request(url, method="HEAD")

    for attempt in range(retries):
        try:
            with urlopen(req, context=SSL_CONTEXT, timeout=timeout) as response:
                return int(response.headers.get("Content-Length", 0))
        except HTTPError as exc:
            if not _should_retry_http_error(exc) or attempt == retries - 1:
                return 0
            time.sleep(2 ** attempt)
        except URLError:
            if attempt == retries - 1:
                return 0
            time.sleep(2 ** attempt)

    return 0


def download_web_file(
    url: str,
    output_dir: str | Path,
    timeout: float = 10.0,
    retries: int = 3,
) -> Path:
    """Download a file from a URL into a directory.

    Args:
        url: The URL of the file to download.
        output_dir: Directory where the downloaded file will be saved.
        timeout: Timeout in seconds for each request attempt.
        retries: Number of attempts.

    Returns:
        The path to the downloaded file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = _filename_from_url(url)
    output_file = output_dir / filename
    temp_file = output_file.with_suffix(output_file.suffix + ".part")

    req = Request(url, method="GET")

    for attempt in range(retries):
        try:
            with urlopen(req, context=SSL_CONTEXT, timeout=timeout) as response:
                with open(temp_file, "wb") as f:
                    while True:
                        chunk = response.read(8192)
                        if not chunk:
                            break
                        f.write(chunk)

            os.replace(temp_file, output_file)
            print(f"Downloaded to {output_file}")
            return output_file

        except HTTPError as exc:
            if temp_file.exists():
                temp_file.unlink()

            if not _should_retry_http_error(exc) or attempt == retries - 1:
                raise

            time.sleep(2 ** attempt)

        except URLError:
            if temp_file.exists():
                temp_file.unlink()

            if attempt == retries - 1:
                raise

            time.sleep(2 ** attempt)