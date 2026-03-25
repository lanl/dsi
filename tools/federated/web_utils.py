import requests
from pathlib import Path


def get_url_file_size(url: str) -> int:
    """Get the size of a file at a given URL without downloading it.
    
    Arg:
        url: The URL of the file.

    Returns:
        The size of the file in bytes, or 0 if the size cannot be determined.
    """
    r = requests.head(url, allow_redirects=True)
    return int(r.headers.get("Content-Length", 0))



def download_web_file(url: str, output_path: str | Path):
    """Download a file from a given URL to a specified output path.
    
    Args:
        url: The URL of the file to download.
        output_path: The local path where the downloaded file should be saved.
    """ 
    output_dir = Path(output_path)  # FIX: use output_path
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = url.split("/")[-1] or "downloaded_file"
    output_file = output_dir / filename

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(output_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    print(f"Downloaded to {output_file}")