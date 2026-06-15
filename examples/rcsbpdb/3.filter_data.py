from pathlib import Path
from urllib.parse import urlparse

import requests

from dsi.dsi import DSI


def safe_filename_from_url(url: str) -> str:
    path = urlparse(url).path
    name = Path(path).name

    if not name:
        return "downloaded_resource"

    return name


def download_file(url: str, output_path: Path) -> None:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    output_path.write_bytes(response.content)


def main():
    dsi = DSI(
        backend_name="RCSBPDB",
        params={"keywords": "genomics", "limit": 5},
        silence_messages=True,
    )

    print("\nLoaded RCSBPDB backend using keyword search.")

    resources_df = dsi.get_table("resources", collection=True)

    target_format = "pdb.gz"

    print(f"\nFiltering resources where format == '{target_format}'.")

    filtered_resources = resources_df[
        resources_df["format"].fillna("").str.lower() == target_format
    ]

    columns = [
        "resource_id",
        "dataset_id",
        "name",
        "format",
        "resource_type",
        "download_url",
    ]
    existing_columns = [col for col in columns if col in filtered_resources.columns]

    print("\nMatching resources:")
    print(filtered_resources[existing_columns])

    download_dir = Path(__file__).resolve().parent / "downloaded_rcsbpdb_files"
    download_dir.mkdir(exist_ok=True)

    print(f"\nDownloading {len(filtered_resources)} matching resource file(s) to:")
    print(download_dir)

    for _, row in filtered_resources.iterrows():
        url = row.get("download_url")

        if not isinstance(url, str) or not url.strip():
            continue

        filename = row.get("name") or safe_filename_from_url(url)
        output_path = download_dir / filename

        try:
            download_file(url, output_path)
            print(f"Downloaded: {output_path}")
        except Exception as exc:
            print(f"Failed to download {url}: {exc}")

    dsi.close()


if __name__ == "__main__":
    main()