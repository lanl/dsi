# examples/maglab/6.download_files.py
"""
Download a couple of .tdms files using metadata from the files table.

The Maglab backend is metadata-first: it does not download files
automatically. Downloading is a manual follow-up step once you have
identified the files you want using the files table.
"""

import requests

from dsi.dsi import DSI

def main():
    dsi = DSI(
        backend_name="Maglab",
        params={"node_id": "8r2b3", "include_ext": ".tdms"}
    )

    files_df = dsi.get_table("files", collection=True)
    print(f"\nFound {len(files_df)} .tdms files")
    print(files_df[["name", "size_bytes", "download_url"]])

    # Download the first couple of files using their download_url column
    for _, row in files_df.head(2).iterrows():
        url = row["download_url"]
        name = row["name"]
        if not url:
            continue

        # verify=False matches the Maglab backend's own default SSL setting
        response = requests.get(url, verify=False)
        with open(name, "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {name}")

    dsi.close()

if __name__ == "__main__":
    main()
