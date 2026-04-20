from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def get_s3_client():
    """Create and return a boto3 S3 client."""
    return boto3.client("s3")


def resolve_s3_bucket_and_key(location: str, path: str) -> tuple[str, str]:
    """
    Resolve S3 bucket and key from either:
    - location='my-bucket', path='folder/file.dat'
    - location='s3://my-bucket', path='folder/file.dat'
    - path='s3://my-bucket/folder/file.dat'
    """
    path = path.strip()
    location = location.strip()

    # Full S3 URL provided in path
    if path.startswith("s3://"):
        parsed = urlparse(path)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if not bucket or not key:
            raise ValueError(f"Could not parse S3 URI from path={path!r}")
        return bucket, key

    # Full S3 URL provided in location
    if location.startswith("s3://"):
        parsed = urlparse(location)
        bucket = parsed.netloc
        base_prefix = parsed.path.lstrip("/").rstrip("/")
        key = path.lstrip("/")
        if base_prefix:
            key = f"{base_prefix}/{key}"
        if not bucket or not key:
            raise ValueError(f"Could not parse S3 URI from location={location!r}, path={path!r}")
        return bucket, key

    # Plain bucket name in location
    bucket = location
    key = path.lstrip("/")
    if not bucket or not key:
        raise ValueError(f"Missing bucket or key: location={location!r}, path={path!r}")
    return bucket, key


def get_s3_object_metadata(bucket: str, key: str) -> dict:
    """Return S3 object metadata from head_object."""
    s3 = get_s3_client()
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            raise FileNotFoundError(f"s3://{bucket}/{key} not found") from e
        raise


def get_s3_remote_file_size(bucket: str, key: str) -> int:
    """Return the size in bytes of an S3 object."""
    metadata = get_s3_object_metadata(bucket, key)
    return int(metadata["ContentLength"])


def get_s3_etag(bucket: str, key: str) -> str:
    """
    Return the S3 ETag without surrounding quotes.

    Note:
    - For single-part uploads, ETag is often the MD5 of the object.
    - For multipart uploads, it is not a plain MD5 and usually contains a dash.
    """
    metadata = get_s3_object_metadata(bucket, key)
    return metadata["ETag"].strip('"')


def should_download_s3(bucket: str, key: str, stored_md5: str) -> bool:
    """
    Decide whether an S3 object should be downloaded.

    Returns:
        True if download is needed, False if local file is up to date.

    Behavior:
    - If the S3 ETag looks like a single-part MD5, compare directly to stored_md5.
    - If it looks like a multipart ETag, return True because direct MD5 comparison
      is not reliable.
    """
    remote_etag = get_s3_etag(bucket, key)

    # Multipart upload ETags are not plain MD5 hashes
    if "-" in remote_etag:
        return True

    return remote_etag != stored_md5.lower()


def download_s3_file(bucket: str, key: str, output_dir: str) -> str:
    """
    Download an S3 object into output_dir.

    Returns:
        The absolute path of the downloaded file.
    """
    s3 = get_s3_client()
    output_path = Path(output_dir) / Path(key).name
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        s3.download_file(bucket, key, str(output_path))
    except (ClientError, BotoCoreError) as e:
        raise RuntimeError(f"Failed to download s3://{bucket}/{key}: {e}") from e

    return str(output_path)