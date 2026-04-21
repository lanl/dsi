from pathlib import Path
from urllib.parse import urlparse
import getpass
import os

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    PartialCredentialsError,
)


def get_s3_client(
    region_name: str | None = None,
    profile_name: str | None = None,
    allow_anonymous: bool = False,
    interactive: bool = True,
):
    """
    Create and return a boto3 S3 client.

    Resolution order:
    1. Explicit profile if provided
    2. Default boto3 credential chain:
       environment, shared config, shared credentials, SSO, assume-role, etc.
    3. Anonymous unsigned client if allow_anonymous=True
    4. Interactive prompt if interactive=True

    Args:
        region_name: AWS region name. Defaults to AWS_DEFAULT_REGION, AWS_REGION,
            or "us-gov-west-1".
        profile_name: Optional AWS profile name to use.
        allow_anonymous: If True, allow unsigned access for public buckets/objects.
        interactive: If True, prompt for access key / secret / session token if
            credentials are not otherwise found.

    Returns:
        boto3 S3 client
    """
    region = (
        region_name
        or os.environ.get("AWS_DEFAULT_REGION")
        or os.environ.get("AWS_REGION")
        or "us-gov-west-1"
    )

    try:
        if profile_name:
            session = boto3.session.Session(profile_name=profile_name, region_name=region)
        else:
            session = boto3.session.Session(region_name=region)
    except Exception as e:
        raise RuntimeError(f"Failed to create boto3 session: {e}") from e

    # Let boto3 resolve credentials through its normal chain
    try:
        creds = session.get_credentials()
    except Exception as e:
        raise RuntimeError(f"Failed while resolving AWS credentials: {e}") from e

    if creds is not None:
        return session.client("s3", region_name=region)

    if allow_anonymous:
        return boto3.client(
            "s3",
            region_name=region,
            config=Config(signature_version=UNSIGNED),
        )

    if not interactive:
        raise NoCredentialsError()

    print(" -- AWS credentials were not found via environment/config/profile/SSO.")
    access_key = input(" -- Enter AWS access key ID: ").strip()
    secret_key = getpass.getpass(" -- Enter AWS secret access key: ").strip()
    session_token = getpass.getpass(
        " -- Enter AWS session token (leave blank if not needed): "
    ).strip()
    region = input(f" -- Enter AWS region [{region}]: ").strip() or region

    if not access_key or not secret_key:
        raise NoCredentialsError()

    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token or None,
    )


def resolve_s3_bucket_and_key(location: str, path: str) -> tuple[str, str]:
    """
    Resolve S3 bucket and key from any of these forms:

    - location='my-bucket', path='folder/file.dat'
    - location='s3://my-bucket', path='folder/file.dat'
    - path='s3://my-bucket/folder/file.dat'
    - path='https://my-bucket.s3.us-gov-west-1.amazonaws.com/folder/file.dat'
    - path='https://s3.us-gov-west-1.amazonaws.com/my-bucket/folder/file.dat'
    """
    path = (path or "").strip()
    location = (location or "").strip()

    def _parse_https_s3_url(url: str) -> tuple[str, str] | None:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return None

        host = parsed.netloc
        clean_path = parsed.path.lstrip("/")

        # Virtual-hosted-style:
        # https://bucket.s3.region.amazonaws.com/key
        if ".s3." in host:
            bucket = host.split(".s3.", 1)[0]
            key = clean_path
            if bucket and key:
                return bucket, key

        # Path-style:
        # https://s3.region.amazonaws.com/bucket/key
        if host.startswith("s3.") or ".s3-" in host or host == "s3.amazonaws.com":
            parts = clean_path.split("/", 1)
            if len(parts) == 2:
                bucket, key = parts
                if bucket and key:
                    return bucket, key

        return None

    if path.startswith("s3://"):
        parsed = urlparse(path)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if not bucket or not key:
            raise ValueError(f"Could not parse S3 URI from path={path!r}")
        return bucket, key

    https_result = _parse_https_s3_url(path)
    if https_result is not None:
        return https_result

    if location.startswith("s3://"):
        parsed = urlparse(location)
        bucket = parsed.netloc
        base_prefix = parsed.path.lstrip("/").rstrip("/")
        key = path.lstrip("/")
        if base_prefix:
            key = f"{base_prefix}/{key}"
        if not bucket or not key:
            raise ValueError(
                f"Could not parse S3 URI from location={location!r}, path={path!r}"
            )
        return bucket, key

    https_result = _parse_https_s3_url(location)
    if https_result is not None:
        bucket, base_key = https_result
        extra_key = path.lstrip("/")
        key = f"{base_key}/{extra_key}" if extra_key else base_key
        return bucket, key

    bucket = location
    key = path.lstrip("/")
    if not bucket or not key:
        raise ValueError(f"Missing bucket or key: location={location!r}, path={path!r}")
    return bucket, key


def get_s3_object_metadata(bucket: str, key: str, s3_client=None) -> dict:
    """Return S3 object metadata from head_object."""
    s3 = s3_client or get_s3_client()
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            raise FileNotFoundError(f"s3://{bucket}/{key} not found") from e
        if error_code in {"403", "AccessDenied"}:
            raise PermissionError(f"Access denied for s3://{bucket}/{key}") from e
        raise
    except (NoCredentialsError, PartialCredentialsError):
        raise


def get_s3_remote_file_size(bucket: str, key: str, s3_client=None) -> int:
    """Return the size in bytes of an S3 object."""
    metadata = get_s3_object_metadata(bucket, key, s3_client=s3_client)
    return int(metadata["ContentLength"])


def get_s3_etag(bucket: str, key: str, s3_client=None) -> str:
    """
    Return the S3 ETag without surrounding quotes.

    Note:
    - For single-part uploads, ETag is often the MD5 of the object.
    - For multipart uploads, it is not a plain MD5 and usually contains a dash.
    """
    metadata = get_s3_object_metadata(bucket, key, s3_client=s3_client)
    return metadata["ETag"].strip('"')


def should_download_s3(bucket: str, key: str, stored_md5: str, s3_client=None) -> bool:
    """
    Decide whether an S3 object should be downloaded.

    Returns:
        True if download is needed, False if local file is up to date.

    Behavior:
    - If the S3 ETag looks like a single-part MD5, compare directly to stored_md5.
    - If it looks like a multipart ETag, return True because direct MD5 comparison
      is not reliable.
    """
    remote_etag = get_s3_etag(bucket, key, s3_client=s3_client)

    # Multipart upload ETags are not plain MD5 hashes
    if "-" in remote_etag:
        return True

    return remote_etag.lower() != stored_md5.lower()


def download_s3_file(bucket: str, key: str, output_dir: str, s3_client=None) -> str:
    """
    Download an S3 object into output_dir.

    Returns:
        The absolute path of the downloaded file.
    """
    s3 = s3_client or get_s3_client()
    output_path = Path(output_dir) / Path(key).name
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        s3.download_file(bucket, key, str(output_path))
    except (ClientError, BotoCoreError) as e:
        raise RuntimeError(f"Failed to download s3://{bucket}/{key}: {e}") from e

    return str(output_path)