"""
S3 storage: upload climate data files to AWS S3.

Security defaults (aligned with Suyana conventions):
  - AES-256 server-side encryption (SSEAlgorithm: AES256 + BucketKeyEnabled)
  - All public access blocked
  - Parallel uploads via ThreadPoolExecutor
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional


# ---------------------------------------------------------------------------
# boto3 import guard
# ---------------------------------------------------------------------------

def _get_s3_client(region: str):
    try:
        import boto3
    except ImportError:
        raise ImportError("boto3 is required for S3 support: pip install boto3")
    return boto3.client("s3", region_name=region)


# ---------------------------------------------------------------------------
# Bucket management
# ---------------------------------------------------------------------------

def ensure_bucket(bucket_name: str, region: str = "us-east-1") -> None:
    """
    Create an S3 bucket if it does not exist, applying:
      - AES-256 server-side encryption
      - Full public access block

    Parameters:
        bucket_name : Name of the S3 bucket.
        region      : AWS region (default: us-east-1).
    """
    s3 = _get_s3_client(region)

    # Check existence
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"[S3] Bucket exists: s3://{bucket_name}")
        return
    except Exception:
        pass  # bucket doesn't exist — create it

    # Create
    if region == "us-east-1":
        s3.create_bucket(Bucket=bucket_name)
    else:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )
    print(f"[S3] Created bucket: s3://{bucket_name}  (region: {region})")

    # AES-256 encryption
    s3.put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration={
            "Rules": [
                {
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "AES256"
                    },
                    "BucketKeyEnabled": True,
                }
            ]
        },
    )
    print(f"[S3] AES-256 encryption applied to s3://{bucket_name}")

    # Block all public access
    s3.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls":      True,
            "IgnorePublicAcls":     True,
            "BlockPublicPolicy":    True,
            "RestrictPublicBuckets": True,
        },
    )
    print(f"[S3] Public access blocked on s3://{bucket_name}")


# ---------------------------------------------------------------------------
# Upload helpers
# ---------------------------------------------------------------------------

def upload_file(
    local_path: str,
    bucket: str,
    s3_key: str,
    region: str = "us-east-1",
) -> str:
    """
    Upload a single file to S3.

    Parameters:
        local_path : Local file path.
        bucket     : S3 bucket name.
        s3_key     : S3 object key (path within the bucket).
        region     : AWS region.

    Returns:
        S3 URI of the uploaded object (s3://bucket/key).
    """
    s3 = _get_s3_client(region)
    s3.upload_file(local_path, bucket, s3_key)
    uri = f"s3://{bucket}/{s3_key}"
    print(f"[S3] ↑ {os.path.basename(local_path)} → {uri}")
    return uri


def upload_files(
    local_paths: List[str],
    bucket: str,
    prefix: str = "",
    region: str = "us-east-1",
    max_workers: int = 5,
    create_bucket: bool = True,
) -> List[str]:
    """
    Upload multiple files to S3 in parallel.

    Parameters:
        local_paths   : List of local file paths to upload.
        bucket        : S3 bucket name.
        prefix        : S3 key prefix (e.g. 'climate-data/lead_001/drought').
        region        : AWS region.
        max_workers   : Number of parallel upload threads.
        create_bucket : If True (default), create the bucket if it doesn't exist.

    Returns:
        Sorted list of S3 URIs.
    """
    if not local_paths:
        return []

    if create_bucket:
        ensure_bucket(bucket, region)

    def _upload(path: str) -> str:
        filename = os.path.basename(path)
        key = f"{prefix}/{filename}".lstrip("/") if prefix else filename
        return upload_file(path, bucket, key, region)

    uris: List[str] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_upload, p): p for p in local_paths}
        for future in as_completed(futures):
            try:
                uris.append(future.result())
            except Exception as exc:
                print(f"[S3] Upload failed for {futures[future]}: {exc}")

    return sorted(uris)
