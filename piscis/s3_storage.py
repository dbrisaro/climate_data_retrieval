import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List


def _get_s3_client(region: str):
    try:
        import boto3
    except ImportError:
        raise ImportError("boto3 is required for S3 support: pip install boto3")
    return boto3.client("s3", region_name=region)


def ensure_bucket(bucket_name: str, region: str = "us-east-1") -> None:
    s3 = _get_s3_client(region)
    try:
        s3.head_bucket(Bucket=bucket_name)
        return
    except Exception:
        pass

    if region == "us-east-1":
        s3.create_bucket(Bucket=bucket_name)
    else:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )

    s3.put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration={
            "Rules": [{
                "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                "BucketKeyEnabled": True,
            }]
        },
    )
    s3.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )


def upload_file(local_path: str, bucket: str, s3_key: str, region: str = "us-east-1") -> str:
    s3 = _get_s3_client(region)
    s3.upload_file(local_path, bucket, s3_key)
    return f"s3://{bucket}/{s3_key}"


def upload_files(
    local_paths: List[str],
    bucket: str,
    prefix: str = "",
    region: str = "us-east-1",
    max_workers: int = 5,
    create_bucket: bool = True,
) -> List[str]:
    if not local_paths:
        return []

    if create_bucket:
        ensure_bucket(bucket, region)

    def _upload(path: str) -> str:
        filename = os.path.basename(path)
        key = f"{prefix}/{filename}".lstrip("/") if prefix else filename
        return upload_file(path, bucket, key, region)

    uris = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_upload, p): p for p in local_paths}
        for future in as_completed(futures):
            try:
                uris.append(future.result())
            except Exception as exc:
                print(f"s3 upload failed for {futures[future]}: {exc}")

    return sorted(uris)
