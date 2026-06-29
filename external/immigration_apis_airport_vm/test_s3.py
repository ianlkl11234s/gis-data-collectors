"""Verify S3 credentials work from this VM."""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

import boto3
from botocore.exceptions import ClientError

BUCKET = os.environ.get("S3_BUCKET")
if not BUCKET:
    sys.exit("FATAL: S3_BUCKET not set")

client = boto3.client(
    "s3",
    region_name=os.environ.get("S3_REGION", "ap-southeast-2"),
    aws_access_key_id=os.environ.get("S3_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("S3_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY"),
)

key = "immigration_apis_airport/_test_connectivity.txt"
try:
    client.put_object(Bucket=BUCKET, Key=key, Body=b"vm connectivity test ok\n")
    print(f"✓ PUT s3://{BUCKET}/{key}")
    r = client.get_object(Bucket=BUCKET, Key=key)
    print(f"✓ GET: {r['Body'].read().decode().strip()}")
    client.delete_object(Bucket=BUCKET, Key=key)
    print(f"✓ DELETE OK")
except ClientError as e:
    sys.exit(f"FAIL: {e}")
