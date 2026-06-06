"""驗證 VM 能用 .env 裡的 key 連 S3。"""
import os
import boto3
from dotenv import load_dotenv

load_dotenv("/opt/ship-ais/.env")

s3 = boto3.client(
    "s3",
    region_name=os.environ["S3_REGION"],
    aws_access_key_id=os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["S3_SECRET_KEY"],
)
r = s3.list_objects_v2(Bucket=os.environ["S3_BUCKET"], Prefix="ship_ais/", MaxKeys=3)
for o in r.get("Contents", [])[:3]:
    print(o["Key"], o["Size"])
print("OK, S3 連線正常")
