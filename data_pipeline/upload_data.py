import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
import os
from . import config
import pandas as pd
import io
import json


class S3Error(Exception):
    """Custom exception for S3 operations"""
    pass


class DataUploader:
    def __init__(self):
        try:
            self.s3 = boto3.client(
                "s3",
                aws_access_key_id=config.aws_access_key_id,
                aws_secret_access_key=config.aws_secret_access_key,
            )
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise S3Error(f"AWS credentials not configured properly: {e}")

    def upload_to_s3_with_path(
        self, df_location: str, bucket_name: str, file_name: str
    ) -> None:
        try:
            self.s3.upload_file(df_location, bucket_name, file_name)
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            raise S3Error(f"Failed to upload {df_location} to s3://{bucket_name}/{file_name}: {error_code} - {e}")
        except FileNotFoundError:
            raise S3Error(f"Local file not found: {df_location}")

    def upload_to_s3(self, df: pd.DataFrame, bucket_name: str, file_name: str) -> None:
        try:
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            self.s3.put_object(
                Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue()
            )
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            raise S3Error(f"Failed to upload DataFrame to s3://{bucket_name}/{file_name}: {error_code} - {e}")

    def list_keys(self, bucket: str, prefix: str = "") -> list[str]:
        """
        Lists all object keys under the given prefix in the specified S3 bucket.
        """
        try:
            keys = []
            paginator = self.s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

            for page in pages:
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])

            return keys
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            raise S3Error(f"Failed to list keys in s3://{bucket}/{prefix}: {error_code} - {e}")

    def decode_utf_8(self, data: bytes) -> str:
        return data.decode("utf-8")

    def download_from_s3(self, bucket_name: str, s3_file_path: str) -> str:
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=s3_file_path)
            if isinstance(response["Body"], bytes):
                response_string = response["Body"].read().decode("utf-8")
            else:
                response_string = response["Body"].read()
            return response_string
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == 'NoSuchKey':
                raise S3Error(f"File not found in S3: s3://{bucket_name}/{s3_file_path}")
            elif error_code == 'NoSuchBucket':
                raise S3Error(f"Bucket does not exist: {bucket_name}")
            elif error_code == 'AccessDenied':
                raise S3Error(f"Access denied to s3://{bucket_name}/{s3_file_path}. Check your AWS credentials and permissions.")
            else:
                raise S3Error(f"Failed to download s3://{bucket_name}/{s3_file_path}: {error_code} - {e}")

    def convert_csv_to_df(self, csv_content: str) -> pd.DataFrame:
        if isinstance(csv_content, bytes):
            csv_content = csv_content.decode("utf-8")
        return pd.read_csv(io.StringIO(csv_content))


if __name__ == "__main__":
    uploader = DataUploader()
    df = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_combined)
    print(df.columns)
    df.to_csv("data/outputs/temp_combined.csv", index=False)
    print(df.head())
    print("end of script")
