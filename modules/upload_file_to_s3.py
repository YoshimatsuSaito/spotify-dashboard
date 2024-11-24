from io import BytesIO

import boto3
import pandas as pd


def upload_file_to_s3(
    df: pd.DataFrame,
    file_key: str,
    bucket_name: str,
) -> None:
    """S3にファイルをアップロード"""

    s3 = boto3.client("s3")
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(
        Bucket=bucket_name,
        Key=file_key,
        Body=csv_buffer.getvalue(),
    )
