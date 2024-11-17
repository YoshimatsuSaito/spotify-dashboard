from io import StringIO

import boto3
import pandas as pd


def load_csv_data(
    key: str,
    bucket_name: str,
) -> pd.DataFrame:
    """Load CSV data from S3 bucket with the given prefix"""

    s3 = boto3.resource("s3")
    content_object = s3.Object(
        bucket_name=bucket_name,
        key=key,
    )
    csv_content = content_object.get()["Body"].read().decode("utf-8")
    try:
        df = pd.read_csv(StringIO(csv_content))
    except:  # noqa
        df = pd.DataFrame()

    return df
