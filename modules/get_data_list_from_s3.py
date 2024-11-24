import boto3
import pandas as pd


def get_data_list_from_s3(bucket_name: str) -> list[str]:
    """S3に保存済みのcsvのkeyを返す"""

    # Get all files key in s3
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name)
    list_all_file = []
    for page in page_iterator:
        if "Contents" in page:
            for obj in page["Contents"]:
                key = obj["Key"]
                if len(key.split("/")) == 3 and ".csv" in key.split("/")[-1]:
                    list_all_file.append(key)

    return list_all_file
