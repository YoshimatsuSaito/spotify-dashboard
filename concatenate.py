import os

import pandas as pd
from dotenv import load_dotenv

from modules.get_data_list_from_s3 import get_data_list_from_s3
from modules.load_csv_data_from_s3 import load_csv_data
from modules.upload_file_to_s3 import upload_file_to_s3

load_dotenv()
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
BUCKET_NAME = os.environ["BUCKET_NAME"]


def main() -> None:
    list_data_key = get_data_list_from_s3(BUCKET_NAME)
    list_df = []
    for key in list_data_key:
        df = load_csv_data(bucket_name=BUCKET_NAME, key=key)
        list_df.append(df)
    df_concat = pd.concat(list_df)
    upload_file_to_s3(df=df_concat, file_key="concat.csv", bucket_name=BUCKET_NAME)


if __name__ == "__main__":
    main()
