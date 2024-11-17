"""パフォーマンス及びapi負荷軽減のため事前にデータを取得しておく"""

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from modules.extractor.spotipy_handler import SpotipyHandler
from modules.storage.upload_file_to_s3 import upload_file_to_s3
from modules.nlp.sentiment_analyzer import SentimentAnalyzer
from util.load_config import load_config

load_dotenv()

CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
BUCKET_NAME = os.environ["BUCKET_NAME"]


def main() -> None:
    # 設定の読み込み
    config_path = Path("./config/config.yml")
    dict_config = load_config(config_path)
    start_year = dict_config["start_year"]
    end_year = dict_config["end_year"]
    list_market = dict_config["markets"]
    list_genres = dict_config["genres"]
    num_to_search = dict_config["num_to_search"]

    # データの取得
    sh = SpotipyHandler(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    for market in list_market:
        for year in range(start_year, end_year + 1):
            for genre in list_genres:
                print(f"Getting data of {market}-{year}-{genre} from api")
                df_result_search = sh.search(
                    year=year, market=market, genre=genre, num_to_search=num_to_search
                )
                df_audio_features = sh.get_audio_features(
                    df_result_search["track_id"].tolist()
                )
                df = pd.merge(
                    df_audio_features,
                    df_result_search.loc[:, ["name", "popularity", "track_id"]],
                    how="left",
                    left_on=["id"],
                    right_on=["track_id"],
                ).reset_index(drop=True)

                # タイトルの感情分析結果の追加
                sa = SentimentAnalyzer()
                df_sa = sa.classify(df["name"].tolist())
                df = pd.concat([df, df_sa], axis=1)

                # データの保存
                upload_file_to_s3(
                    df=df,
                    file_key=f"{market}/{year}/{genre}.csv",
                    bucket_name=BUCKET_NAME,
                )


if __name__ == "__main__":
    main()
