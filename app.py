import os

import seaborn as sns
import streamlit as st
from dotenv import load_dotenv
from matplotlib import pyplot as plt

from modules.inmemory_db import InmemoryDB
from modules.load_config import load_config

load_dotenv()

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
DICT_CONFIG = load_config("./config/config.yml")
TABLE_NAME = DICT_CONFIG["table_name"]


@st.cache_resource(ttl=60 * 60 * 24, show_spinner=True)
def _cached_inmemory_db() -> InmemoryDB:
    db = InmemoryDB(bucket_name=BUCKET_NAME)
    db.create_inmemory_db(
        data_key=DICT_CONFIG["concat_data_key"], table_name=TABLE_NAME
    )
    return db


db = _cached_inmemory_db()

st.title("Data From Spotify API")

list_feature = DICT_CONFIG["features"]
list_genre = db.execute_query(f"SELECT DISTINCT genre from {TABLE_NAME}")[
    "genre"
].tolist()
list_genre = sorted(list_genre)

feature = st.selectbox("表示したい特徴を選択", list_feature, index=0)
genre = st.selectbox("表示したいジャンルを選択", list_genre, index=0)

if feature == "sentiment":
    df = db.execute_query(
        f"SELECT label, score, market, year FROM {TABLE_NAME} WHERE genre = '{genre}'"
    )
    df["sentiment"] = df["score"] * df["label"].map({"POSITIVE": 1, "NEGATIVE": -1})
else:
    df = db.execute_query(
        f"SELECT {feature}, market, year FROM {TABLE_NAME} WHERE genre = '{genre}'"
    )

fig, ax = plt.subplots()
sns.lineplot(data=df, x="year", y=feature, hue="market", ax=ax)

st.pyplot(fig)
