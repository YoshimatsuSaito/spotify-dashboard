import pandas as pd
from transformers import pipeline

LIMIT = 100


class SentimentAnalyzer:
    def __init__(
        self, model: str = "distilbert/distilbert-base-uncased-finetuned-sst-2-english"
    ) -> None:
        """transformersで使うモデルの指定.

        一旦、英語表現だけに対応.
        """
        self._classifier = pipeline(task="sentiment-analysis", model=model)

    def classify(self, list_str: list[str]) -> pd.DataFrame:
        # メモリ対策で100件ずつにする
        list_sentiment_analysis = []
        for i in range(0, len(list_str), LIMIT):
            batch = list_str[i : i + LIMIT]
            list_sentiment_analysis.extend(self._classifier(batch))
        return pd.DataFrame(list_sentiment_analysis).reset_index(drop=True)
