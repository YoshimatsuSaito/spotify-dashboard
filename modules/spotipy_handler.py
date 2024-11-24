from dataclasses import dataclass

import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

LIMIT = 50
TYPE = "track"
AUDIO_FEATURES_LIMIT = 100


@dataclass
class SearchResult:
    """search apiの結果を格納する"""

    duration_ms: float
    name: str
    popularity: int
    track_id: str


class SpotipyHandler:
    def __init__(self, client_id: str, client_secret: str) -> None:
        """認証系の初期化とAPI側から取得可能な情報のセット"""
        ccm = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
        self._sp = spotipy.Spotify(client_credentials_manager=ccm)
        self._available_genres = self._sp.recommendation_genre_seeds()["genres"]
        self._available_markets = self._sp.available_markets()["markets"]

    @property
    def available_genres(self) -> list[str]:
        return self._available_genres

    @property
    def available_markets(self) -> list[str]:
        return self._available_markets

    def search(
        self, year: int, genre: str, market: str, num_to_search: int
    ) -> pd.DataFrame:
        """year, genre, marketを指定して、APIから約num_to_search個の結果を取得する.
        結果は、使えそうなものだけ抽出する.
        曲の細かい情報は取得した曲のidを用いて、別APIメソッドから取得する.
        """
        list_result = []
        for offset in range(0, num_to_search, LIMIT):
            result = self._sp.search(
                q=f"year:{year} genre:{genre}",
                limit=LIMIT,
                offset=offset,
                type=TYPE,
                market=market,
            )
            for result_item in result["tracks"]["items"]:
                search_result = SearchResult(
                    duration_ms=result_item["duration_ms"],
                    name=result_item["name"],
                    popularity=result_item["popularity"],
                    track_id=result_item["id"],
                )
                list_result.append(search_result)
        return pd.DataFrame(list_result)

    def get_audio_features(self, list_track_id: list[str]) -> pd.DataFrame:
        """与えられた曲のidについて、特徴を取得して返す"""
        list_features = []
        for i in range(0, len(list_track_id), AUDIO_FEATURES_LIMIT):
            batch = list_track_id[i : i + AUDIO_FEATURES_LIMIT]
            list_features.extend(self._sp.audio_features(batch))
        return pd.DataFrame(list_features)
