import duckdb
import pandas as pd

from .load_csv_data_from_s3 import load_csv_data


class InmemoryDB:
    def __init__(self, bucket_name: str) -> None:
        """Initialize inmemory db"""
        self.con = duckdb.connect(database=":memory:")
        self.bucket_name = bucket_name

    def create_inmemory_db(self, data_key: str, table_name: str) -> None:
        """Create duckdb tables from csv data"""
        list_tables = self.con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        df = load_csv_data(bucket_name=self.bucket_name, key=data_key)  # noqa
        if table_name not in list_tables:
            self.con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute query"""
        return self.con.execute(query).fetchdf()

    def close(self) -> None:
        """Clone connection"""
        self.con.close()
