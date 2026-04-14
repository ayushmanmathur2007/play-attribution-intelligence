"""Data abstraction layer — swap between DuckDB (local) and BigQuery (GCP)."""

from abc import ABC, abstractmethod
from pathlib import Path
import duckdb
import pandas as pd


class DataClient(ABC):
    @abstractmethod
    def query(self, sql: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def close(self):
        pass


class DuckDBClient(DataClient):
    """Local — queries parquet/CSV files via DuckDB."""

    def __init__(self, data_dir: str = "data/synthetic"):
        self.data_dir = Path(data_dir)
        self.conn = duckdb.connect()
        self._register_tables()

    def _register_tables(self):
        """Register parquet/CSV files as virtual tables."""
        parquet_files = {
            "daily_metrics": "daily_metrics.parquet",
            "journey_aggregates": "journey_aggregates.parquet",
        }
        csv_files = {
            "initiative_calendar": "initiative_calendar.csv",
            "offer_catalog": "offer_catalog.csv",
            "metric_movements_golden": "metric_movements_golden.csv",
            "change_points": "change_points.csv",
            "confounder_log": "confounder_log.csv",
        }

        for table_name, filename in parquet_files.items():
            filepath = self.data_dir / filename
            if filepath.exists():
                self.conn.execute(
                    f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{filepath}')"
                )

        for table_name, filename in csv_files.items():
            filepath = self.data_dir / filename
            if filepath.exists():
                self.conn.execute(
                    f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_csv_auto('{filepath}')"
                )

    def query(self, sql: str) -> pd.DataFrame:
        return self.conn.execute(sql).fetchdf()

    def close(self):
        self.conn.close()


class BigQueryClient(DataClient):
    """GCP — queries BigQuery tables. Stub for portability."""

    def __init__(self, project_id: str, dataset: str):
        self.project_id = project_id
        self.dataset = dataset

    def query(self, sql: str) -> pd.DataFrame:
        raise NotImplementedError(
            "BigQuery client is a stub for GCP portability. "
            "Install google-cloud-bigquery and implement."
        )

    def close(self):
        pass


class DataClientFactory:
    @staticmethod
    def create(config: dict) -> DataClient:
        provider = config.get("provider", "duckdb")
        if provider == "duckdb":
            return DuckDBClient(data_dir=config.get("data_dir", "data/synthetic"))
        elif provider == "bigquery":
            return BigQueryClient(
                project_id=config.get("project_id", ""),
                dataset=config.get("dataset", ""),
            )
        else:
            raise ValueError(f"Unknown data provider: {provider}")
