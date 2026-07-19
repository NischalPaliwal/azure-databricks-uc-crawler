import json
from datetime import datetime, timedelta, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunType
from pyspark.sql import DataFrame
from databricks.connect import DatabricksSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from .base import BaseCrawler

def fetch_submit_runs(client: WorkspaceClient, lookback_days: int = 30) -> list[dict]:
    """
    Fetches raw jobs.list_runs() results filtered by SUBMIT_RUN within the lookback window.
    """
    lookback_ms = int((datetime.now(timezone.utc) - timedelta(days=lookback_days)).timestamp() * 1000)

    runs_iterator = client.jobs.list_runs(
        run_type=RunType.SUBMIT_RUN,
        start_time_from=lookback_ms
    )

    return [run.as_dict() for run in runs_iterator]

class SubmitRunsCrawler(BaseCrawler):
    inventory_table_name: str = "submit_runs"

    def __init__(self, client: WorkspaceClient, spark: DatabricksSession):
        super().__init__(spark)
        self.client = client

    def crawl(self) -> DataFrame:
        runs = fetch_submit_runs(self.client)
        parsed_runs = []

        for run in runs:
            parsed_runs.append({
                "run_id": run.get("run_id"),
                "run_name": run.get("run_name"),
                "creator_user_name": run.get("creator_user_name"),
                "start_time": run.get("start_time"),
                "state_life_cycle_state": run.get("state", {}).get("life_cycle_state"),
                "state_result_state": run.get("state", {}).get("result_state")
            })

        if not parsed_runs:
            schema = StructType([
                StructField("run_id", LongType(), True),
                StructField("run_name", StringType(), True),
                StructField("creator_user_name", StringType(), True),
                StructField("start_time", LongType(), True),
                StructField("state_life_cycle_state", StringType(), True),
                StructField("state_result_state", StringType(), True)
            ])
            return self.spark.createDataFrame([], schema)
        
        return self.spark.createDataFrame(parsed_runs)