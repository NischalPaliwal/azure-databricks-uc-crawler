from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame
from databricks.connect import DatabricksSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from .base import BaseCrawler

def fetch_global_init_scripts(client: WorkspaceClient) -> list[dict]:
    try:
        return [script.as_dict() for script in client.global_init_scripts.list()]
    except Exception as e:
        print(f"Warning: Failed to fetch global init scripts: {e}")
        return []

def parse_script_record(script: dict) -> dict:
    return {
        "script_id": script.get("script_id"),
        "script_name": script.get("name"),
        "created_by": script.get("created_by"),
        "enabled": script.get("enabled", False)
    }

class GlobalInitScriptsCrawler(BaseCrawler):
    inventory_table_name: str = "global_init_scripts"

    def __init__(self, spark: DatabricksSession, client: WorkspaceClient):
        super().__init__(spark)
        self.client = client

    def crawl(self) -> DataFrame:
        raw_scripts = fetch_global_init_scripts(self.client)
        parsed_scripts = [parse_script_record(script) for script in raw_scripts]

        schema = StructType([
            StructField("script_id", StringType(), True),
            StructField("script_name", StringType(), True),
            StructField("created_by", StringType(), True),
            StructField("enabled", BooleanType(), True)
        ])

        if not parsed_scripts:
            return self.spark.createDataFrame([], schema)

        return self.spark.createDataFrame(parsed_scripts, schema)
