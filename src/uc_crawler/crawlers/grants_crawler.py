from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame
from databricks.connect import DatabricksSession
from pyspark.sql.types import StructType, StructField, StringType
from .base import BaseCrawler
from .tables_crawler import list_catalogs, list_databases

def fetch_grants(spark: DatabricksSession, object_type: str, object_name: str) -> list[dict]:
    """
    Runs SHOW GRANTS ON <object_type> <object_name> and returns the raw rows.
    Column names differ slightly across DBR versions, so callers should look up
    'Principal'/'principal' and 'ActionType'/'action_type' defensively.
    """
    try:
        rows = spark.sql(f"SHOW GRANTS ON {object_type} {object_name}").collect()
        return [row.asDict() for row in rows]
    except Exception as e:
        print(f"Failed to fetch grants on {object_type} {object_name}: {e}")
        return []

def parse_grant_row(database: str, object_type: str, object_name: str, row: dict) -> dict:
    return {
        "database": database,
        "object_type": object_type,
        "object_name": object_name,
        "principal": row.get("Principal") or row.get("principal"),
        "action_type": row.get("ActionType") or row.get("action_type")
    }

class GrantsCrawler(BaseCrawler):
    inventory_table_name: str = "grants"

    def __init__(self, spark: DatabricksSession, client: WorkspaceClient):
        super().__init__(spark)
        self.client = client

    def crawl(self) -> DataFrame:
        catalogs = list_catalogs(self.spark)
        databases = list_databases(self.spark, catalogs)

        all_grants = []
        for database in databases:
            for row in fetch_grants(self.spark, "SCHEMA", database):
                all_grants.append(parse_grant_row(database, "SCHEMA", database, row))

        schema = StructType([
            StructField("database", StringType(), True),
            StructField("object_type", StringType(), True),
            StructField("object_name", StringType(), True),
            StructField("principal", StringType(), True),
            StructField("action_type", StringType(), True)
        ])

        if not all_grants:
            return self.spark.createDataFrame([], schema)

        return self.spark.createDataFrame(all_grants, schema)
