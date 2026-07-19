from pyspark.sql import DataFrame
from databricks.connect import DatabricksSession
from pyspark.sql.types import StructType, StructField, StringType
from .base import BaseCrawler
from typing import Any

def fetch_mounts(dbutils: Any) -> list[dict]:
    """
    Fetches dbutils.fs.mounts() output and formats it as a list of dictionaries.
    """
    try:
        mounts = dbutils.fs.mounts()
        return [
            {
                "mountPoint": mount.mountPoint,
                "source": mount.source
            }
            for mount in mounts
        ]
    except Exception as e:
        print(f"Warning: Failed to fetch mounts using dbutils: {e}")
        return []
    
class MountsCrawler(BaseCrawler):
    inventory_table_name: str = "mounts"

    def __init__(self, spark: DatabricksSession, dbutils: Any = None):
        super().__init__(spark)
        self.dbutils = dbutils

    def crawl(self) -> DataFrame:
        if not self.dbutils:
            raise ValueError("MountsCrawler requires 'dbutils' to be passed during initialization.")
        
        mount_records = fetch_mounts(self.dbutils)

        if not mount_records:
            schema = StructType([
                StructField("mountPoint", StringType(), True),
                StructField("source", StringType(), True)
            ])
            return self.spark.createDataFrame([], schema)
        
        return self.spark.createDataFrame(mount_records)