from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame
from databricks.connect import DatabricksSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType
from pyspark.sql.functions import udf, col
from .base import BaseCrawler

def fetch_external_locations(client: WorkspaceClient) -> list[dict]:
    """
    Fetches Unity Catalog external locations. 
    Gracefully handles environments where UC is not enabled or permissions are lacking.
    """
    try:
        return [loc.as_dict() for loc in client.external_locations.list()]
    except Exception as e:
        print(f"Warning: Failed to fetch external locations (UC might not be enabled): {e}")
        return []
    
def derive_storage_summary(tables_df: DataFrame, mounts_df: DataFrame) -> DataFrame:
    """
    Classifies storage paths for all tables and aggregates them to feed the "Storage Summary" widget.
    It groups table counts by storage type (EXTERNAL, DBFS_MOUNT, DBFS_ROOT, UNSUPPORTED).
    """

    mount_points = []

    if mounts_df and not mounts_df.isEmpty():
        if "mountPoint" in mounts_df.columns:
            mount_points = [row["mountPoint"] for row in mounts_df.select("mountPoint").collect()]

    def classify(location: str) -> str:
        if not location:
            return "UNSUPPORTED"
            
        external_prefixes = ("s3://", "s3a://", "abfss://", "wasbs://", "gs://", "adl://")
        if location.startswith(external_prefixes):
            return "EXTERNAL"
            
        if location.startswith("dbfs:/mnt/") or location.startswith("/mnt/"):
            return "DBFS_MOUNT"
            
        if location.startswith("dbfs:/") or location.startswith("file:/dbfs/"):
            for mnt in mount_points:
                if mnt in location:
                    return "DBFS_MOUNT"
            return "DBFS_ROOT"
            
        return "UNSUPPORTED"

    classify_udf = udf(classify, StringType())

    if "location" not in tables_df.columns:
        raise ValueError("tables_df must contain a 'location' column to derive storage summary.")

    summary_df = (
        tables_df
        .withColumn("storage_type", classify_udf(col("location")))
        .groupBy("storage_type")
        .count()
        .withColumnRenamed("count", "table_count")
    )
    
    return summary_df

    
class ExternalLocationsCrawler(BaseCrawler):
    inventory_table_name: str = "external_locations"

    def __init__(self, spark: DatabricksSession, client: WorkspaceClient):
        super().__init__(spark)
        self.client = client

    def crawl(self) -> DataFrame:
        raw_locations = fetch_external_locations(self.client)
        parsed_locations = []

        for loc in raw_locations:
            parsed_locations.append({
                "name": loc.get("name"),
                "url": loc.get("url"),
                "credential_name": loc.get("credential_name"),
                "owner": loc.get("owner"),
                "read_only": loc.get("read_only", False)
            })

        if not parsed_locations:
            schema = StructType([
                StructField("name", StringType(), True),
                StructField("url", StringType(), True),
                StructField("credential_name", StringType(), True),
                StructField("owner", StringType(), True),
                StructField("read_only", BooleanType(), True)
            ])
            return self.spark.createDataFrame([], schema)

        return self.spark.createDataFrame(parsed_locations)