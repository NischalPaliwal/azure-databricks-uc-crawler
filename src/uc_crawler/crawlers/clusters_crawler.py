from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterDetails
from pyspark.sql import DataFrame
from databricks.connect import DatabricksSession
from pyspark.sql.types import StructType, StructField, StringType
from .base import BaseCrawler

def fetch_clusters(client: WorkspaceClient) -> list[ClusterDetails]:
    """
    Fetches client.clusters.list() results and returns them as a list of dictionaries.
    """
    # client.clusters.list() returns an Iterable[ClusterDetails].
    return [cluster.as_dict() for cluster in client.clusters.list()]

def parse_cluster_record(cluster: ClusterDetails) -> dict:
    """
    Extracts cluster_id, spark_version, access_mode, and policy_id from a ClusterDetails object.
    """
    access_mode = None
    if cluster.data_security_mode:
        access_mode = cluster.data_security_mode.value

    return {
        "cluster_id": cluster.cluster_id,
        "spark_version": cluster.spark_version,
        "access_mode": access_mode,
        "policy_id": cluster.policy_id
    }

class ClusterCrawler(BaseCrawler):
    inventory_table_name = "clusters"

    def __init__(self, spark: DatabricksSession, client: WorkspaceClient):
        super().__init__(spark)
        self.client = client

    def crawl(self) -> DataFrame:
        clusters = self.client.clusters.list()
        parsed_records = [parse_cluster_record(cluster) for cluster in clusters]
        schema = StructType([
            StructField("cluster_id", StringType(), True),
            StructField("spark_version", StringType(), True),
            StructField("access_mode", StringType(), True),
            StructField("policy_id", StringType(), True)
        ])
        if not parsed_records:
            return self.spark.createDataFrame([], schema)
        
        return self.spark.createDataFrame(parsed_records, schema)