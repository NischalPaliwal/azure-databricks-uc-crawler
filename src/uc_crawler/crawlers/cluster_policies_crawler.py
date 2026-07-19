import json

from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame
from databricks.connect import DatabricksSession
from pyspark.sql.types import StructType, StructField, StringType
from .base import BaseCrawler

def fetch_cluster_policies(client: WorkspaceClient) -> list[dict]:
    try:
        return [policy.as_dict() for policy in client.cluster_policies.list()]
    except Exception as e:
        print(f"Warning: Failed to fetch cluster policies: {e}")
        return []

def parse_policy_record(policy: dict) -> dict:
    """
    Extracts the pinned Spark/DBR version (if any) from a policy's definition,
    matching UCX's "Cluster Policies" widget.
    """
    definition = policy.get("definition") or "{}"

    try:
        definition_dict = json.loads(definition) if isinstance(definition, str) else (definition or {})
    except (TypeError, ValueError):
        definition_dict = {}

    spark_version_spec = definition_dict.get("spark_version", {}) or {}
    policy_spark_version = spark_version_spec.get("value") if isinstance(spark_version_spec, dict) else None

    return {
        "policy_id": policy.get("policy_id"),
        "policy_name": policy.get("name"),
        "policy_spark_version": policy_spark_version
    }

class ClusterPoliciesCrawler(BaseCrawler):
    inventory_table_name: str = "cluster_policies"

    def __init__(self, spark: DatabricksSession, client: WorkspaceClient):
        super().__init__(spark)
        self.client = client

    def crawl(self) -> DataFrame:
        raw_policies = fetch_cluster_policies(self.client)
        parsed_policies = [parse_policy_record(policy) for policy in raw_policies]

        schema = StructType([
            StructField("policy_id", StringType(), True),
            StructField("policy_name", StringType(), True),
            StructField("policy_spark_version", StringType(), True)
        ])

        if not parsed_policies:
            return self.spark.createDataFrame([], schema)

        return self.spark.createDataFrame(parsed_policies, schema)