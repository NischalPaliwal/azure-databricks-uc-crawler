import json

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineStateInfo, PipelineSpec
from pyspark.sql import DataFrame
from databricks.connect import DatabricksSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType
from .base import BaseCrawler

def fetch_pipelines(client: WorkspaceClient) -> list[dict]:
    """
    Fetches raw DLT pipeline list results.
    """
    return [pipeline.as_dict() for pipeline in client.pipelines.list_pipelines()]

def parse_pipeline_record(pipeline: any) -> dict:
    """
    Extracts cluster, DLT channel, serverless status, and target catalog info.
    """
    spec = getattr(pipeline, "spec", None)

    clusters = []

    if spec and getattr(spec, "clusters", None):
        for cluster in spec.clusters:
            clusters.append({
                "label": getattr(cluster, "label", None),
                "node_type_id": getattr(cluster, "node_type_id", None),
                "custom_tags": getattr(cluster, "custom_tags", None)
            })
    
    return {
        "pipeline_id": getattr(pipeline, "pipeline_id", None),
        "name": getattr(spec, "name", getattr(pipeline, "name", None)),
        "creator_user_name": getattr(pipeline, "creator_user_name", None),
        "target": getattr(spec, "target", None),
        "catalog": getattr(spec, "catalog", None),
        "channel": getattr(spec, "channel", None),
        "serverless": getattr(spec, "serverless", False),
        "clusters": json.dumps(clusters)
    }

class PipelinesCrawler(BaseCrawler):
    inventory_table_name: str = "pipelines"

    def __init__(self, spark: DatabricksSession, client: WorkspaceClient):
        super().__init__(spark)
        self.client = client

    def crawl(self) -> DataFrame:
        pipelines = self.client.pipelines.list_pipelines()
        parsed_records = []

        for base_pipeline in pipelines:
            try:
                full_pipeline = self.client.pipelines.get(base_pipeline.pipeline_id)
            except Exception:
                continue
            
            parsed_record = parse_pipeline_record(full_pipeline)
            parsed_records.append(parsed_record)

        schema = StructType([
            StructField("pipeline_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("creator_user_name", StringType(), True),
            StructField("target", StringType(), True),
            StructField("catalog", StringType(), True),
            StructField("clusters", StringType(), True)
        ])

        if not parsed_records:
            return self.spark.createDataFrame([], schema)
        
        return self.spark.createDataFrame(parsed_records, schema)