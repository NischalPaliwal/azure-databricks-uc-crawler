import json

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Job
from pyspark.sql import DataFrame
from databricks.connect import DatabricksSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType
from .base import BaseCrawler

def fetch_jobs(client: WorkspaceClient) -> list[dict]:
    """
    Fetches raw jobs.list() results.
    """
    return [job.as_dict() for job in client.jobs.list()]

def extract_job_task_clusters(job: Job) -> list[dict]:
    """
    Extracts per-task cluster specifications directly from a fetched Job object.
    """
    task_clusters = []
    if job.settings and job.settings.tasks:
        for task in job.settings.tasks:
            task_clusters.append({
                "task_key": task.task_key,
                "existing_cluster_id": getattr(task, "existing_cluster_id", None),
                "job_cluster_key": getattr(task, "job_cluster_key", None)
            })
            
    return task_clusters

def parse_job_record(job: Job) -> dict:
    """
    Flattens the job record, extracting creator, DBR versions, and task clusters.
    """
    dbr_versions = []
    
    if job.settings and job.settings.job_clusters:
        for jc in job.settings.job_clusters:
            if jc.new_cluster and jc.new_cluster.spark_version:
                dbr_versions.append(jc.new_cluster.spark_version)
    
    task_clusters = extract_job_task_clusters(job)
                
    return {
        "job_id": job.job_id,
        "job_name": job.settings.name if job.settings else None,
        "creator": job.creator_user_name,
        "dbr_versions": list(set(dbr_versions)),
        "task_clusters": json.dumps(task_clusters) 
    }

class JobsCrawler(BaseCrawler):
    inventory_table_name: str = "jobs"

    def __init__(self, spark: DatabricksSession, client: WorkspaceClient):
        super().__init__(spark)
        self.client = client

    def crawl(self) -> DataFrame:
        jobs = self.client.jobs.list()
        parsed_records = []

        for base_job in jobs:
            try:
                full_job = self.client.jobs.get(base_job.job_id)
            except Exception:
                continue
            
            parsed_record = parse_job_record(full_job)
            parsed_records.append(parsed_record)

        schema = StructType([
            StructField("job_id", LongType(), True),
            StructField("job_name", StringType(), True),
            StructField("creator", StringType(), True),
            StructField("dbr_versions", ArrayType(StringType()), True),
            StructField("task_clusters", StringType(), True)
        ])

        if not parsed_records:
            return self.spark.createDataFrame([], schema)
        
        return self.spark.createDataFrame(parsed_records, schema)