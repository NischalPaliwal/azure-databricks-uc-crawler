from pyspark.sql import DataFrame
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from pyspark.sql.types import StructType, StructField, StringType
from .base import BaseCrawler
from .tables_crawler import list_catalogs, list_databases

def list_udfs(spark: DatabricksSession, schema: str) -> list[str]:
    """
    Returns a list of User Defined Functions (UDFs) in the specified database/schema.
    Filters to return only USER functions, ignoring built-in Spark functions.
    """
    try:
        rows = spark.sql(f"SHOW USER FUNCTIONS IN {schema}").collect()
        return [getattr(row, "function", list(row.asDict().values())[0]) for row in rows]
    except Exception as e:
        print(f"Failed to list UDFs in {schema}: {e}")
        return []
    
def describe_udf(spark: DatabricksSession, schema: str, function_name: str) -> dict:
    """
    Runs DESCRIBE FUNCTION EXTENDED to extract input/return types, deterministic flag, and body.
    """
    record = {
        "schema": schema,
        "function_name": function_name,
        "input_types": None,
        "return_type": None,
        "deterministic": None,
        "body": None,
        "error": None
    }
    
    try:
        fq_function = f"{schema}.{function_name}"
        rows = spark.sql(f"DESCRIBE FUNCTION EXTENDED {fq_function}").collect()
        
        for row in rows:
            row_dict = row.asDict()
            if "info_name" in row_dict and "info_value" in row_dict:
                key = str(row_dict["info_name"]).strip().lower()
                val = str(row_dict["info_value"]).strip()
                
                if "deterministic" in key:
                    record["deterministic"] = val
                elif "body" in key or "expression" in key:
                    record["body"] = val
                elif "return type" in key or "returns" in key:
                    record["return_type"] = val
                elif "arguments" in key or "signature" in key:
                    record["input_types"] = val
                    
    except Exception as e:
        record["error"] = str(e)
        
    return record

class UDFsCrawler(BaseCrawler):
    inventory_table_name: str = "udfs"

    def __init__(self, spark: DatabricksSession, client: WorkspaceClient):
        super().__init__(spark)
        self.client = client

    def crawl(self) -> DataFrame:
        catalogs = list_catalogs(self.spark)
        databases = list_databases(catalogs)

        all_udf_records = []

        for db in databases:
            udfs = list_udfs(self.spark, db)
            for udf in udfs:
                record = describe_udf(self.spark, db, udf)
                all_udf_records.append(record)

        if not all_udf_records:
            schema = StructType([
                StructField("database", StringType(), True),
                StructField("function_name", StringType(), True),
                StructField("input_types", StringType(), True),
                StructField("return_type", StringType(), True),
                StructField("deterministic", StringType(), True),
                StructField("body", StringType(), True),
                StructField("error", StringType(), True)
            ])
            return self.spark.createDataFrame([], schema)

        return self.spark.createDataFrame(all_udf_records)