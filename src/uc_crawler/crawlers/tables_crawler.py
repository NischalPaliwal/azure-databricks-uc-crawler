import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import DataFrame
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
from .base import BaseCrawler

def list_catalogs(spark: DatabricksSession) -> list[str]:
    """
    Returns all catalogs accessible to the current user/service principal.
    """
    try:
        rows = spark.sql("SHOW CATALOGS").collect()
        return [row.catalog for row in rows]
    except Exception as e:
        print(f"Warning: Failed to list catalogs: {e}")
        return []
    
def list_databases(spark: DatabricksSession, catalogs: list[str] | None = None) -> list[str]:
    """
    Returns schemas across the provided catalogs.
    Outputs strings in the format: "catalog_name.schema_name"
    """
    if catalogs is None:
        catalogs = list_catalogs(spark)
        
    all_schemas = []
    
    for catalog in catalogs:
        try:
            if catalog == "system":
                continue
                
            schemas = spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()
            for row in schemas:
                schema_name = getattr(row, "databaseName", getattr(row, "namespace", ""))
                if schema_name:
                    all_schemas.append(f"{catalog}.{schema_name}")
        except Exception as e:
            print(f"Could not list schemas in catalog '{catalog}': {e}")
            
    return all_schemas

def list_tables(spark: DatabricksSession, schema: str) -> list[str]:
    """
    Returns all table names within a specific UC catalog.schema
    """
    try:
        rows = spark.sql(f"SHOW TABLES IN {schema}").collect()
        return [row.tableName for row in rows]
    except Exception as e:
        print(f"Failed to list tables in {schema}: {e}")
        return []
    
def fetch_table_types(spark: DatabricksSession, schema: str) -> dict:
    """
    Queries information_schema.tables for the given catalog.schema to determine
    each table's table_type (MANAGED, EXTERNAL, VIEW). This is the only reliable
    way to tell views apart from tables in Unity Catalog.
    """
    try:
        catalog, schema_name = schema.split(".", 1)
        rows = spark.sql(
            f"SELECT table_name, table_type FROM {catalog}.information_schema.tables "
            f"WHERE table_schema = '{schema_name}'"
        ).collect()
        return {row.table_name: row.table_type for row in rows}
    except Exception as e:
        print(f"Failed to fetch table types in {schema}: {e}")
        return {}

def describe_table_detail(spark: DatabricksSession, schema: str, table: str, table_type: str | None = None) -> dict:
    """
    Runs DESCRIBE DETAIL to extract format, location, size, is_delta, and partitions.
    """
    record = {
        "catalog_schema": schema,
        "table": table,
        "table_type": table_type,
        "format": None,
        "location": None,
        "size": 0,
        "is_delta": False,
        "partition_columns": "[]",
        "error": None
    }

    try:
        detail_df = spark.sql(f"DESCRIBE DETAIL {schema}.{table}")
        row = detail_df.first()

        if row:
            record["format"] = getattr(row, "format", None)
            record["location"] = getattr(row, "location", None)
            record["size"] = getattr(row, "sizeInBytes", 0)
            record["is_delta"] = (record["format"] == "delta") if record["format"] else False
            
            part_cols = getattr(row, "partitionColumns", [])
            record["partition_columns"] = json.dumps(part_cols) if part_cols else "[]"
            
    except Exception as e:
        record["error"] = str(e)

    return record

def crawl_database(spark: DatabricksSession, schema: str) -> list[dict]:
    """
    Gets all tables in a catalog.schema and describes them.
    Views are recorded without DESCRIBE DETAIL, since that command only
    supports physical tables and raises on views.
    """
    tables = list_tables(spark, schema)
    table_types = fetch_table_types(spark, schema)
    table_records = []

    for table in tables:
        table_type = table_types.get(table, "UNKNOWN")

        if table_type == "VIEW":
            table_records.append({
                "catalog_schema": schema,
                "table": table,
                "table_type": "VIEW",
                "format": None,
                "location": None,
                "size": 0,
                "is_delta": False,
                "partition_columns": "[]",
                "error": None
            })
            continue

        detail = describe_table_detail(spark, schema, table, table_type)
        table_records.append(detail)

    return table_records

class TablesCrawler(BaseCrawler):
    inventory_table_name: str = "tables"

    def __init__(self, client: WorkspaceClient, spark: DatabricksSession):
        super().__init__(spark)
        self.client = client

    def crawl(self, max_workers: int = 8) -> DataFrame:
        catalogs = list_catalogs(self.spark)
        databases = list_databases(self.spark, catalogs)
        all_table_records = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_db = {
                executor.submit(crawl_database, self.spark, db): db
                for db in databases
            }

            for future in as_completed(future_to_db):
                db_name = future_to_db[future]
                try:
                    db_records = future.result()
                    all_table_records.extend(db_records)
                except Exception as e:
                    print(f"Failed crawling schema {db_name}: {e}")

        if not all_table_records:
            schema = StructType([
                StructField("catalog_schema", StringType(), True),
                StructField("table", StringType(), True),
                StructField("table_type", StringType(), True),
                StructField("format", StringType(), True),
                StructField("location", StringType(), True),
                StructField("size", LongType(), True),
                StructField("is_delta", BooleanType(), True),
                StructField("partition_columns", StringType(), True),
                StructField("error", StringType(), True)
            ])
            return self.spark.createDataFrame([], schema)
        
        return self.spark.createDataFrame(all_table_records)