from abc import ABC, abstractmethod
from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from datetime import datetime, timezone


class BaseCrawler(ABC):
    inventory_table_name: str = ""

    def __init__(self, spark: DatabricksSession):
        if not self.inventory_table_name:
            raise ValueError(f"{self.__class__.__name__} must set inventory_table_name")
        self.spark = spark

    @abstractmethod
    def crawl(self) -> DataFrame:
        raise NotImplementedError
    
    def write_table(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
        df.write.mode(mode).format("delta").saveAsTable(table_name)

    def read_table(self, table_name: str) -> DataFrame:
        return self.spark.table(table_name)

    def table_exists(self, table_name: str) -> bool:
        return self.spark.catalog.tableExists(table_name)
    
    def write_to_delta(self, df: DataFrame, mode: str = "overwrite") -> None:
        stamped_df = df.withColumn("_crawled_at", lit(datetime.now(timezone.utc).isoformat()))
        self.write_table(stamped_df, self.inventory_table_name, mode)

    def read_from_delta(self) -> DataFrame:
        return self.read_table(self.inventory_table_name)
    
    def run(self, force_refresh: bool = False) -> DataFrame:
        """
        Entry point every call should
        use instead of calling crawl() directly.

        - force_refresh=False and a cached table exists -> read from Delta.
        - Otherwise -> crawl fresh, write to Delta, return the new DataFrame.
        - A failed crawl does not raise past this point by default; it logs
          and re-raises, letting the orchestration layer decide whether one
          crawler's failure should block the rest (see cli.run_all_crawlers).
        """
        cache_available = self.table_exists(self.inventory_table_name)

        if not force_refresh and cache_available:
            return self.read_from_delta()
        
        try:
            df = self.crawl()
        except Exception as e:
            raise e
        
        self.write_to_delta(df)
        return df