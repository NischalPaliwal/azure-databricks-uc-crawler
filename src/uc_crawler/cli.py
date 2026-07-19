import argparse
from pathlib import Path

from .client import get_spark_session, get_workspace_client, load_config, load_env, validate_permissions
from .crawlers.cluster_policies_crawler import ClusterPoliciesCrawler
from .crawlers.clusters_crawler import ClusterCrawler
from .crawlers.external_locations_crawler import ExternalLocationsCrawler, derive_storage_summary
from .crawlers.global_init_scripts_crawler import GlobalInitScriptsCrawler
from .crawlers.grants_crawler import GrantsCrawler
from .crawlers.jobs_crawler import JobsCrawler
from .crawlers.mounts_crawler import MountsCrawler
from .crawlers.pipelines_crawler import PipelinesCrawler
from .crawlers.submit_runs_crawler import SubmitRunsCrawler
from .crawlers.tables_crawler import TablesCrawler
from .crawlers.udfs_crawler import UDFsCrawler
from . import assessment
from .excel_report import export_to_excel


def get_dbutils(spark):
    """
    databricks-connect exposes dbutils via pyspark.dbutils.DBUtils rather than
    an implicit notebook global. Mount discovery is skipped (with a warning)
    if that's unavailable in the current runtime.
    """
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except Exception as e:
        print(f"Warning: dbutils unavailable ({e}); mount point discovery will be skipped.")
        return None


def run_all_crawlers(
    force_refresh: bool = False,
    config_path: str = "config/config.yml",
    output_path: str = "output/uc_assessment.xlsx",
) -> dict:
    load_env()
    config = load_config(config_path)
    client = get_workspace_client()
    spark = get_spark_session()

    for warning in validate_permissions(client):
        print(f"[permission warning] {warning}")

    dbutils = get_dbutils(spark)

    crawlers = {
        "clusters": ClusterCrawler(spark, client),
        "cluster_policies": ClusterPoliciesCrawler(spark, client),
        "jobs": JobsCrawler(spark, client),
        "pipelines": PipelinesCrawler(spark, client),
        "submit_runs": SubmitRunsCrawler(client, spark),
        "tables": TablesCrawler(client, spark),
        "udfs": UDFsCrawler(spark, client),
        "grants": GrantsCrawler(spark, client),
        "mounts": MountsCrawler(spark, dbutils),
        "external_locations": ExternalLocationsCrawler(spark, client),
        "global_init_scripts": GlobalInitScriptsCrawler(spark, client),
    }

    results = {}
    failures = {}

    for name, crawler in crawlers.items():
        print(f"Crawling {name}...")
        try:
            results[name] = crawler.run(force_refresh=force_refresh)
        except Exception as e:
            print(f"Crawler '{name}' failed: {e}")
            failures[name] = str(e)
            results[name] = None

    pandas_results = {
        name: (df.toPandas() if df is not None else None)
        for name, df in results.items()
    }

    if results.get("tables") is not None and results.get("mounts") is not None:
        pandas_results["storage_summary"] = derive_storage_summary(results["tables"], results["mounts"]).toPandas()
    else:
        pandas_results["storage_summary"] = None

    assessment_data = assessment.build_assessment(pandas_results, config)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    export_to_excel(pandas_results, assessment_data, str(output))

    print(f"Assessment written to {output}")
    if failures:
        print(f"Completed with {len(failures)} crawler failure(s): {list(failures.keys())}")

    return {
        "results": pandas_results,
        "assessment": assessment_data,
        "failures": failures,
        "output_path": str(output),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Crawl an Azure Databricks workspace and produce a Unity Catalog migration assessment workbook."
    )
    parser.add_argument("--config", default="config/config.yml", help="Path to config.yml")
    parser.add_argument("--output", default="output/uc_assessment.xlsx", help="Path to the output Excel workbook")
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Ignore cached Delta inventory tables and re-crawl everything from the workspace",
    )
    args = parser.parse_args()

    run_all_crawlers(force_refresh=args.force_refresh, config_path=args.config, output_path=args.output)


if __name__ == "__main__":
    main()