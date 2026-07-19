import json
import re

import pandas as pd

_DBR_VERSION_RE = re.compile(r"^(\d+)\.(\d+)")


def parse_dbr_major_minor(spark_version: str):
    if not spark_version:
        return None
    match = _DBR_VERSION_RE.match(spark_version.strip())
    if not match:
        return None
    return (int(match.group(1)), int(match.group(2)))


def is_dbr_unsupported(spark_version: str, min_dbr_version: str) -> bool:
    version = parse_dbr_major_minor(spark_version)
    minimum = parse_dbr_major_minor(min_dbr_version)
    if version is None or minimum is None:
        return False
    return version < minimum


def dbr_finding(spark_version: str, min_dbr_version: str):
    if is_dbr_unsupported(spark_version, min_dbr_version):
        return f"not supported DBR: {spark_version}"
    return None


def access_mode_finding(access_mode: str, disallowed_access_modes: list):
    if access_mode and access_mode in (disallowed_access_modes or []):
        if access_mode == "NO_ISOLATION":
            return "No isolation shared clusters not supported in UC"
        return f"cluster type not supported : {access_mode}"
    return None


def assess_clusters(clusters_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    columns = ["cluster_id", "spark_version", "access_mode", "policy_id", "findings"]
    if clusters_df is None or clusters_df.empty:
        return pd.DataFrame(columns=columns)

    min_dbr = config.get("min_dbr_version")
    disallowed = config.get("disallowed_access_modes", [])

    def findings_for(row: dict) -> list:
        findings = []
        dbr = dbr_finding(row.get("spark_version"), min_dbr)
        if dbr:
            findings.append(dbr)
        access = access_mode_finding(row.get("access_mode"), disallowed)
        if access:
            findings.append(access)
        return findings

    result = clusters_df.copy()
    result["findings"] = result.apply(lambda r: findings_for(r.to_dict()), axis=1)
    return result


def assess_jobs(jobs_df: pd.DataFrame, clusters_assessed_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    columns = ["job_id", "job_name", "creator", "dbr_versions", "task_clusters", "findings"]
    if jobs_df is None or jobs_df.empty:
        return pd.DataFrame(columns=columns)

    min_dbr = config.get("min_dbr_version")

    cluster_findings_map = {}
    if clusters_assessed_df is not None and not clusters_assessed_df.empty:
        cluster_findings_map = {
            row["cluster_id"]: row["findings"]
            for _, row in clusters_assessed_df.iterrows()
        }

    def findings_for(row: dict) -> list:
        findings = []

        for dbr_version in (row.get("dbr_versions") or []):
            finding = dbr_finding(dbr_version, min_dbr)
            if finding and finding not in findings:
                findings.append(finding)

        try:
            task_clusters = json.loads(row.get("task_clusters") or "[]")
        except (TypeError, ValueError):
            task_clusters = []

        for task in task_clusters:
            existing_cluster_id = task.get("existing_cluster_id")
            for finding in cluster_findings_map.get(existing_cluster_id, []):
                if finding not in findings:
                    findings.append(finding)

        return findings

    result = jobs_df.copy()
    result["findings"] = result.apply(lambda r: findings_for(r.to_dict()), axis=1)
    return result


def assess_tables(tables_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["catalog_schema", "table", "table_type", "format", "location", "is_delta", "findings"]
    if tables_df is None or tables_df.empty:
        return pd.DataFrame(columns=columns)

    def findings_for(row: dict) -> list:
        if str(row.get("table_type")).upper() == "VIEW":
            return []

        findings = []
        fmt = row.get("format")
        if fmt and str(fmt).lower() != "delta":
            findings.append(f"Non-DELTA format: {str(fmt).upper()}")

        location = str(row.get("location") or "")
        if location.startswith("dbfs:/mnt/") or location.startswith("/mnt/"):
            findings.append("Data is in DBFS Mount")
        elif location.startswith("dbfs:/"):
            findings.append("Data is in DBFS Root")

        return findings

    result = tables_df.copy()
    result["findings"] = result.apply(lambda r: findings_for(r.to_dict()), axis=1)
    return result


def build_readiness_summary(assessed: dict) -> pd.DataFrame:
    rows = []
    for object_type, df in assessed.items():
        if df is None or df.empty:
            rows.append({"object_type": object_type, "readiness": None})
            continue
        total = len(df)
        with_issues = int(df["findings"].apply(lambda f: bool(f)).sum())
        readiness = round(100 * (1 - with_issues / total), 1) if total else None
        rows.append({"object_type": object_type, "readiness": readiness})
    return pd.DataFrame(rows)


def build_assessment_summary(assessed: dict) -> pd.DataFrame:
    counts: dict = {}
    for df in assessed.values():
        if df is None or df.empty:
            continue
        for findings in df["findings"]:
            for finding in findings:
                counts[finding] = counts.get(finding, 0) + 1

    rows = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    return pd.DataFrame(rows, columns=["finding", "count"])


def build_database_summary(tables_assessed: pd.DataFrame, grants_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["database", "tables", "views", "unsupported", "dbfs_root", "delta_tables", "total_grants", "granted_principals"]
    if tables_assessed is None or tables_assessed.empty:
        return pd.DataFrame(columns=columns)

    df = tables_assessed.copy()
    location = df.get("location", pd.Series(dtype=str)).astype(str)

    df["is_view"] = df.get("table_type", "").astype(str).str.upper().eq("VIEW")
    df["is_unsupported"] = df["findings"].apply(lambda f: bool(f))
    df["is_dbfs_root"] = location.str.startswith("dbfs:/") & ~location.str.startswith("dbfs:/mnt/")
    df["is_delta"] = df.get("is_delta", False)

    summary = df.groupby("catalog_schema").agg(
        tables=("table", "count"),
        views=("is_view", "sum"),
        unsupported=("is_unsupported", "sum"),
        dbfs_root=("is_dbfs_root", "sum"),
        delta_tables=("is_delta", "sum"),
    ).reset_index().rename(columns={"catalog_schema": "database"})

    if grants_df is not None and not grants_df.empty:
        grant_counts = grants_df.groupby("database").agg(
            total_grants=("principal", "count"),
            granted_principals=("principal", pd.Series.nunique),
        ).reset_index()
        summary = summary.merge(grant_counts, on="database", how="left")

    return summary


def build_assessment(pandas_results: dict, config: dict) -> dict:
    """
    Runs UCX-style compatibility checks over the crawled inventory and produces
    the readiness/finding rollups that back the "Assessment Overview" sheet.
    """
    clusters_assessed = assess_clusters(pandas_results.get("clusters"), config)
    jobs_assessed = assess_jobs(pandas_results.get("jobs"), clusters_assessed, config)
    tables_assessed = assess_tables(pandas_results.get("tables"))

    assessed = {
        "clusters": clusters_assessed,
        "jobs": jobs_assessed,
        "tables": tables_assessed,
    }

    return {
        "clusters": clusters_assessed,
        "jobs": jobs_assessed,
        "tables": tables_assessed,
        "readiness_summary": build_readiness_summary(assessed),
        "assessment_summary": build_assessment_summary(assessed),
        "database_summary": build_database_summary(tables_assessed, pandas_results.get("grants")),
    }
