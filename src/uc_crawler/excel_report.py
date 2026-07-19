import json

import pandas as pd

SHEET_ORDER = [
    ("Assessment Overview", "overview"),
    ("Readiness Summary", "readiness_summary"),
    ("Assessment Summary", "assessment_summary"),
    ("Database Summary", "database_summary"),
    ("Tables", "tables"),
    ("UDFs", "udfs"),
    ("Storage Summary", "storage_summary"),
    ("External Locations", "external_locations"),
    ("Mount Points", "mounts"),
    ("Clusters", "clusters"),
    ("Cluster Policies", "cluster_policies"),
    ("Jobs", "jobs"),
    ("Submit Runs", "submit_runs"),
    ("Pipelines", "pipelines"),
    ("Global Init Scripts", "global_init_scripts"),
    ("Grants", "grants"),
]


def _stringify_nested_cells(df: pd.DataFrame) -> pd.DataFrame:
    """Excel cells can't hold list/dict values; JSON-encode any column that has them."""
    out = df.copy()
    for col in out.columns:
        if out[col].apply(lambda v: isinstance(v, (list, dict))).any():
            out[col] = out[col].apply(lambda v: json.dumps(v) if isinstance(v, (list, dict)) else v)
    return out


def _autosize_columns(worksheet, df: pd.DataFrame) -> None:
    for i, col in enumerate(df.columns, start=1):
        sample = [str(v) for v in df[col].tolist()[:200]]
        max_len = max([len(str(col))] + [len(v) for v in sample]) if sample else len(str(col))
        worksheet.column_dimensions[worksheet.cell(row=1, column=i).column_letter].width = min(max_len + 2, 60)


def build_overview_sheet(pandas_results: dict, assessment_data: dict) -> pd.DataFrame:
    def count(df):
        return 0 if df is None else len(df)

    tables_df = assessment_data.get("tables")
    if tables_df is not None and not tables_df.empty and "table_type" in tables_df.columns:
        is_view = tables_df["table_type"].astype(str).str.upper().eq("VIEW")
        total_views = int(is_view.sum())
        total_tables = count(tables_df) - total_views
    else:
        total_views = 0
        total_tables = count(tables_df)

    raw_tables = pandas_results.get("tables")
    total_databases = raw_tables["catalog_schema"].nunique() if raw_tables is not None and not raw_tables.empty else 0

    metrics = {
        "Total Databases": total_databases,
        "Total Tables": total_tables,
        "Total Views": total_views,
        "Total Storage Locations": count(pandas_results.get("external_locations")) + count(pandas_results.get("mounts")),
        "Total Jobs": count(pandas_results.get("jobs")),
        "Total Clusters": count(pandas_results.get("clusters")),
        "Total Pipelines": count(pandas_results.get("pipelines")),
        "Total UDFs": count(pandas_results.get("udfs")),
    }
    return pd.DataFrame(list(metrics.items()), columns=["metric", "value"])


def export_to_excel(pandas_results: dict, assessment_data: dict, output_path: str) -> None:
    """
    Writes the crawled inventory plus computed assessment/readiness rollups
    to a single multi-sheet Excel workbook, mirroring the UCX assessment
    dashboard layout (Overview, Readiness, Findings, Database Summary, ...).
    """
    sheets = dict(pandas_results)
    sheets["clusters"] = assessment_data.get("clusters", sheets.get("clusters"))
    sheets["jobs"] = assessment_data.get("jobs", sheets.get("jobs"))
    sheets["tables"] = assessment_data.get("tables", sheets.get("tables"))
    sheets["readiness_summary"] = assessment_data.get("readiness_summary")
    sheets["assessment_summary"] = assessment_data.get("assessment_summary")
    sheets["database_summary"] = assessment_data.get("database_summary")
    sheets["overview"] = build_overview_sheet(pandas_results, assessment_data)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, key in SHEET_ORDER:
            df = sheets.get(key)
            if df is None:
                df = pd.DataFrame([{"info": "No data collected"}])

            df = _stringify_nested_cells(df)
            safe_name = sheet_name[:31]
            df.to_excel(writer, sheet_name=safe_name, index=False)

            worksheet = writer.sheets[safe_name]
            worksheet.freeze_panes = "A2"
            _autosize_columns(worksheet, df)
