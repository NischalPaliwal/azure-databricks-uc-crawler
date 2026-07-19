from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from databricks.connect import DatabricksSession
from databricks.sdk.core import Config
from databricks.sdk.errors import PermissionDenied
from pathlib import Path
import yaml
import os

_DEFAULT_CONFIG = {
    "inventory_schema": "crawler_inventory",
    "min_dbr_version": "11.3",
    "disallowed_access_modes": ["NO_ISOLATION", "LEGACY_SINGLE_USER"],
    "submit_runs_lookback_days": 30,
    "max_workers": 8,
    "dashboard_output": "excel",
}

def load_env(dotenv_path: str | None = None) -> None:
    load_dotenv(dotenv_path, override=False)

def get_workspace_client(profile: str | None = None) -> WorkspaceClient:
    w = WorkspaceClient(
        host=os.getenv("DBX_WORKSPACE_URL"),
        client_id=os.getenv("DBX_CLIENT_ID"),
        client_secret=os.getenv("DBX_CLIENT_SECRET")
    )
    return w

def get_spark_session() -> DatabricksSession:
    config = Config(
        host=os.getenv("DBX_WORKSPACE_URL"),
        token=os.getenv("DBX_TOKEN"),
        cluster_id=os.getenv("DBX_CLUSTER_ID")
    )
    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
    return spark

def load_config(path: str = "config/config.yml") -> dict:
    config_path = Path(path)
    user_config: dict = {}

    if config_path.exists():
        with config_path.open("r") as f:
            user_config = yaml.safe_load(f) or {}
    else:
        user_config = {}

    config = {**_DEFAULT_CONFIG, **user_config}

    required_keys = ["inventory_schema", "min_dbr_version", "disallowed_access_modes"]
    missing = [k for k in required_keys if k not in config or config[k] in (None, "")]
    if missing:
        raise ValueError(f"config.yml is missing required keys: {missing}")

    return config

def validate_permissions(client: WorkspaceClient) -> list[str]:
    warnings: list[str] = []

    checks = {
        "current_user":         lambda: client.current_user.me(),
        "clusters":              lambda: list(client.clusters.list()),
        "cluster_policies":      lambda: list(client.cluster_policies.list()),
        "jobs":                  lambda: list(client.jobs.list(limit=1)),
        "pipelines":             lambda: list(client.pipelines.list_pipelines()),
        "global_init_scripts":   lambda: list(client.global_init_scripts.list()),
    }

    for name, probe in checks.items():
        try:
            probe()
        except PermissionDenied as e:
            msg = f"Missing or insufficient permission for '{name}': {e}"
            warnings.append(msg)
        except Exception as e:
            msg = f"Unexpected error probing '{name}': {e}"
            warnings.append(msg)

    try:
        me = client.current_user.me()
        is_admin = "admins" in [g.display for g in (me.groups or [])]
        if not is_admin:
            warnings.append(
                "Authenticated principal is not a workspace admin — grants "
                "and some workspace-wide inventory may be incomplete."
            )
    except Exception as e:
        warnings.append(f"Could not determine admin status: {e}")

    return warnings