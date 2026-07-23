import json
import os
import subprocess
from typing import Any, Dict, List, Optional

import yaml


def _pipeline_configuration(secret_scope: str, credentials: Dict[str, str]) -> Dict[str, str]:
    """Same secret-ref shape _create_pipeline already builds for community connectors."""
    configuration = {}
    for cred_key, secret_key in credentials.items():
        configuration[cred_key] = f"secret('{secret_scope}', '{secret_key}')"
        configuration[f"spark.datasource.lakeflow_connect.{cred_key}"] = f"secret('{secret_scope}', '{secret_key}')"
    return configuration


def write_databricks_yml_community(
    workspace_url: str,
    catalog: str,
    schema: str,
    secret_scope: str,
    customer_name: str,
    connector_name: str,
    connector_config: Dict[str, Any],
    notebook_path: str,
    out_path: str,
) -> None:
    """Notebook-based DLT pipeline for a community connector - same fields
    _create_pipeline already sets via the SDK, just expressed as a bundle
    resource instead. Connection/notebook/repo-clone stay imperative SDK
    steps upstream of this; this function only owns the pipeline half."""
    pipeline_key = f"{connector_name}_ingestion"
    pipeline_name = f"{customer_name}_{connector_name}_ingestion"
    credentials = connector_config.get("credentials", {}) or {}

    bundle_yaml: Dict[str, Any] = {
        "bundle": {"name": pipeline_name},
        "workspace": {"host": workspace_url},
        "resources": {
            "pipelines": {
                pipeline_key: {
                    "name": pipeline_name,
                    "catalog": catalog,
                    "target": schema,
                    "channel": "PREVIEW",
                    "serverless": True,
                    "libraries": [{"notebook": {"path": notebook_path}}],
                    "configuration": _pipeline_configuration(secret_scope, credentials),
                }
            }
        },
    }
    _dump(bundle_yaml, out_path)


def _table_objects(catalog: str, schema: str, tables: List[Dict[str, Any]], source_catalog: Optional[str]) -> List[Dict[str, Any]]:
    objects = []
    for table in tables:
        src_schema = table.get("source_schema", schema)
        src_table = table["source_table"]
        dest_schema = table.get("destination_schema", schema)
        dest_table = table.get("destination_table", src_table)
        table_obj: Dict[str, Any] = {
            "destination_catalog": catalog,
            "destination_schema": dest_schema,
            "destination_table": dest_table,
            "source_schema": src_schema,
            "source_table": src_table,
        }
        if source_catalog:
            table_obj["source_catalog"] = source_catalog
        if "connector_options" in table:
            table_obj["connector_options"] = table["connector_options"]
        objects.append({"table": table_obj})
    return objects


def write_databricks_yml_official(
    workspace_url: str,
    catalog: str,
    schema: str,
    customer_name: str,
    connector_name: str,
    connector_config: Dict[str, Any],
    connection_name: str,
    source_type: str,
    out_path: str,
) -> None:
    """Fully declarative managed-ingestion pipeline - no notebook, no repo
    clone. Branches on connector_config['gateway']: CDC database connectors
    (Postgres, MySQL, SQL Server) need a gateway + ingestion pipeline + a
    refresh job; simple SaaS-app connectors (SharePoint, Google Drive) need
    only a single ingestion_definition pipeline."""
    tables = connector_config.get("tables", [])
    gateway = bool(connector_config.get("gateway", False))
    pipelines: Dict[str, Any] = {}
    jobs: Dict[str, Any] = {}

    # Real key inside source_configurations[].catalog is the source *system*
    # name, not always source_type.lower() - confirmed against the real
    # local bundle schema (pipelines.SourceCatalogConfig): POSTGRESQL uses
    # "postgres", not "postgresql".
    _SOURCE_CONFIG_KEY = {"POSTGRESQL": "postgres"}

    if gateway:
        gateway_key = f"{connector_name}_gateway"
        ingestion_key = f"{connector_name}_ingestion"
        source_catalog = connector_config.get("source_catalog") or (connector_config.get("connection_options", {}) or {}).get("database")

        pipelines[gateway_key] = {
            "name": f"{customer_name}_{connector_name}_gateway",
            "catalog": catalog,
            "schema": schema,
            "gateway_definition": {
                "connection_name": connection_name,
                "gateway_storage_catalog": catalog,
                "gateway_storage_schema": schema,
                "gateway_storage_name": f"{connector_name}_gateway",
            },
        }

        source_config = connector_config.get("source_configuration", {}) or {}
        slot_config = {
            "slot_name": source_config.get("slot_name", f"{customer_name}_{connector_name}_slot"),
            "publication_name": source_config.get("publication_name", f"{customer_name}_{connector_name}_publication"),
        }
        pipelines[ingestion_key] = {
            "name": f"{customer_name}_{connector_name}_ingestion",
            "catalog": catalog,
            "schema": schema,
            "ingestion_definition": {
                "ingestion_gateway_id": f"${{resources.pipelines.{gateway_key}.id}}",
                "source_type": source_type,
                "objects": _table_objects(catalog, schema, tables, source_catalog),
                "source_configurations": [
                    {"catalog": {
                        "source_catalog": source_catalog,
                        _SOURCE_CONFIG_KEY.get(source_type, source_type.lower()): {"slot_config": slot_config},
                    }}
                ],
            },
        }

        job_key = f"{connector_name}_refresh"
        jobs[job_key] = {
            "name": f"{customer_name}_{connector_name}_refresh",
            "trigger": {"periodic": {"interval": 1, "unit": "DAYS"}},
            "tasks": [
                {
                    "task_key": "refresh",
                    "pipeline_task": {"pipeline_id": f"${{resources.pipelines.{ingestion_key}.id}}"},
                }
            ],
        }
    else:
        ingestion_key = f"{connector_name}_ingestion"
        pipelines[ingestion_key] = {
            "name": f"{customer_name}_{connector_name}_ingestion",
            "catalog": catalog,
            "schema": schema,
            "channel": "PREVIEW",
            "serverless": True,
            "ingestion_definition": {
                "connection_name": connection_name,
                "objects": _table_objects(catalog, schema, tables, None),
            },
        }

    resources: Dict[str, Any] = {"pipelines": pipelines}
    if jobs:
        resources["jobs"] = jobs

    bundle_yaml: Dict[str, Any] = {
        "bundle": {"name": f"{customer_name}_{connector_name}_ingestion"},
        "workspace": {"host": workspace_url},
        "resources": resources,
    }
    _dump(bundle_yaml, out_path)


def write_github_actions_workflow(out_dir: str, deployment_yaml_path: str, env_var_names: List[str]) -> None:
    """Writes .github/workflows/deploy.yml into the extracted bundle - this is
    what actually makes merging the bundle's PR deploy anything. The PR only
    lands the code in the client's repo; nothing touches the real Databricks
    workspace until this workflow runs `databricks bundle deploy` for real.
    Fires only on push to the repo's default branch (i.e. after the PR is
    merged), never on the PR itself - a PR is proposed code, not a deploy
    trigger. The branch name is a placeholder (__DEFAULT_BRANCH__):
    github_publish.py substitutes the repo's real default branch after
    cloning it, since that isn't known at bundle-generation time."""
    workflow_dir = os.path.join(out_dir, ".github", "workflows")
    os.makedirs(workflow_dir, exist_ok=True)

    env_block = "\n".join(f"          {name}: ${{{{ secrets.{name} }}}}" for name in env_var_names)

    content = f"""name: Deploy ingestion pipelines

on:
  push:
    branches: ["__DEFAULT_BRANCH__"]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install Databricks CLI
        run: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

      - name: Install Python dependencies
        run: pip install -r requirements.txt

      - name: Deploy via Databricks Asset Bundles
        run: python scripts/deploy.py {deployment_yaml_path}
        env:
{env_block}
"""
    with open(os.path.join(workflow_dir, "deploy.yml"), "w") as f:
        f.write(content)


def _dump(bundle_yaml: Dict[str, Any], out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        yaml.dump(bundle_yaml, f, sort_keys=False)


def _run_env(extra_env: Optional[Dict[str, str]]) -> Dict[str, str]:
    env = os.environ.copy()
    env.update(extra_env or {})
    return env


def connection_exists(connection_name: str, extra_env: Optional[Dict[str, str]], logger) -> bool:
    """Official connectors only: checks existence via the real `databricks
    connections get` CLI command, never the SDK - kept consistent with how
    the pipeline itself is deployed (bundle validate/deploy), so an official
    connector's entire deploy path runs through official CLI commands only."""
    result = subprocess.run(
        ["databricks", "connections", "get", connection_name],
        env=_run_env(extra_env), capture_output=True, text=True,
    )
    return result.returncode == 0


def create_connection_cli(connection_name: str, connection_type: str, options: Dict[str, str],
                           extra_env: Optional[Dict[str, str]], logger) -> None:
    payload = json.dumps({"name": connection_name, "connection_type": connection_type, "options": options})
    logger.info(f"Running 'databricks connections create' for '{connection_name}'...")
    result = subprocess.run(
        ["databricks", "connections", "create", "--json", payload],
        env=_run_env(extra_env), capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"'databricks connections create' failed for '{connection_name}': {result.stderr}")
    logger.info(result.stdout)


def update_connection_cli(connection_name: str, options: Dict[str, str],
                           extra_env: Optional[Dict[str, str]], logger) -> None:
    payload = json.dumps({"options": options})
    logger.info(f"Running 'databricks connections update' for '{connection_name}'...")
    result = subprocess.run(
        ["databricks", "connections", "update", connection_name, "--json", payload],
        env=_run_env(extra_env), capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"'databricks connections update' failed for '{connection_name}': {result.stderr}")
    logger.info(result.stdout)


def delete_connection_cli(connection_name: str, extra_env: Optional[Dict[str, str]], logger) -> None:
    logger.info(f"Running 'databricks connections delete' for '{connection_name}'...")
    result = subprocess.run(
        ["databricks", "connections", "delete", connection_name],
        env=_run_env(extra_env), capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"'databricks connections delete' failed for '{connection_name}': {result.stderr}")


def run_bundle_deploy(bundle_dir: str, extra_env: Optional[Dict[str, str]], logger, dry_run: bool = False) -> None:
    """Runs `databricks bundle validate` then `bundle deploy` against
    bundle_dir. extra_env carries the same per-customer host/token
    _make_workspace_client already resolved (see deploy.py) - empty/None
    falls through to ambient env vars / the .databrickscfg DEFAULT profile,
    matching WorkspaceClient()'s own fallback exactly."""
    env = os.environ.copy()
    env.update(extra_env or {})

    logger.info(f"Running 'databricks bundle validate' in {bundle_dir}...")
    result = subprocess.run(
        ["databricks", "bundle", "validate"],
        cwd=bundle_dir, env=env, capture_output=True, text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"'databricks bundle validate' failed in {bundle_dir}")

    if dry_run:
        logger.info("[DRY-RUN] Skipping 'databricks bundle deploy'.")
        return

    logger.info(f"Running 'databricks bundle deploy' in {bundle_dir}...")
    result = subprocess.run(
        ["databricks", "bundle", "deploy"],
        cwd=bundle_dir, env=env, capture_output=True, text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"'databricks bundle deploy' failed in {bundle_dir}")
