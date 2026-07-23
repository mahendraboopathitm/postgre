import os
import re
import sys
import argparse
import base64
from typing import Dict, Any, List, Optional
from types import SimpleNamespace
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog, workspace

# Add current directory to path for relative imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils import load_yaml, setup_logger
from validate import validate_config
import bundle_gen

# Loads .env from the current working directory if present; a no-op otherwise
# (falls through to ambient env vars / .databrickscfg, same as before).
load_dotenv()

# Setup logger
logger = setup_logger("deployer")

class ConnectorDeployer:
    def __init__(self, deployment_config_path: str, dry_run: bool = False):
        """Load and validate config, and initialize the Databricks WorkspaceClient."""
        self.config_path = deployment_config_path
        self.dry_run = dry_run
        self.config = self._load_and_validate_config()

        customer_sec = self.config["customer"]
        self.customer_name = customer_sec["name"]
        self.catalog = customer_sec["catalog"]
        self.schema = customer_sec["schema"]
        self.secret_scope = customer_sec.get("secret_scope", f"{self.customer_name}_secrets")
        self.customer_suffix = re.sub(r"[^A-Za-z0-9]", "_", self.customer_name).upper()
        self.workspace_url = customer_sec["workspace_url"]

        logger.info("Initializing Databricks Workspace Client...")
        try:
            self.client = self._make_workspace_client()
            logger.info("Databricks Workspace Client initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Databricks Workspace Client: {e}")
            raise e

        # Define community connector repository details
        self.repo_url = "https://github.com/databrickslabs/lakeflow-community-connectors.git"
        self.repo_root = f"/Repos/{self.customer_name}/lakeflow-community-connectors"

        if self.dry_run:
            logger.info("--- RUNNING IN DRY-RUN MODE ---")
            self._verify_workspace_resources()

    def _make_workspace_client(self) -> WorkspaceClient:
        """Builds a WorkspaceClient for this deployment's customer.

        Prefers per-customer credentials from .env
        (DATABRICKS_HOST_<CUSTOMER>/DATABRICKS_TOKEN_<CUSTOMER>, where <CUSTOMER>
        is customer.name uppercased with non-alphanumeric characters replaced by
        underscores) so one .env can safely hold every customer's workspace
        without risking a deploy landing in the wrong one. Falls back to ambient
        DATABRICKS_HOST/DATABRICKS_TOKEN or the .databrickscfg DEFAULT profile
        when no customer-specific pair is set, matching the previous behavior.
        """
        host_var = f"DATABRICKS_HOST_{self.customer_suffix}"
        token_var = f"DATABRICKS_TOKEN_{self.customer_suffix}"
        host: Optional[str] = os.environ.get(host_var)
        token: Optional[str] = os.environ.get(token_var)

        if host and token:
            logger.info(f"Using per-customer credentials from {host_var} / {token_var}.")
            # Reused by bundle_gen.run_bundle_deploy so `databricks bundle
            # deploy` targets the same workspace, without a separate CLI profile.
            self.bundle_env = {"DATABRICKS_HOST": host, "DATABRICKS_TOKEN": token}
            return WorkspaceClient(host=host, token=token)

        logger.info(
            f"{host_var} / {token_var} not set - falling back to ambient "
            f"DATABRICKS_HOST/DATABRICKS_TOKEN or the .databrickscfg DEFAULT profile."
        )
        self.bundle_env = {}
        return WorkspaceClient()

    def _verify_workspace_resources(self):
        """Validates catalog, schema, and secret scope existence in dry-run mode."""
        logger.info("[DRY-RUN] Checking if secret scope exists...")
        try:
            self.client.secrets.list_secrets(scope=self.secret_scope)
            logger.info(f"[DRY-RUN] Secret scope '{self.secret_scope}' verified.")
        except Exception as e:
            logger.warning(f"[DRY-RUN] Warning verifying secret scope '{self.secret_scope}': {e}. (Ensure it is created prior to running deployment.)")

        logger.info("[DRY-RUN] Checking if catalog exists...")
        try:
            self.client.catalogs.get(name=self.catalog)
            logger.info(f"[DRY-RUN] Unity Catalog '{self.catalog}' verified.")
        except Exception as e:
            logger.error(f"[DRY-RUN] Error: Catalog '{self.catalog}' does not exist or is not accessible: {e}")
            raise ValueError(f"Catalog '{self.catalog}' not found in workspace.")

        logger.info("[DRY-RUN] Checking if schema exists...")
        try:
            self.client.schemas.get(full_name=f"{self.catalog}.{self.schema}")
            logger.info(f"[DRY-RUN] Schema '{self.catalog}.{self.schema}' verified.")
        except Exception as e:
            logger.error(f"[DRY-RUN] Error: Schema '{self.catalog}.{self.schema}' does not exist or is not accessible: {e}")
            raise ValueError(f"Schema '{self.catalog}.{self.schema}' not found in workspace.")

    def _load_and_validate_config(self) -> Dict[str, Any]:
        """Loads the configuration file and runs structural validation."""
        logger.info(f"Loading configuration from: {self.config_path}")
        try:
            config = load_yaml(self.config_path)
        except Exception as e:
            logger.error(f"Failed to load YAML config: {e}")
            raise e
            
        logger.info("Running configuration validation checks...")
        is_valid, errors = validate_config(config)
        if not is_valid:
            error_details = "\n".join([f" - {err}" for err in errors])
            logger.error(f"Configuration is invalid:\n{error_details}")
            raise ValueError(f"Configuration validation failed:\n{error_details}")
            
        logger.info("Configuration is structurally valid.")
        return config

    def deploy_all(self):
        """Deploy all connectors specified in the configuration."""
        logger.info(f"Starting deployment for customer: '{self.customer_name}'")
        connectors = self.config.get("connectors", [])
        
        for conn in connectors:
            connector_name = conn["name"]
            logger.info(f"Deploying connector: '{connector_name}'...")
            try:
                self.deploy_connector(connector_name)
            except Exception as e:
                logger.error(f"Failed to deploy connector '{connector_name}': {e}")
                raise e
                
        logger.info(f"Successfully finished deployment for customer: '{self.customer_name}'")

    def deploy_connector(self, connector_name: str):
        """Deploy a single connector by name."""
        logger.info(f"Retrieving configuration for connector: '{connector_name}'")
        connector_config = None
        for conn in self.config.get("connectors", []):
            if conn["name"] == connector_name:
                connector_config = conn
                break
                
        if not connector_config:
            raise ValueError(f"Connector '{connector_name}' not found in configuration file.")

        kind = connector_config.get("kind", "community")
        # One bundle dir per connector (not per customer) - deploy_all() loops
        # connector-by-connector, and each needs its own independent
        # `databricks bundle deploy` so redeploying one connector never
        # clobbers another's already-deployed pipeline definition.
        bundle_dir = os.path.join(os.path.dirname(os.path.abspath(self.config_path)), "bundles", connector_name)
        databricks_yml_path = os.path.join(bundle_dir, "databricks.yml")

        if kind == "official":
            logger.info(f"[1/3] Creating/verifying Unity Catalog connection for '{connector_name}'")
            self._create_connection(connector_name, connector_config)

            logger.info(f"[2/3] Generating databricks.yml for managed ingestion pipeline...")
            bundle_gen.write_databricks_yml_official(
                workspace_url=self.workspace_url,
                catalog=self.catalog,
                schema=self.schema,
                customer_name=self.customer_name,
                connector_name=connector_name,
                connector_config=connector_config,
                connection_name=connector_config["connection_name"],
                source_type=connector_config.get("source_type", connector_config.get("connection_type", "")),
                out_path=databricks_yml_path,
            )

            logger.info(f"[3/3] Deploying pipeline via Databricks Asset Bundle...")
            bundle_gen.run_bundle_deploy(bundle_dir, self.bundle_env, logger, dry_run=self.dry_run)
            logger.info(f"Connector '{connector_name}' successfully deployed.")
            return

        # community connector (HubSpot/YouTube/Gmail): clone + connection +
        # notebook stay imperative SDK steps; only the pipeline moves to a
        # generated databricks.yml + `databricks bundle deploy`.
        logger.info(f"[1/4] Ensuring community repository is cloned to {self.repo_root}")
        self._clone_repo(self.repo_root)

        logger.info(f"[2/4] Creating Unity Catalog connection for '{connector_name}'")
        self._create_connection(connector_name, connector_config)

        logger.info(f"[3/4] Generating and uploading ingest notebook...")
        notebook_path = self._create_ingest_notebook(self.repo_root, connector_name, connector_config["tables"])

        logger.info(f"[4/4] Deploying Delta Live Tables (SDP) pipeline via Databricks Asset Bundle...")
        bundle_gen.write_databricks_yml_community(
            workspace_url=self.workspace_url,
            catalog=self.catalog,
            schema=self.schema,
            secret_scope=self.secret_scope,
            customer_name=self.customer_name,
            connector_name=connector_name,
            connector_config=connector_config,
            notebook_path=notebook_path,
            out_path=databricks_yml_path,
        )
        bundle_gen.run_bundle_deploy(bundle_dir, self.bundle_env, logger, dry_run=self.dry_run)

        logger.info(f"Connector '{connector_name}' successfully deployed.")

    def _clone_repo(self, root_path: str):
        """Clones or updates the community connector repository in Databricks Workspace."""
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would clone/update repo '{self.repo_url}' at path '{root_path}'")
            return
        existing_repo_id = None
        try:
            logger.info("Checking for existing repository clones in workspace...")
            for repo in self.client.repos.list():
                if repo.path == root_path:
                    existing_repo_id = repo.id
                    break
        except Exception as e:
            logger.warning(f"Failed to fetch existing repositories: {e}. Attempting direct creation...")
            
        if existing_repo_id:
            logger.info(f"Repository already cloned at '{root_path}' (ID: {existing_repo_id}). Updating to latest main branch...")
            try:
                self.client.repos.update(repo_id=existing_repo_id, branch="main")
                logger.info("Repository updated successfully.")
            except Exception as e:
                logger.warning(f"Repository update failed: {e}. Ingestion will proceed with current repository state.")
        else:
            # Ensure parent directory exists in workspace
            parent_path = "/".join(root_path.rstrip("/").split("/")[:-1])
            logger.info(f"Ensuring parent directory '{parent_path}' exists in workspace...")
            try:
                self.client.workspace.mkdirs(path=parent_path)
            except Exception as e:
                logger.warning(f"Failed to ensure parent directory exists: {e}")

            logger.info(f"Cloning '{self.repo_url}' to workspace path '{root_path}'...")
            try:
                self.client.repos.create(
                    url=self.repo_url,
                    provider="gitHub",
                    path=root_path
                )
                logger.info("Repository cloned successfully.")
            except Exception as e:
                # Handle race conditions where repo might have been created simultaneously
                if "ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
                    logger.info("Repository already exists (handled gracefully).")
                else:
                    logger.error(f"Failed to clone repository: {e}")
                    raise e

    def _resolve_env_credentials(self, connector_name: str, fields: List[str]) -> Dict[str, str]:
        """Resolves plain-credential fields (e.g. a Postgres user/password)
        straight from this customer's .env, never a Databricks secret scope.

        Unlike the static-secret community connectors (HubSpot/YouTube), whose
        real credential values get injected later by the generated notebook
        running as a pipeline (dbutils.secrets.get only works inside a running
        notebook/job), an official connector's connection is created directly
        by this script with real values - and this script has no way to
        dereference a Databricks secret scope itself. So these come from
        <CONNECTOR>_<FIELD>_<CUSTOMER> in .env, the same per-customer
        convention _make_workspace_client already uses for the workspace
        host/token.
        """
        resolved = {}
        for field in fields:
            env_var = f"{connector_name.upper()}_{field.upper()}_{self.customer_suffix}"
            value = os.environ.get(env_var)
            if not value:
                raise ValueError(
                    f"Missing required env var '{env_var}' for connector '{connector_name}' - "
                    f"add it to .env. It cannot be a Databricks secret scope entry: this script "
                    f"runs outside a notebook/job context and can't dereference those."
                )
            resolved[field] = value
        return resolved

    def _build_connection_options(self, connector_name: str, config: Dict[str, Any]) -> tuple:
        """Returns (connection_type, options) for a connection this script is
        allowed to create/update with real values."""
        if config.get("kind") == "official":
            connection_type = config.get("connection_type", "").upper()
            options = dict(config.get("connection_options", {}) or {})
            options.update(self._resolve_env_credentials(connector_name, config.get("env_credentials", []) or []))
            return connection_type, options
        # community, static-secret (HubSpot/YouTube): bare options - real
        # values get injected later by the generated notebook at pipeline run time.
        return "COMMUNITY", {"sourceName": connector_name}

    def _create_connection(self, connector_name: str, config: Dict[str, Any]):
        """Creates, updates, or verifies a Unity Catalog Connection, depending
        on the connector's auth model:

        - community connectors with a non-empty 'credentials' map (HubSpot,
          YouTube) use a framework-managed static secret; safe to create/update
          here with bare options every deploy.
        - community connectors with no 'credentials' (Gmail) and official
          connectors with auth_mode 'interactive' authenticate via OAuth
          granted once, interactively, outside this script (see
          connectors/<name>/README.md). We only verify the connection exists
          and never create, update, or recreate it - doing so could silently
          destroy a human-granted OAuth authorization CI has no way to redo.
        - official connectors with auth_mode 'automated' (Postgres) use plain
          credentials resolved from .env (see _resolve_env_credentials) and
          get created/updated with real values every deploy, same as
          static-secret community connectors.
        """
        connection_name = config["connection_name"]
        kind = config.get("kind", "community")
        auth_mode = config.get("auth_mode")
        externally_managed = (auth_mode != "automated") if kind == "official" else not config.get("credentials")

        if externally_managed:
            logger.info(
                f"'{connector_name}' has no automated connection setup - connection "
                f"'{connection_name}' must already exist and be authorized; this script "
                f"will not create, update, or recreate it."
            )
            if kind == "official":
                # Official connectors only ever touch connections via the real
                # `databricks connections ...` CLI, never the SDK - see
                # _create_connection_via_cli below.
                if bundle_gen.connection_exists(connection_name, self.bundle_env, logger):
                    logger.info(f"Confirmed connection '{connection_name}' exists. Leaving it untouched.")
                    return
                logger.error(
                    f"Connection '{connection_name}' not found via 'databricks connections get'. "
                    f"'{connector_name}' authenticates via a pre-authorized UC connection that this "
                    f"script never creates automatically. Complete the one-time authorization step "
                    f"in connectors/{connector_name}/README.md, then re-run."
                )
                raise ValueError(f"Connection '{connection_name}' not found or not authorized for '{connector_name}'.")
            try:
                self.client.connections.get(name=connection_name)
                logger.info(f"Confirmed connection '{connection_name}' exists. Leaving it untouched.")
            except Exception as e:
                logger.error(
                    f"Connection '{connection_name}' does not exist (or is not accessible): {e}. "
                    f"'{connector_name}' authenticates via a pre-authorized UC connection that this "
                    f"script never creates automatically. Complete the one-time authorization step "
                    f"in connectors/{connector_name}/README.md, then re-run."
                )
                raise ValueError(f"Connection '{connection_name}' not found or not authorized for '{connector_name}'.") from e
            return

        connection_type_value, options = self._build_connection_options(connector_name, config)

        if self.dry_run:
            safe_options = {k: ("***" if k in ("password", "client_secret") else v) for k, v in options.items()}
            logger.info(f"[DRY-RUN] Would create/update {connection_type_value} UC connection '{connection_name}' with options: {safe_options}")
            return

        if kind == "official":
            self._create_connection_via_cli(connection_name, connection_type_value, options)
            return

        conn_exists = False
        try:
            self.client.connections.get(name=connection_name)
            conn_exists = True
        except Exception:
            pass

        conn_type = SimpleNamespace(value=connection_type_value)

        if conn_exists:
            logger.info(f"Connection '{connection_name}' already exists. Updating options...")
            try:
                self.client.connections.update(
                    name=connection_name,
                    options=options
                )
                logger.info("Connection updated successfully.")
            except Exception as e:
                logger.warning(f"Failed to update connection '{connection_name}': {e}. Recreating...")
                try:
                    self.client.connections.delete(name=connection_name)
                    self.client.connections.create(
                        name=connection_name,
                        connection_type=conn_type,
                        options=options
                    )
                    logger.info("Connection recreated successfully.")
                except Exception as ex:
                    logger.error(f"Failed to recreate connection '{connection_name}': {ex}")
                    raise ex
        else:
            logger.info(f"Creating new connection '{connection_name}' (type: {connection_type_value})...")
            try:
                self.client.connections.create(
                    name=connection_name,
                    connection_type=conn_type,
                    options=options
                )
                logger.info("Connection created successfully.")
            except Exception as e:
                if "ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
                    logger.info(f"Connection '{connection_name}' already exists (handled gracefully).")
                else:
                    logger.error(f"Failed to create connection: {e}")
                    raise e

    def _create_connection_via_cli(self, connection_name: str, connection_type_value: str, options: Dict[str, str]) -> None:
        """Official connectors only: create/update the UC connection via the
        real `databricks connections ...` CLI commands (see bundle_gen.py),
        never the SDK - so an official connector's whole deploy path (this
        step, plus bundle validate/deploy) runs through official Databricks
        CLI commands only, matching how the pipeline itself is deployed."""
        if bundle_gen.connection_exists(connection_name, self.bundle_env, logger):
            logger.info(f"Connection '{connection_name}' already exists. Updating options...")
            try:
                bundle_gen.update_connection_cli(connection_name, options, self.bundle_env, logger)
                logger.info("Connection updated successfully.")
            except Exception as e:
                logger.warning(f"Failed to update connection '{connection_name}': {e}. Recreating...")
                try:
                    bundle_gen.delete_connection_cli(connection_name, self.bundle_env, logger)
                    bundle_gen.create_connection_cli(connection_name, connection_type_value, options, self.bundle_env, logger)
                    logger.info("Connection recreated successfully.")
                except Exception as ex:
                    logger.error(f"Failed to recreate connection '{connection_name}': {ex}")
                    raise ex
        else:
            logger.info(f"Creating new connection '{connection_name}' (type: {connection_type_value})...")
            try:
                bundle_gen.create_connection_cli(connection_name, connection_type_value, options, self.bundle_env, logger)
                logger.info("Connection created successfully.")
            except Exception as e:
                if "ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
                    logger.info(f"Connection '{connection_name}' already exists (handled gracefully).")
                else:
                    logger.error(f"Failed to create connection: {e}")
                    raise e

    def _create_ingest_notebook(self, root_path: str, connector: str, tables: List[Dict[str, Any]]) -> str:
        """Generates and uploads a complete Databricks notebook for Lakeflow Community ingestion."""
        # Find connection configuration to inject credentials
        connector_config = None
        for conn in self.config.get("connectors", []):
            if conn["name"] == connector:
                connector_config = conn
                break
                
        connection_name = connector_config.get("connection_name") if connector_config else ""
        # Generate notebook lines
        notebook_lines = [
            "# Databricks notebook source",
            "# DBTITLE 1,Inject Connection Credentials and Update Connection Options",
            "from databricks.sdk import WorkspaceClient",
            "w = WorkspaceClient()",
            f"connection_name = '{connection_name}'"
        ]
        
        credentials = connector_config.get("credentials", {}) if connector_config else {}
        if credentials:
            notebook_lines.append(f"conn_options = {{'sourceName': '{connector}'}}")
            for cred_key, secret_key in credentials.items():
                secret_lookup = f"dbutils.secrets.get(scope='{self.secret_scope}', key='{secret_key}')"
                notebook_lines.extend([
                    f"conn_options['{cred_key}'] = {secret_lookup}",
                    f"spark.conf.set('{cred_key}', {secret_lookup})",
                    f"spark.conf.set('spark.datasource.lakeflow_connect.{cred_key}', {secret_lookup})"
                ])
                if connection_name:
                    notebook_lines.append(
                        f"spark.conf.set('spark.datasource.connection.{connection_name}.{cred_key}', {secret_lookup})"
                    )
            notebook_lines.extend([
                "if connection_name:",
                "    w.connections.update(name=connection_name, options=conn_options)"
            ])
        else:
            notebook_lines.append("# No credentials configured to inject.")
            
        notebook_lines.extend([
            "",
            "# COMMAND ----------",
            "",
            "# DBTITLE 2,Register & Ingest via Lakeflow Community Connector",
            "from databricks.labs.community_connector.pipeline import ingest",
            "from databricks.labs.community_connector import register",
            "",
            "spark.conf.set('spark.databricks.unityCatalog.connectionDfOptionInjection.enabled', 'true')",
            "",
            f"source_name = '{connector}'",
            "register(spark, source_name)",
            "",
            "pipeline_spec = {",
            f"    'connection_name': '{connector_config.get('connection_name')}'," if connector_config else "    'connection_name': '',",
            "    'objects': ["
        ])
        
        for table in tables:
            src_table = table["source_table"]
            dest_table = table.get("destination_table", src_table)
            
            table_entry = {
                "source_table": src_table,
                "destination_table": dest_table
            }
            
            if "table_configuration" in table:
                table_entry["table_configuration"] = table["table_configuration"]
                
            notebook_lines.append(
                f"        {{'table': {table_entry}}},"
            )
            
        notebook_lines.extend([
            "    ]",
            "}",
            "",
            "ingest(spark, pipeline_spec)"
        ])
        
        notebook_content = "\n".join(notebook_lines)
        
        # Save path inside the repo clone's src directory for correct path imports
        notebook_path = f"{root_path}/src/{connector}_ingest"
        
        # Encode content to base64 for upload API
        encoded_content = base64.b64encode(notebook_content.encode('utf-8')).decode('utf-8')
        
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would upload notebook to workspace path: '{notebook_path}' with contents:\n{notebook_content}")
            return notebook_path
            
        logger.info(f"Uploading notebook to workspace: '{notebook_path}'")
        try:
            self.client.workspace.import_(
                path=notebook_path,
                content=encoded_content,
                format=workspace.ImportFormat.SOURCE,
                language=workspace.Language.PYTHON,
                overwrite=True
            )
            logger.info("Notebook uploaded successfully.")
        except Exception as e:
            logger.error(f"Failed to upload notebook: {e}")
            raise e
            
        return notebook_path

def main():
    parser = argparse.ArgumentParser(description="Deploy community connectors to Databricks.")
    parser.add_argument("config_path", help="Path to the deployment config YAML file.")
    parser.add_argument("--dry-run", action="store_true", help="Perform verification and validate catalog/schema/secrets without creating or modifying resources.")
    
    args = parser.parse_args()
    
    try:
        deployer = ConnectorDeployer(args.config_path, dry_run=args.dry_run)
        deployer.deploy_all()
    except Exception as e:
        logger.error(f"Deployment process failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
