# PostgreSQL Official Connector

Ingests PostgreSQL tables via Databricks' first-party **Lakeflow Connect** managed connector
(CDC via logical replication) — not a cloned community repo, and not a notebook. This is the
first "official connector" in this framework; see `connectors/gmail/README.md` for how the
auth model differs from the community connectors (HubSpot/YouTube/Gmail).

**Two things to confirm before relying on this in production, not glossed over:**
1. The PostgreSQL Lakeflow connector is **Public Preview** — Databricks' own docs say to
   contact your account team to enroll your workspace. This framework cannot script around
   that; it's an account-level gate.
2. The bundle field this connector relies on (`gateway_definition`) is marked
   `"x-databricks-preview": "PRIVATE", "doNotSuggest": true` in the real, local
   `databricks bundle schema` output, even though it's the field the official published docs
   tell you to use. It works, but the CLI won't autocomplete it and treats it as unstable —
   always run `databricks bundle validate` before trusting a deploy.

## Authentication model — fully automated, no UI, ever

Unlike Gmail (OAuth, one-time browser consent) or HubSpot/YouTube (static secret injected at
notebook run time), PostgreSQL uses a plain username/password, and `scripts/deploy.py` creates
the Unity Catalog connection itself, non-interactively, on every deploy:

```bash
databricks connections create --json '{
  "name": "<connection_name>",
  "connection_type": "POSTGRESQL",
  "options": {
    "host": "<db_host>", "port": "<db_port>", "database": "<db_name>",
    "user": "<replication_user>", "password": "<replication_password>"
  }
}'
```

`host`/`port`/`database` come straight from `deployment.yaml` (`connection_options` — not
secret). `user`/`password` come from **this customer's `.env`**, never a Databricks secret
scope: `deploy.py` runs outside a notebook/job context and has no way to dereference a secret
scope value (`dbutils.secrets.get()` only works inside a running notebook/job — see
`_resolve_env_credentials` in `scripts/deploy.py`). Add to `.env`:

```
POSTGRESQL_USER_<CUSTOMER>=databricks_replication
POSTGRESQL_PASSWORD_<CUSTOMER>=<the real password>
```

where `<CUSTOMER>` matches the same uppercased, non-alphanumeric-stripped suffix already used
for `DATABRICKS_HOST_<CUSTOMER>`/`DATABRICKS_TOKEN_<CUSTOMER>`.

## Source-side setup (one-time, on the actual Postgres instance — not Databricks)

Run once against the source database, as an admin:

```sql
CREATE USER databricks_replication WITH PASSWORD '...';
GRANT CONNECT ON DATABASE <db> TO databricks_replication;
GRANT USAGE ON SCHEMA <schema> TO databricks_replication;
GRANT SELECT ON TABLE <schema>.<table> TO databricks_replication;
ALTER USER databricks_replication WITH REPLICATION;
-- AWS RDS/Aurora also needs:
GRANT rds_replication TO databricks_replication;

-- wal_level must be 'logical' (on RDS/Aurora: set the rds.logical_replication
-- parameter to 1 in the parameter group, then reboot).
SHOW wal_level;

-- Per table:
ALTER TABLE <schema>.<table> REPLICA IDENTITY DEFAULT; -- or FULL if no primary key / large columns

CREATE PUBLICATION databricks_publication FOR TABLE <schema>.<table1>, <schema>.<table2>;

SET ROLE databricks_replication;
SELECT pg_create_logical_replication_slot('databricks_slot', 'pgoutput');
RESET ROLE;
```

**If your provider fronts the database with a connection pooler** (e.g. a hostname containing
`-pooler`, PgBouncer in transaction-pooling mode): logical replication needs a direct,
non-pooled connection — transaction pooling doesn't support the replication protocol. Use the
provider's direct/unpooled host for `connection_options.host`, not the pooled one, and confirm
logical replication is enabled for the project/instance (some managed providers, e.g. Neon,
have it off by default as a per-project setting).

## Pipeline shape — gateway + ingestion + a scheduled job

This connector needs *two* pipeline resources plus a job (confirmed against both the docs and
the real local bundle schema) — unlike the single-pipeline shape simpler official connectors
(no CDC/replication) would use:

- A **gateway** pipeline captures the CDC stream via the slot/publication above.
- An **ingestion** pipeline references the gateway by bundle-native id
  (`ingestion_gateway_id: ${resources.pipelines.<gateway_key>.id}`) and lists the
  tables/schemas to replicate.
- A **job** triggers the ingestion pipeline on a schedule (`trigger.periodic`) — CDC refresh
  here is scheduled, not continuous like the notebook-based community pipelines.

`scripts/deploy.py` generates all three into `deployments/<customer>/bundles/postgresql/databricks.yml`
and runs `databricks bundle validate` + `databricks bundle deploy` for you — see
`scripts/bundle_gen.py`'s `write_databricks_yml_official`.

## Deployment Configuration

```yaml
connectors:
  - name: "postgresql"
    connection_name: "customer_a_postgresql"
    connection_options:
      host: "<direct, non-pooled host>"
      port: "5432"
      database: "<db_name>"
    source_configuration:
      slot_name: "databricks_slot"
      publication_name: "databricks_publication"
    tables:
      - source_schema: "public"
        source_table: "orders"
        destination_schema: "raw_ingestion"
```
