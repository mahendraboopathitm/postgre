# mahi - Ingestion Bundle

Extracted from the master ingestion framework repo. Every connector selected here is an official Lakeflow Connect connector, so this bundle ships only the two real artifacts needed to deploy it - no Python scripts, no `deployment.yaml`. If a community connector is ever added for this client later, re-extract - that pulls in the full `scripts/`/`deployment.yaml` flow instead, since community connectors need the SDK for their connection + notebook clone.

## Files

- `databricks.yml` - the real Databricks Asset Bundle definition (pipelines/jobs), fully resolved, reviewable as-is in this PR. No secrets in it - it only ever references a connection by name.
- `connection_postgresql.json` - the `databricks connections create/update --json` payload for `mahi_postgresql`. Credential fields are placeholders (e.g. `__PASSWORD__`) - the workflow below fills them in from GitHub secrets immediately before the CLI call. The real values are never written to this file, so nothing secret ever gets committed.

## Connectors

### postgresql
Ingests PostgreSQL tables via change data capture (logical replication) into Unity Catalog, using Databricks' first-party Lakeflow Connect PostgreSQL connector - not a cloned community repo.

See `connectors/postgresql/README.md` for source-side setup.

## Before merging

Add these as repo secrets (Settings -> Secrets and variables -> Actions -> New repository secret), using the same real values from your local `.env`:

- `DATABRICKS_HOST_MAHI`
- `DATABRICKS_TOKEN_MAHI`
- `POSTGRESQL_USER_MAHI`
- `POSTGRESQL_PASSWORD_MAHI`

## CI/CD (GitHub Actions)

`.github/workflows/deploy.yml` is already included and fires automatically on every push to this repo's default branch - in practice, **merging the PR this bundle arrived in is what actually deploys to Databricks.** Nothing touches the real workspace before that merge. For each connector, the workflow:
1. Fills in `connection_<name>.json`'s placeholder field(s) from the GitHub secrets above via `jq` (handles quoting/escaping correctly - never a raw string substitution).
2. Runs `databricks connections get` to check whether the connection already exists, then the matching bare `databricks connections create` or `databricks connections update` - official CLI commands only, nothing wraps them.

Then, once for the whole bundle: bare `databricks bundle validate` then `databricks bundle deploy` - the actual pipeline/job deploy.

## Testing locally (optional)

With the Databricks CLI and `jq` installed, and the env vars above exported (e.g. `set -a; source .env; set +a` after filling in real values):
```bash
databricks bundle validate
```
validates `databricks.yml` without creating anything. To create/update a connection by hand, run the same `jq` + `databricks connections create/update` commands `.github/workflows/deploy.yml` runs.

Optional: add branch protection / a GitHub Actions "environment" with required reviewers on `deploy.yml`'s job if you want a manual approval step between merge and deploy, instead of it running immediately.