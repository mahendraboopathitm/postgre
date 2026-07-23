# boopathi - Ingestion Bundle

Extracted from the master ingestion framework repo. Contains only the connector(s) selected at extraction time, plus the scripts needed to validate and deploy them - nothing else from the master repo ships here.

## Connectors

### postgresql
Ingests PostgreSQL tables via change data capture (logical replication) into Unity Catalog, using Databricks' first-party Lakeflow Connect PostgreSQL connector - not a cloned community repo.

Official Lakeflow Connect connector - the Unity Catalog connection is created/updated automatically by `deploy.py` every deploy, no manual UI step. Real credential values live in `.env` (gitignored, never a Databricks secret scope - this connector has no notebook/job for deploy.py to dereference one from):
- `user` -> `.env` var `POSTGRESQL_USER_BOOPATHI`
- `password` -> `.env` var `POSTGRESQL_PASSWORD_BOOPATHI`

See `connectors/postgresql/README.md` for source-side setup.

## Quick Start

1. `pip install -r requirements.txt`
2. For each connector above: upload static-secret values under the secret scope, or fill in `.env` for official connectors - see that connector's section above.
3. `.env` already has this client's workspace host/token filled in (gitignored - never commit it). `.env.example` is the template if you need to recreate it.
4. `deployments/<client>/bundles/<connector>/databricks.yml` is already generated and committed for each connector - this is the real Databricks Asset Bundle definition (pipelines/jobs) that will be deployed, included here so it's reviewable in this PR. `deploy.py` regenerates it fresh from `deployment.yaml` immediately before every deploy, so it can never drift from what's committed.
5. Validate, dry-run, then deploy:
   ```bash
   python scripts/validate.py deployments/boopathi/deployment.yaml
   python scripts/deploy.py deployments/boopathi/deployment.yaml --dry-run
   python scripts/deploy.py deployments/boopathi/deployment.yaml
   ```

## CI/CD (GitHub Actions)

`.github/workflows/deploy.yml` is already included. It runs `python scripts/deploy.py deployments/boopathi/deployment.yaml` automatically on every push to this repo's default branch - in practice, **merging the PR this bundle arrived in is what actually deploys the pipeline(s) to Databricks.** Nothing touches the real workspace before that merge.

Before merging, add these as repo secrets (Settings -> Secrets and variables -> Actions -> New repository secret) with these exact names and the same real values already in `.env`:

- `DATABRICKS_HOST_BOOPATHI`
- `DATABRICKS_TOKEN_BOOPATHI`
- `POSTGRESQL_USER_BOOPATHI`
- `POSTGRESQL_PASSWORD_BOOPATHI`

Optional: add branch protection / a GitHub Actions "environment" with required reviewers on `deploy.yml`'s job if you want a manual approval step between merge and deploy, instead of it running immediately.