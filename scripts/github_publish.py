import base64
import os
import shutil
import subprocess
import tempfile
import time
from typing import Optional

import jwt as pyjwt
import requests

GITHUB_API = "https://api.github.com"


def _generate_app_jwt(app_id: str, private_key_pem: str) -> str:
    """Short-lived (9 min) JWT identifying the GitHub App itself - used only
    to look up the installation and mint an installation access token. Never
    used directly against repo endpoints."""
    # +/- 5 minutes, not the naive (-30s, +9min) GitHub's docs example uses:
    # GitHub checks both claims against ITS OWN clock, not the caller's. A
    # local clock running even ~100s fast (seen for real during development)
    # pushes a 9-minute exp past GitHub's 10-minute max, rejected as "exp too
    # far in the future" - and a too-small backdate leaves iat in the
    # future, rejected as generic "Bad credentials". +/-5 min tolerates
    # several minutes of drift in either direction while staying inside
    # GitHub's window.
    now = int(time.time())
    payload = {"iat": now - 5 * 60, "exp": now + 5 * 60, "iss": app_id}
    return pyjwt.encode(payload, private_key_pem, algorithm="RS256")


def _get_installation_access_token(app_id: str, private_key_pem: str, owner: str, repo: str) -> str:
    """Exchanges the App JWT for a short-lived (~1h) installation access
    token scoped to whatever repos the client installed the App on - never
    the App's own broad credentials."""
    app_jwt = _generate_app_jwt(app_id, private_key_pem)
    headers = {"Authorization": f"Bearer {app_jwt}", "Accept": "application/vnd.github+json"}

    # Check the JWT/App ID/private key are actually valid *before* asking
    # about a specific repo's installation - otherwise a bad credential and
    # a real "not installed here" both come back as opaque errors on the
    # repo-installation lookup below, which is confusing to tell apart.
    self_check = requests.get(f"{GITHUB_API}/app", headers=headers, timeout=30)
    if self_check.status_code != 200:
        raise RuntimeError(
            f"GitHub rejected the App credentials themselves ({self_check.status_code}): {self_check.text}\n"
            f"Check: is '{app_id}' the App ID (numeric, on the App's settings page) and not the "
            f"Client ID (starts with 'Iv1.')? Is the .pem the private key for that exact App? "
            f"Is your system clock roughly correct (GitHub rejects JWTs with skewed iat/exp)?"
        )

    resp = requests.get(f"{GITHUB_API}/repos/{owner}/{repo}/installation", headers=headers, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Could not find a GitHub App installation for '{owner}/{repo}' ({resp.status_code}): {resp.text}\n"
            f"The App credentials are valid ({self_check.json().get('name')}), but it isn't installed on "
            f"this repo yet - Settings on the App -> Install App -> select '{repo}'."
        )
    installation_id = resp.json()["id"]

    resp = requests.post(
        f"{GITHUB_API}/app/installations/{installation_id}/access_tokens",
        headers=headers, timeout=30,
    )
    if resp.status_code != 201:
        raise RuntimeError(f"Failed to mint an installation access token ({resp.status_code}): {resp.text}")
    return resp.json()["token"]


def _run_git(args: list, cwd: str, env: Optional[dict] = None) -> None:
    result = subprocess.run(["git"] + args, cwd=cwd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {result.stderr}")


def publish(bundle_dir: str, owner_repo: str, app_id: str, private_key_pem: str, logger) -> str:
    """Publishes the already-generated bundle at bundle_dir as a pull request
    against the client's own repo, using a GitHub App installation token -
    never the App's private key or a raw OAuth user token against the repo
    itself. Never touches the repo's default branch directly; opens a PR for
    the client to review and merge.

    Returns the PR URL.
    """
    owner, repo = owner_repo.split("/", 1)
    logger.info(f"Requesting a short-lived installation access token for '{owner_repo}'...")
    token = _get_installation_access_token(app_id, private_key_pem, owner, repo)

    work_dir = tempfile.mkdtemp(prefix="ingestion_bundle_publish_")
    repo_dir = os.path.join(work_dir, repo)
    try:
        clone_url = f"https://x-access-token:{token}@github.com/{owner}/{repo}.git"
        logger.info(f"Cloning '{owner_repo}'...")
        _run_git(["clone", "--depth", "1", clone_url, repo_dir], cwd=work_dir)

        # A shallow clone checks out the remote's default branch by name -
        # read it back here so the bundle's deploy.yml workflow (which only
        # knows a placeholder at generation time, since the real default
        # branch isn't known until we can actually query/clone this specific
        # repo) fires on the branch that will actually receive the merge, and
        # so the PR targets the same branch without a second API call.
        branch_result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=repo_dir, capture_output=True, text=True,
        )
        base_branch = branch_result.stdout.strip() or "main"

        branch = f"ingestion-bundle/{int(time.time())}"
        _run_git(["checkout", "-b", branch], cwd=repo_dir)

        for entry in os.listdir(bundle_dir):
            src = os.path.join(bundle_dir, entry)
            dst = os.path.join(repo_dir, entry)
            if os.path.isdir(src):
                if os.path.exists(dst):
                    shutil.rmtree(dst)
                shutil.copytree(src, dst)
            else:
                shutil.copy2(src, dst)

        workflow_path = os.path.join(repo_dir, ".github", "workflows", "deploy.yml")
        if os.path.exists(workflow_path):
            with open(workflow_path, "r") as f:
                workflow_content = f.read()
            with open(workflow_path, "w") as f:
                f.write(workflow_content.replace("__DEFAULT_BRANCH__", base_branch))

        _run_git(["add", "-A"], cwd=repo_dir)
        commit_env = os.environ.copy()
        commit_env["GIT_AUTHOR_NAME"] = "ingestion-framework-bot"
        commit_env["GIT_AUTHOR_EMAIL"] = "ingestion-framework-bot@users.noreply.github.com"
        commit_env["GIT_COMMITTER_NAME"] = "ingestion-framework-bot"
        commit_env["GIT_COMMITTER_EMAIL"] = "ingestion-framework-bot@users.noreply.github.com"
        _run_git(["commit", "-m", "Add ingestion framework bundle"], cwd=repo_dir, env=commit_env)

        logger.info(f"Pushing branch '{branch}'...")
        _run_git(["push", "origin", branch], cwd=repo_dir)

        logger.info(f"Opening PR against '{base_branch}'...")
        pr_resp = requests.post(
            f"{GITHUB_API}/repos/{owner}/{repo}/pulls",
            headers={"Authorization": f"token {token}", "Accept": "application/vnd.github+json"},
            json={
                "title": "Add ingestion framework bundle",
                "head": branch,
                "base": base_branch,
                "body": "Generated by the ingestion framework's extraction CLI (`scripts/cli.py`). "
                        "Review before merging - nothing is pushed to this repo's default branch directly.",
            },
            timeout=30,
        )
        if pr_resp.status_code != 201:
            raise RuntimeError(f"Failed to open PR ({pr_resp.status_code}): {pr_resp.text}")
        return pr_resp.json()["html_url"]
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
