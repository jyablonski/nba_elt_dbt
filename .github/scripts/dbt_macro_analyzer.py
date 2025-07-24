import json
import os
import subprocess
from pathlib import Path
import requests

# environment variables from GitHub Actions
PR_NUMBER = os.getenv("PR_NUMBER")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO = os.getenv("GITHUB_REPOSITORY")  # in the form of org/repo

MANIFEST_PATH = Path("target/manifest.json")


def strip_prefix(name: str) -> str:
    prefixes = ["macro.nba_elt_project.", "model.nba_elt_project."]
    for prefix in prefixes:
        if name.startswith(prefix):
            return name[len(prefix) :]
    return name


def get_changed_macro_files(base_branch: str = "origin/master") -> list[str]:
    result = subprocess.check_output(
        ["git", "diff", "--name-only", base_branch, "--", "macros/"]
    )
    files = result.decode().splitlines()
    return [f for f in files if f.endswith(".sql")]


def extract_macro_names_from_manifest(files: list[str], manifest: dict) -> list[str]:
    changed_macros = []
    for macro_id, macro_data in manifest.get("macros", {}).items():
        file_path = macro_data.get("path")
        if file_path and any(file_path.endswith(f) for f in files):
            changed_macros.append(macro_id)
    return changed_macros


def find_models_depending_on_macros(
    macros: list[str], manifest: dict
) -> dict[str, list[str]]:
    affected_models = {}
    for node_id, node_data in manifest.get("nodes", {}).items():
        if node_data.get("resource_type") != "model":
            continue
        depends_on_macros = node_data.get("depends_on", {}).get("macros", [])
        intersecting = list(set(depends_on_macros) & set(macros))
        for macro in intersecting:
            affected_models.setdefault(macro, []).append(node_id)
    return affected_models


def generate_comment(
    changed_macros: list[str], affected_models: dict[str, list[str]]
) -> str:
    # add an HTML comment marker so we can find it later
    comment_id_marker = "<!-- macro-analyzer-comment -->"
    clean_macros = [strip_prefix(m) for m in changed_macros]

    total_affected = len(set(m for models in affected_models.values() for m in models))
    body = f"{comment_id_marker}\n### 🧪 Macro Impact Analysis\n\n"
    body += f"- Changed macros: {', '.join(clean_macros)}\n"
    body += f"- Total models affected: **{total_affected}**\n\n"

    for macro, models in affected_models.items():
        clean_macro = strip_prefix(macro)
        body += f"#### `{clean_macro}` used in {len(models)} models:\n"
        for model in models:
            clean_model = strip_prefix(model)
            body += f"- `{clean_model}`\n"
        body += "\n"

    if total_affected == 0:
        body += "✅ No dbt models are affected by these macro changes."

    return body


def post_github_comment(
    pr_number: int, repo: str, token: str, body: str, marker: str
) -> None:
    """
    Function which either posts a new PR comment, or updates the existing
    one already on the PR
    """

    existing_comment_id = find_existing_comment(pr_number, repo, token, marker)

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
    }

    if existing_comment_id:
        url = (
            f"https://api.github.com/repos/{repo}/issues/comments/{existing_comment_id}"
        )
        response = requests.patch(url, json={"body": body}, headers=headers)
        response.raise_for_status()
        print("Updated existing comment.")
    else:
        url = f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments"
        response = requests.post(url, json={"body": body}, headers=headers)
        response.raise_for_status()
        print("Created new comment.")


def find_existing_comment(
    pr_number: int, repo: str, token: str, marker: str
) -> int | None:
    url = f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    comments = response.json()
    for comment in comments:
        if (
            comment["user"]["login"] == "github-actions[bot]"
            and marker in comment["body"]
        ):
            return comment["id"]

    return None


def main():
    marker = "<!-- macro-analyzer-comment -->"
    changed_files = get_changed_macro_files()
    if not changed_files:
        print("No macros changed.")
        return

    if not MANIFEST_PATH.exists():
        print("manifest.json not found. Did dbt compile fail?")
        return

    with open(MANIFEST_PATH, "r") as f:
        manifest = json.load(f)

    changed_macro_ids = extract_macro_names_from_manifest(changed_files, manifest)
    if not changed_macro_ids:
        print("No macros matched the changed files.")
        return

    model_map = find_models_depending_on_macros(changed_macro_ids, manifest)
    comment = generate_comment(changed_macro_ids, model_map)
    print(comment)

    post_github_comment(PR_NUMBER, REPO, GITHUB_TOKEN, comment, marker)


if __name__ == "__main__":
    main()
