"""Upload the project to GitHub using the REST API. No git required."""
import os
import base64
from pathlib import Path
from dotenv import load_dotenv
import requests

load_dotenv()
TOKEN = os.environ["GITHUB_TOKEN"]
OWNER = "baheldeepti"
REPO = "autonomous-analyst"
BRANCH = "main"

SKIP_DIRS = {".venv", "__pycache__", ".git", "node_modules", "data", "demo"}
SKIP_FILES = {".env", ".DS_Store"}
SKIP_SUFFIXES = {".pyc", ".pyo"}

API = f"https://api.github.com/repos/{OWNER}/{REPO}/contents"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}


def should_skip(path: Path) -> bool:
    if path.name in SKIP_FILES:
        return True
    if path.suffix in SKIP_SUFFIXES:
        return True
    for part in path.parts:
        if part in SKIP_DIRS:
            return True
    return False


def get_existing_sha(remote_path: str):
    r = requests.get(f"{API}/{remote_path}?ref={BRANCH}", headers=HEADERS)
    if r.status_code == 200:
        return r.json().get("sha")
    return None


def upload_file(local_path: Path, remote_path: str):
    with open(local_path, "rb") as f:
        content_b64 = base64.b64encode(f.read()).decode()

    sha = get_existing_sha(remote_path)
    payload = {
        "message": f"Add {remote_path}",
        "content": content_b64,
        "branch": BRANCH,
    }
    if sha:
        payload["sha"] = sha
        payload["message"] = f"Update {remote_path}"

    r = requests.put(f"{API}/{remote_path}", headers=HEADERS, json=payload)
    if r.status_code in (200, 201):
        size_kb = len(content_b64) * 3 // 4 // 1024
        action = "updated" if sha else "created"
        print(f"  {action:8s} {remote_path} ({size_kb} KB)")
    else:
        print(f"  FAILED   {remote_path}: {r.status_code} {r.text[:200]}")


def main():
    root = Path(".").resolve()
    files = []
    for p in root.rglob("*"):
        if p.is_file() and not should_skip(p.relative_to(root)):
            files.append(p)
    # upload small files first, big DB last
    files.sort(key=lambda p: (p.stat().st_size, str(p)))

    print(f"Uploading {len(files)} files to {OWNER}/{REPO}:\n")
    for p in files:
        remote = str(p.relative_to(root))
        upload_file(p, remote)
    print("\nDone.")


if __name__ == "__main__":
    main()
