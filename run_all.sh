#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

export PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}"

if [[ ! -d .venv ]]; then
  python3.11 -m venv .venv
fi

source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

pytest -q

python -m uvicorn antigravity.api.main:app --host 0.0.0.0 --port 8000 >/tmp/antigravity_api.log 2>&1 &
API_PID=$!

cleanup() {
  if [[ -n "${API_PID:-}" ]] && kill -0 "$API_PID" 2>/dev/null; then
    kill "$API_PID" 2>/dev/null || true
    wait "$API_PID" 2>/dev/null || true
  fi
  exit 0
}

trap cleanup SIGINT SIGTERM EXIT

python - <<'PY'
import json
import time
import urllib.request


def fetch_json(url: str, method: str = "GET", payload: dict | None = None):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method=method)
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


time.sleep(2)
health = fetch_json("http://127.0.0.1:8000/health")
assert health["status"] == "ok", health
submission = fetch_json(
    "http://127.0.0.1:8000/tasks/submit",
    method="POST",
    payload={
        "nodes": [
            {"id": "start", "function": "double", "args": [2]},
            {"id": "final", "function": "sum", "depends_on": ["start"], "args": [5]},
        ]
    },
)
assert submission["status"] == "succeeded", submission
assert submission["results"]["final"] == 9, submission
print(json.dumps({"demo_status": "ok", "health": health, "submission": submission}))
PY

cleanup
