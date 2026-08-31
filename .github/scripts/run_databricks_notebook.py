"""
.github/scripts/run_databricks_notebook.py

Runs a Databricks notebook via REST API.
Compatible with Databricks Free Edition (serverless compute).

Databricks Free Edition requires a minimal payload —
no new_cluster, no queue config. Just notebook_task.
The platform assigns serverless compute automatically.

Usage:
  python run_databricks_notebook.py \
    --notebook "/Workspace/Users/me@email.com/03_feature_engineering" \
    --params '{"train_cutoff": "2026-07-16", "run_date": "2026-08-30"}' \
    --timeout 3600
"""

import argparse
import json
import os
import sys
import time
import requests

DATABRICKS_HOST  = os.environ["DATABRICKS_HOST"].rstrip("/")
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]

HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type":  "application/json",
}


def verify_connection():
    """Quick auth check before submitting."""
    response = requests.get(
        f"{DATABRICKS_HOST}/api/2.0/workspace/list",
        headers=HEADERS,
        params={"path": "/"},
    )
    if response.status_code != 200:
        print(f"❌ Auth failed ({response.status_code}): {response.text[:300]}")
        sys.exit(1)
    print(f"✅ Connected to {DATABRICKS_HOST}")


def submit_run(notebook_path: str, params: dict, timeout: int) -> str:
    """
    Submit notebook run with minimal payload.

    Databricks Free Edition (serverless) requires NO cluster config.
    Just notebook_task — the platform assigns compute automatically.
    Adding new_cluster or queue fields causes 400 Bad Request.
    """
    payload = {
        "run_name":       f"github_actions_{notebook_path.split('/')[-1]}",
        "timeout_seconds": timeout,
        "notebook_task": {
            "notebook_path":   notebook_path,
            "base_parameters": params,
            "source":          "WORKSPACE",
        },
        # No new_cluster — Free Edition is serverless only
        # No queue config — causes 400 on Free Edition
    }

    print(f"  Submitting payload:")
    print(f"  {json.dumps(payload, indent=2)}")

    response = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/submit",
        headers=HEADERS,
        json=payload,
    )

    # Always print response body — critical for debugging 400 errors
    print(f"  Response status: {response.status_code}")
    if response.status_code != 200:
        print(f"  Response body: {response.text[:500]}")
        response.raise_for_status()

    run_id = response.json()["run_id"]
    print(f"  ✅ Submitted run_id: {run_id}")
    return run_id


def poll_run(run_id: str, poll_interval: int = 30) -> bool:
    """
    Poll run status until complete.
    Returns True if success, False if failed.
    """
    start_time = time.time()

    while True:
        response = requests.get(
            f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get",
            headers=HEADERS,
            params={"run_id": run_id},
        )
        response.raise_for_status()
        data = response.json()

        state = data.get("state", {})
        lc    = state.get("life_cycle_state", "UNKNOWN")
        rs    = state.get("result_state", "")
        msg   = state.get("state_message", "")

        elapsed = int(time.time() - start_time)
        status_line = f"[{elapsed:>4}s] {lc}"
        if rs:
            status_line += f" | {rs}"
        if msg:
            status_line += f" | {msg[:80]}"
        print(f"  {status_line}")

        if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            if rs == "SUCCESS":
                print(f"  ✅ Completed successfully ({elapsed}s)")
                return True

            # Fetch notebook output for detailed error message
            try:
                output_response = requests.get(
                    f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get-output",
                    headers=HEADERS,
                    params={"run_id": run_id},
                )
                if output_response.status_code == 200:
                    output = output_response.json()
                    error  = output.get("error", "")
                    trace  = output.get("error_trace", "")
                    if error:
                        print(f"\n  ❌ Notebook error: {error[:500]}")
                    if trace:
                        print(f"  Traceback (last 500 chars):\n  {trace[-500:]}")
            except Exception:
                pass

            print(f"  ❌ Run failed after {elapsed}s: {rs} — {msg}")
            return False

        time.sleep(poll_interval)


def main():
    parser = argparse.ArgumentParser(
        description="Run a Databricks notebook via REST API (Free Edition compatible)"
    )
    parser.add_argument("--notebook", required=True,
                        help="Workspace path to notebook")
    parser.add_argument("--params",   default="{}",
                        help="JSON string of notebook parameters")
    parser.add_argument("--timeout",  type=int, default=3600,
                        help="Timeout in seconds (default: 3600)")
    parser.add_argument("--poll",     type=int, default=30,
                        help="Poll interval in seconds (default: 30)")
    args = parser.parse_args()

    notebook = args.notebook
    params   = json.loads(args.params)
    timeout  = args.timeout
    poll     = args.poll

    print(f"\n{'='*55}")
    print(f"  Notebook: {notebook.split('/')[-1]}")
    print(f"  Params:   {json.dumps(params)}")
    print(f"  Timeout:  {timeout}s")
    print(f"{'='*55}")

    verify_connection()

    run_id  = submit_run(notebook, params, timeout)
    success = poll_run(run_id, poll_interval=poll)

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()