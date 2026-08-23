# .github/scripts/run_databricks_notebook.py
"""
Submits a Databricks notebook run via REST API and polls until complete.
Used by GitHub Actions ML pipeline workflow.
"""

import argparse
import json
import os
import sys
import time
import requests

DATABRICKS_HOST  = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]

HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type":  "application/json",
}


def submit_notebook_run(notebook_path: str,
                        params: dict,
                        timeout: int = 3600) -> str:
    """Submit notebook run and return run_id."""

    payload = {
        "run_name": f"github_actions_{notebook_path.split('/')[-1]}",
        "timeout_seconds": timeout,
        "notebook_task": {
            "notebook_path":   notebook_path,
            "base_parameters": params,
        },
        "new_cluster": {
            "spark_version": "14.3.x-scala2.12",
            "node_type_id":  "i3.xlarge",
            "num_workers":   0,
            "spark_conf":    {"spark.master": "local[*]"},
        }
    }

    response = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/submit",
        headers=HEADERS,
        json=payload,
    )
    response.raise_for_status()
    run_id = response.json()["run_id"]
    print(f"  Submitted run_id: {run_id}")
    return run_id


def poll_run(run_id: str, poll_interval: int = 30) -> bool:
    """Poll run until complete. Returns True if success, False if failed."""

    while True:
        response = requests.get(
            f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get",
            headers=HEADERS,
            params={"run_id": run_id},
        )
        response.raise_for_status()
        data  = response.json()
        state = data["state"]
        lc    = state.get("life_cycle_state", "UNKNOWN")
        rs    = state.get("result_state", "")

        print(f"  Status: {lc} {rs}")

        if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            if rs == "SUCCESS":
                print(f"  ✅ Run {run_id} completed successfully")
                return True
            else:
                msg = state.get("state_message", "No message")
                print(f"  ❌ Run {run_id} failed: {msg}")
                return False

        time.sleep(poll_interval)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--notebook", required=True)
    parser.add_argument("--params",   default="{}")
    parser.add_argument("--timeout",  type=int, default=3600)
    args = parser.parse_args()

    notebook = args.notebook
    params   = json.loads(args.params)
    timeout  = args.timeout

    print(f"\nRunning notebook: {notebook}")
    print(f"Parameters:       {json.dumps(params, indent=2)}")

    run_id  = submit_notebook_run(notebook, params, timeout)
    success = poll_run(run_id)

    if not success:
        sys.exit(1)    # non-zero exit → GitHub Actions marks step as failed


if __name__ == "__main__":
    main()