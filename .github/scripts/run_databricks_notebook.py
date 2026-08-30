"""
.github/scripts/run_databricks_notebook.py

Runs a Databricks notebook via REST API.
Compatible with Databricks Free Edition (serverless compute).

Two execution modes — auto-detected:
  1. Serverless (Free Edition) → uses /api/2.0/jobs/runs/submit
     with serverless_compute_config instead of new_cluster
  2. Classic cluster → falls back if serverless fails

Usage:
  python run_databricks_notebook.py \
    --notebook "/Workspace/Users/me@email.com/03_feature_engineering" \
    --params '{"train_cutoff": "2026-07-16", "run_date": "2026-08-15"}' \
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


def submit_serverless(notebook_path: str,
                      params: dict,
                      timeout: int) -> str:
    """
    Submit notebook run using serverless compute.
    Works on Databricks Free Edition.
    """
    payload = {
        "run_name": f"github_actions_{notebook_path.split('/')[-1]}",
        "timeout_seconds": timeout,
        "notebook_task": {
            "notebook_path":   notebook_path,
            "base_parameters": params,
            "source":          "WORKSPACE",
        },
        # Serverless compute — no cluster config needed
        # Databricks Free Edition only supports this mode
        "queue": {"enabled": True},
    }

    response = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/submit",
        headers=HEADERS,
        json=payload,
    )

    if response.status_code == 200:
        run_id = response.json()["run_id"]
        print(f"  Submitted (serverless) run_id: {run_id}")
        return run_id

    # If serverless fails, try classic cluster config
    print(f"  Serverless submit failed ({response.status_code}), "
          f"trying classic cluster...")
    return submit_classic(notebook_path, params, timeout)


def submit_classic(notebook_path: str,
                   params: dict,
                   timeout: int) -> str:
    """
    Fallback: submit with classic cluster config.
    Used when running on paid Databricks tier.
    """
    payload = {
        "run_name": f"github_actions_{notebook_path.split('/')[-1]}",
        "timeout_seconds": timeout,
        "notebook_task": {
            "notebook_path":   notebook_path,
            "base_parameters": params,
            "source":          "WORKSPACE",
        },
        "new_cluster": {
            "spark_version":    "14.3.x-scala2.12",
            "node_type_id":     "i3.xlarge",
            "num_workers":      0,
            "spark_conf":       {"spark.master": "local[*]"},
            "runtime_engine":   "STANDARD",
        }
    }

    response = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/submit",
        headers=HEADERS,
        json=payload,
    )
    response.raise_for_status()
    run_id = response.json()["run_id"]
    print(f"  Submitted (classic cluster) run_id: {run_id}")
    return run_id


def poll_run(run_id: str, poll_interval: int = 30) -> bool:
    """
    Poll run status until complete.
    Returns True if success, False if failed.
    Prints detailed error on failure.
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
        print(f"  [{elapsed:>4}s] {lc} {rs} {msg[:60] if msg else ''}")

        if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            if rs == "SUCCESS":
                print(f"  ✅ Run {run_id} completed successfully ({elapsed}s)")
                return True
            else:
                # Get notebook output for debugging
                output_response = requests.get(
                    f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get-output",
                    headers=HEADERS,
                    params={"run_id": run_id},
                )
                if output_response.status_code == 200:
                    output = output_response.json()
                    error  = output.get("error", "")
                    if error:
                        print(f"\n  ❌ Notebook error:\n{error[:500]}")

                print(f"  ❌ Run {run_id} failed after {elapsed}s: {rs} — {msg}")
                return False

        time.sleep(poll_interval)


def verify_connection():
    """Quick auth check before submitting."""
    response = requests.get(
        f"{DATABRICKS_HOST}/api/2.0/workspace/list",
        headers=HEADERS,
        params={"path": "/"},
    )
    if response.status_code != 200:
        print(f"❌ Auth failed ({response.status_code}): {response.text[:200]}")
        sys.exit(1)
    print(f"✅ Connected to {DATABRICKS_HOST}")


def main():
    parser = argparse.ArgumentParser(
        description="Run a Databricks notebook via REST API"
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

    run_id  = submit_serverless(notebook, params, timeout)
    success = poll_run(run_id, poll_interval=poll)

    if not success:
        sys.exit(1)    # GitHub Actions marks step as failed


if __name__ == "__main__":
    main()
