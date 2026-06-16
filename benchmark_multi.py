"""DuckLake INSERT benchmark with **multiple concurrent processes** writing to
a shared **PostgreSQL** catalog.

This benchmark measures how DuckLake behaves when several processes each attach
their own DuckLake instance to the *same* Postgres-backed catalog and insert
into the *same* ``user`` table at the same time. Rows are written in **batches**
(one multi-row INSERT per batch, each its own commit) rather than a single burst.

Architecture:

  * The orchestrator (this script, run normally) checks Postgres is up, resets
    the ``user`` table once, then spawns ``--processes`` worker **subprocesses**.
  * Each worker attaches its own DuckLake instance to the Postgres catalog and
    inserts ``num_rows`` rows (the **full** amount — the workload is *not* split
    across workers) in batches of ``--batch-size``, reporting a JSON timing line.
  * The orchestrator waits for all workers, verifies the total row count, and
    prints per-worker timings + aggregate throughput.

Postgres is **not** deployed by this script — manage it separately so it lives
between executions::

    uv run python pg_setup.py up      # start (+build, wait healthy)
    uv run python benchmark_multi.py  # run the benchmark (re-run freely)
    uv run python pg_setup.py down    # stop when done

Parameters:

  * ``num_rows``      – rows written by **each** worker (total = num_rows × processes).
  * ``--processes``   – number of concurrent DuckLake processes.
  * ``--batch-size``  – rows per multi-row INSERT batch.

Requires Docker + Docker Compose (for Postgres, managed externally).

Usage::

    uv run python benchmark_multi.py [num_rows] [--processes P] [--batch-size B]
    # defaults: num_rows=1000, processes=4, batch-size=100
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import duckdb

from bench_common import DirectRunner, time_batched_inserts
from pg_setup import (
    CATALOG_ALIAS,
    PROJECT_DIR,
    attach_ducklake,
    require_postgres,
)

SCRIPT = Path(__file__).resolve()
WORKER_RETRIES = 8  # per-batch retries on catalog commit conflicts


# --- helpers ---------------------------------------------------------------

def parse_worker_output(raw: str) -> dict:
    """Return the last JSON object printed by a worker (or an error dict)."""
    for line in reversed(raw.splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    return {"error": f"no JSON output from worker; raw:\n{raw.strip()}"}


# --- worker (runs in a spawned subprocess) ---------------------------------

def run_worker(worker_id: int, rows: int, batch_size: int) -> int:
    """Attach DuckLake and write ``rows`` rows in batches; print a JSON result."""
    con = None
    try:
        con = attach_ducklake()
        runner = DirectRunner(con, CATALOG_ALIAS)
        elapsed, n_batches, n_retries = time_batched_inserts(
            runner, rows, batch_size, seed=worker_id, retries=WORKER_RETRIES
        )
        print(json.dumps({
            "id": worker_id,
            "rows": rows,
            "batches": n_batches,
            "retries": n_retries,
            "elapsed": round(elapsed, 6),
        }))
        return 0
    except Exception as err:  # surface failures as JSON so the parent can report
        print(json.dumps({"id": worker_id, "error": f"{type(err).__name__}: {err}"}))
        return 1
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


# --- orchestrator -----------------------------------------------------------

def run(num_rows: int, num_processes: int, batch_size: int) -> int:
    print("=" * 74)
    print(
        f" DuckLake multi-process INSERT — Postgres catalog  "
        f"(rows/worker={num_rows}, processes={num_processes}, batch_size={batch_size})"
    )
    print("=" * 74)

    require_postgres()  # preflight check; container is managed separately
    con: duckdb.DuckDBPyConnection | None = None
    try:
        # One writer creates a clean table before the workers start.
        con = attach_ducklake()
        DirectRunner(con, CATALOG_ALIAS).reset()
        con.close()
        con = None

        expected_total = num_rows * num_processes
        print(
            f"\n spawning {num_processes} worker processes; each writes "
            f"{num_rows} rows in batches of {batch_size}  (total = {expected_total})"
        )

        env = dict(os.environ, PYTHONUNBUFFERED="1")
        procs: list[tuple[int, subprocess.Popen]] = []
        start = time.perf_counter()
        for i in range(num_processes):
            cmd = [
                sys.executable, str(SCRIPT),
                "--worker", "--wid", str(i),
                "--wrows", str(num_rows), "--wbatch", str(batch_size),
            ]
            procs.append((i, subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, cwd=str(PROJECT_DIR), env=env,
            )))

        # All workers were spawned immediately; communicate() just joins + reads.
        results: list[dict] = []
        for i, proc in procs:
            out, _ = proc.communicate()
            res = parse_worker_output(out)
            res.setdefault("id", i)
            res["returncode"] = proc.returncode
            results.append(res)
        wall = time.perf_counter() - start

        # Verify the global row count.
        con = attach_ducklake()
        total_rows = DirectRunner(con, CATALOG_ALIAS).count()

        # --- report ---
        print("\nworker  rows   batches  retries    time     throughput")
        print("-" * 62)
        total_retries = 0
        failed = False
        for res in sorted(results, key=lambda r: r.get("id", 0)):
            if "error" in res:
                failed = True
                print(f"  {res.get('id'):<5}  ERROR: {res['error']}")
                continue
            elapsed = res["elapsed"]
            rate = res["rows"] / elapsed if elapsed > 0 else float("inf")
            total_retries += res.get("retries", 0)
            print(
                f"  {res['id']:<5} {res['rows']:>5}  {res['batches']:>7}  "
                f"{res.get('retries', 0):>7}  {elapsed:7.3f}s  {rate:9.1f} rows/s"
            )
        print("-" * 62)

        throughput = expected_total / wall if wall > 0 else float("inf")
        status = "OK" if total_rows == expected_total else (
            f"MISMATCH (got {total_rows}, expected {expected_total})"
        )
        print(
            f"\n total rows : {expected_total}  | table holds: {total_rows}  [{status}]\n"
            f" wall time  : {wall:7.3f} s   (spawn -> all workers done)\n"
            f" throughput : {throughput:9.1f} rows/s   ({num_processes} processes)\n"
            f" retries    : {total_retries} total across workers"
        )
        if failed:
            print("\n one or more workers failed.")
            return 1
        return 0
    finally:
        if con is not None:
            con.close()


# --- entry point -----------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("num_rows", nargs="?", type=int, default=1000,
                        help="rows written by EACH worker; total = num_rows × processes "
                             "(default: 1000)")
    parser.add_argument("-p", "--processes", type=int, default=4,
                        help="number of concurrent DuckLake processes (default: 4)")
    parser.add_argument("-b", "--batch-size", dest="batch_size", type=int, default=100,
                        help="rows per multi-row INSERT batch (default: 100)")
    # Internal flags used when this script re-invokes itself as a worker.
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--wid", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--wrows", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--wbatch", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker:
        sys.exit(run_worker(args.wid, args.wrows, args.wbatch))
    sys.exit(run(args.num_rows, args.processes, args.batch_size))


if __name__ == "__main__":
    main()
