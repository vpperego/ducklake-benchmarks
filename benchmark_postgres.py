"""DuckLake INSERT benchmark with a **PostgreSQL** metadata catalog.

Catalog backend: **PostgreSQL**, running in a Docker container whose data lives
in the named volume ``pg_data`` (see ``postgres/``).

Unlike ``benchmark.py`` (which measures inserts over the Quack client–server
protocol), this benchmark attaches a single DuckLake instance **directly** to the
Postgres catalog and inserts into it. It reuses the exact INSERT methodology from
:mod:`bench_common` (row generation, single-row vs. multi-row statements,
timing, printing) via :class:`bench_common.DirectRunner`.

Lifecycle (managed by this script):

  1. ``docker compose up --wait`` starts the Postgres container and waits for it
     to be healthy (the ``pg_data`` volume persists across runs).
  2. A DuckLake instance is attached to the Postgres catalog, with parquet data
     written to ``data_files_pg/``.
  3. The two INSERT strategies are timed against the ``user`` table.
  4. ``docker compose down`` stops the container (the ``pg_data`` volume is kept).

Requires Docker + Docker Compose.

Usage::

    uv run python benchmark_postgres.py [num_rows]   # default num_rows = 100
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import duckdb

from bench_common import DirectRunner, run_insert_benchmark, data_dir_summary

# --- configuration ---------------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parent
POSTGRES_DIR = PROJECT_DIR / "postgres"
DATA_DIR = PROJECT_DIR / "data_files_pg"        # DuckLake parquet storage
CATALOG_ALIAS = "ducklake_pg"                   # local alias for the attached catalog

# Postgres connection (must match postgres/Dockerfile + docker-compose.yml).
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "ducklake_catalog"
PG_USER = "ducklake"
PG_PASSWORD = "ducklake"

READY_TIMEOUT = 120.0


# --- Postgres container lifecycle ------------------------------------------

def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def ensure_postgres() -> None:
    """Start the Postgres container and wait until it accepts connections."""
    print(" starting Postgres container (docker compose up --wait) ...", flush=True)
    subprocess.run(
        ["docker", "compose", "up", "-d", "--wait", "--build"],
        cwd=str(POSTGRES_DIR),
        check=True,
    )
    deadline = time.monotonic() + READY_TIMEOUT
    while time.monotonic() < deadline:
        if _port_open(PG_HOST, PG_PORT):
            return
        time.sleep(0.5)
    raise RuntimeError("Postgres did not become reachable on port 5432")


def stop_postgres() -> None:
    """Stop + remove the container; keep the pg_data volume (data persists)."""
    print(" stopping Postgres container (docker compose down; volume kept) ...", flush=True)
    subprocess.run(
        ["docker", "compose", "down"],
        cwd=str(POSTGRES_DIR),
        check=True,
    )


# --- DuckLake attach --------------------------------------------------------

def attach_ducklake() -> duckdb.DuckDBPyConnection:
    """Attach a DuckLake instance to the Postgres catalog."""
    os.makedirs(DATA_DIR, exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute("INSTALL postgres; LOAD postgres;")
    dsn = (
        f"dbname={PG_DB} host={PG_HOST} port={PG_PORT} "
        f"user={PG_USER} password={PG_PASSWORD}"
    )
    # The catalog DB has just become ready; retry the attach briefly.
    last_err: Exception | None = None
    for _ in range(60):
        try:
            con.execute(
                f"ATTACH 'ducklake:postgres:{dsn}' AS {CATALOG_ALIAS} "
                f"(DATA_PATH '{DATA_DIR}/')"
            )
            return con
        except Exception as err:
            last_err = err
            time.sleep(0.5)
    raise RuntimeError(f"could not attach DuckLake to Postgres: {last_err}")


# --- entry point -----------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "num_rows", nargs="?", type=int, default=100,
        help="number of rows to insert in each benchmark (default: 100)",
    )
    args = parser.parse_args()
    n = args.num_rows

    print("=" * 74)
    print(f" DuckLake INSERT benchmark — Postgres catalog  (num_rows = {n})")
    print("=" * 74)

    ensure_postgres()
    con: duckdb.DuckDBPyConnection | None = None
    try:
        con = attach_ducklake()
        print(f" attached DuckLake to Postgres (db={PG_DB})")

        runner = DirectRunner(con, CATALOG_ALIAS)
        run_insert_benchmark(runner, n)

        print()
        data_dir_summary(DATA_DIR, PROJECT_DIR)
    finally:
        if con is not None:
            con.close()
        stop_postgres()


if __name__ == "__main__":
    main()
