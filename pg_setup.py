"""Shared PostgreSQL setup for the DuckLake benchmarks that use a Postgres
catalog: Docker container lifecycle (``docker compose``) plus a DuckLake-attach
helper.

Used by ``benchmark_postgres.py`` (single process) and ``benchmark_multi.py``
(many concurrent processes writing to the same catalog).
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
from pathlib import Path

import duckdb

PROJECT_DIR = Path(__file__).resolve().parent
POSTGRES_DIR = PROJECT_DIR / "postgres"
DATA_DIR = PROJECT_DIR / "data_files_pg"          # DuckLake parquet storage
CATALOG_ALIAS = "ducklake_pg"                      # alias used when attaching

# Postgres connection (must match postgres/Dockerfile + docker-compose.yml).
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "ducklake_catalog"
PG_USER = "ducklake"
PG_PASSWORD = "ducklake"

READY_TIMEOUT = 120.0


# --- container lifecycle ---------------------------------------------------

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


def require_postgres() -> None:
    """Fail fast if the Postgres container is not reachable.

    The benchmarks do **not** deploy the container — it is managed separately
    (``uv run python pg_setup.py up``) so it lives between executions. This is a
    preflight check only.
    """
    if not _port_open(PG_HOST, PG_PORT):
        raise SystemExit(
            f"Postgres is not reachable on {PG_HOST}:{PG_PORT}.\n"
            f"Start it first with:  uv run python pg_setup.py up"
        )


def main() -> None:
    """CLI entry point: ``uv run python pg_setup.py {up|down|status}``."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Manage the Postgres catalog container for the DuckLake benchmarks."
    )
    parser.add_argument("action", choices=["up", "down", "status"],
                        help="up = start (+build, wait healthy); down = stop; status = check")
    args = parser.parse_args()

    if args.action == "up":
        ensure_postgres()
        print("Postgres is up.")
    elif args.action == "down":
        stop_postgres()
        print("Postgres stopped.")
    else:  # status
        if _port_open(PG_HOST, PG_PORT):
            print(f"Postgres is UP on {PG_HOST}:{PG_PORT}.")
        else:
            print(f"Postgres is DOWN (nothing listening on {PG_HOST}:{PG_PORT}).")
            raise SystemExit(1)


if __name__ == "__main__":
    main()


# --- DuckLake attach --------------------------------------------------------

def attach_ducklake(alias: str = CATALOG_ALIAS, data_dir: Path | str = DATA_DIR):
    """Attach a DuckLake instance to the Postgres catalog; return the connection."""
    data_dir = str(data_dir)
    os.makedirs(data_dir, exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute("INSTALL postgres; LOAD postgres;")
    dsn = (
        f"dbname={PG_DB} host={PG_HOST} port={PG_PORT} "
        f"user={PG_USER} password={PG_PASSWORD}"
    )
    last_err: Exception | None = None
    for _ in range(60):
        try:
            con.execute(
                f"ATTACH 'ducklake:postgres:{dsn}' AS {alias} "
                f"(DATA_PATH '{data_dir}/')"
            )
            return con
        except Exception as err:
            last_err = err
            time.sleep(0.5)
    raise RuntimeError(f"could not attach DuckLake to Postgres: {last_err}")
