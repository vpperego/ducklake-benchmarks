"""DuckLake INSERT benchmark with a **PostgreSQL** metadata catalog (single process).

Catalog backend: **PostgreSQL**, running in a Docker container whose data lives
in the named volume ``pg_data`` (see ``postgres/``).

This benchmark attaches a single DuckLake instance **directly** to the Postgres
catalog and inserts into it. It reuses the INSERT methodology from
:mod:`bench_common` (single-row vs. multi-row statements, timing, printing) via
:class:`bench_common.DirectRunner`. Postgres lifecycle + attach are shared via
:mod:`pg_setup`.

For a multi-process variant (concurrent writers + batched inserts), see
``benchmark_multi.py``.

Lifecycle (managed by this script):

  1. ``docker compose up --wait`` starts Postgres and waits for it to be healthy.
  2. A DuckLake instance is attached to the Postgres catalog, parquet data in
     ``data_files_pg/``.
  3. The two INSERT strategies are timed against the ``user`` table.
  4. ``docker compose down`` stops the container (the ``pg_data`` volume is kept).

Requires Docker + Docker Compose.

Usage::

    uv run python benchmark_postgres.py [num_rows]   # default num_rows = 100
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from bench_common import DirectRunner, run_insert_benchmark, data_dir_summary
from pg_setup import (
    CATALOG_ALIAS,
    DATA_DIR,
    PROJECT_DIR,
    attach_ducklake,
    require_postgres,
)


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

    require_postgres()  # preflight check; container is managed separately
    con: duckdb.DuckDBPyConnection | None = None
    try:
        con = attach_ducklake()
        print(f" attached DuckLake to Postgres (alias: {CATALOG_ALIAS})")

        runner = DirectRunner(con, CATALOG_ALIAS)
        run_insert_benchmark(runner, n)

        print()
        data_dir_summary(DATA_DIR, PROJECT_DIR)
    finally:
        if con is not None:
            con.close()


if __name__ == "__main__":
    main()
