"""Standalone DuckDB Quack server for the DuckLake INSERT benchmark.

This is the SERVER side of the client–server benchmark. It is meant to be
launched (and later terminated) as a **separate process** by ``benchmark.py``,
but it can also be run on its own::

    uv run python server.py

What it does, in order:

  * attaches a DuckLake catalog whose ``DATA_PATH`` is ``data_files_server/``,
  * (re)creates the date-partitioned ``user`` table,
  * starts the Quack server on ``quack:localhost`` with the given token,
  * prints a single readiness line to stdout and serves until SIGTERM/SIGINT,
  * on shutdown it stops the Quack server and closes the catalog cleanly.

Readiness protocol (consumed by ``benchmark.py``)::

    QUACK_READY token=<token>

is printed exactly once, to stdout, after the server is listening.
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import threading

import duckdb

from bench_common import TABLE_SCHEMA, PARTITION_BY

# Defaults are mirrored in benchmark.py; benchmark.py passes these explicitly
# when it launches this script so the two always agree.
DATA_DIR_DEFAULT = "data_files_server"
CATALOG_DEFAULT = "server_catalog.ducklake"
CATALOG_ALIAS_DEFAULT = "lake"
TABLE_DEFAULT = "user"
TOKEN_DEFAULT = "benchtoken1234"
HOST = "localhost"
READY_PREFIX = "QUACK_READY"


def create_schema(con: duckdb.DuckDBPyConnection, catalog_alias: str, table: str) -> None:
    """(Re)create the date-partitioned target table on the server side."""
    con.execute(f"USE {catalog_alias}")
    con.execute(f"DROP TABLE IF EXISTS {table}")
    con.execute(f"CREATE TABLE {table} ({TABLE_SCHEMA})")
    # Identity partitioning on the DATE column -> one partition per calendar
    # date (folders like user_date=YYYY-MM-DD).
    con.execute(f"ALTER TABLE {table} SET PARTITIONED BY ({PARTITION_BY})")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", default=DATA_DIR_DEFAULT)
    parser.add_argument("--catalog", default=CATALOG_DEFAULT)
    parser.add_argument("--catalog-alias", default=CATALOG_ALIAS_DEFAULT)
    parser.add_argument("--table", default=TABLE_DEFAULT)
    parser.add_argument("--token", default=TOKEN_DEFAULT)
    args = parser.parse_args(argv)

    os.makedirs(args.data_dir, exist_ok=True)

    con = duckdb.connect(":memory:")
    con.execute("INSTALL quack; LOAD quack;")
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute(
        f"ATTACH 'ducklake:{args.catalog}' AS {args.catalog_alias} "
        f"(DATA_PATH '{args.data_dir}/')"
    )
    create_schema(con, args.catalog_alias, args.table)
    con.execute(f"CALL quack_serve('quack:{HOST}', token := '{args.token}')")

    # Tell the parent we are up and listening.
    print(f"{READY_PREFIX} token={args.token}", flush=True)

    # Serve until asked to stop. The signal handler just flips the flag;
    # cleanup happens on this main thread so it stays simple and safe.
    stop_requested = threading.Event()

    def _on_signal(_signum, _frame):
        stop_requested.set()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    stop_requested.wait()

    # Graceful shutdown.
    try:
        con.execute(f"CALL quack_stop('quack:{HOST}')")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
