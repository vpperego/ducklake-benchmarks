"""DuckLake INSERT benchmark over the DuckDB Quack client-server protocol.

Architecture (two independent DuckDB instances connected over Quack/HTTP):

  * **Server** – a DuckDB instance that attaches a DuckLake catalog whose
    ``DATA_PATH`` is ``data_files_server/``, creates a date-partitioned
    ``user`` table there, and starts the Quack server on localhost.

  * **Client** – a second, independent DuckDB instance that attaches the
    remote via Quack (``ATTACH 'quack:localhost'``) and inserts rows through
    the ``remote_db.query()`` table macro. Every statement is one HTTP
    round-trip to the server, so the benchmark measures the real client-server
    INSERT cost.

Two insertion strategies are compared for a given ``number of rows``:

  1. ``benchmark_single_row_inserts`` – one INSERT statement per row.
  2. ``benchmark_multi_row_insert``    – a single INSERT with all rows.

Each strategy is timed with ``time.perf_counter`` and the result is printed.

Usage::

    uv run python benchmark.py [num_rows]      # default num_rows = 100
"""

from __future__ import annotations

import argparse
import os
import random
import time
from datetime import date, timedelta

import duckdb

# --- configuration ---------------------------------------------------------

DATA_DIR = "data_files_server"          # where DuckLake writes parquet data
CATALOG_FILE = "server_catalog.ducklake" # DuckLake metadata catalog file
CATALOG_ALIAS = "lake"                   # server-side catalog alias
TABLE = "user"                           # benchmark target table
TOKEN = "benchtoken1234"                 # shared Quack auth token
HOST = "localhost"                       # Quack binds here (plain HTTP locally)

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael",
    "Linda", "William", "Elizabeth", "David", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
]


# --- data generation -------------------------------------------------------

def generate_rows(num_rows: int, *, seed: int = 42):
    """Return ``num_rows`` synthetic ``(first_name, last_name, email, date)`` tuples."""
    rng = random.Random(seed)
    start, end = date(2015, 1, 1), date(2024, 12, 31)
    span = (end - start).days
    rows = []
    for i in range(num_rows):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        email = f"{first.lower()}.{last.lower()}{i}@example.com"
        user_date = start + timedelta(days=rng.randint(0, span))
        rows.append((first, last, email, user_date))
    return rows


def _sql_literal(value: str) -> str:
    """Quote a Python string as a DuckDB SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


# --- server side -----------------------------------------------------------

def reset_table(server_con: duckdb.DuckDBPyConnection) -> None:
    """(Re)create the partitioned ``user`` table so each run starts clean."""
    server_con.execute(f"USE {CATALOG_ALIAS}")
    server_con.execute(f"DROP TABLE IF EXISTS {TABLE}")
    server_con.execute(
        f"CREATE TABLE {TABLE} ("
        "first_name VARCHAR, last_name VARCHAR, email VARCHAR, user_date DATE)"
    )
    # Identity partitioning on the DATE column -> one partition per calendar
    # date, i.e. `user_date` is the partition key (user_date=YYYY-MM-DD).
    server_con.execute(f"ALTER TABLE {TABLE} SET PARTITIONED BY (user_date)")


def start_server() -> duckdb.DuckDBPyConnection:
    """Start the DuckDB server: DuckLake catalog + partitioned table + Quack."""
    os.makedirs(DATA_DIR, exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute("INSTALL quack; LOAD quack;")
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute(
        f"ATTACH 'ducklake:{CATALOG_FILE}' AS {CATALOG_ALIAS} "
        f"(DATA_PATH '{DATA_DIR}/')"
    )
    reset_table(con)
    con.execute(f"CALL quack_serve('quack:{HOST}', token := '{TOKEN}')")
    return con


def stop_server(server_con: duckdb.DuckDBPyConnection) -> None:
    server_con.execute(f"CALL quack_stop('quack:{HOST}')")


# --- client side -----------------------------------------------------------

def connect_client() -> duckdb.DuckDBPyConnection:
    """Start the client DuckDB instance and attach the Quack remote."""
    con = duckdb.connect(":memory:")
    con.execute("INSTALL quack; LOAD quack;")
    # The server listener is up immediately, but retry briefly to be robust.
    last_err: Exception | None = None
    for _ in range(50):
        try:
            con.execute(f"ATTACH 'quack:{HOST}' AS remote_db (TOKEN '{TOKEN}')")
            return con
        except Exception as err:  # server not ready yet
            last_err = err
            time.sleep(0.1)
    raise RuntimeError(f"could not attach Quack remote: {last_err}")


def _remote_insert(client_con: duckdb.DuckDBPyConnection, sql: str) -> int:
    """Run an INSERT on the server via the Quack macro; return affected rows."""
    rows = client_con.execute(
        "SELECT * FROM remote_db.query(?)", [sql]
    ).fetchall()
    return int(rows[0][0]) if rows else 0


def _remote_count(client_con: duckdb.DuckDBPyConnection) -> int:
    rows = client_con.execute(
        "SELECT * FROM remote_db.query(?)",
        [f"SELECT count(*) FROM {CATALOG_ALIAS}.{TABLE}"],
    ).fetchall()
    return int(rows[0][0])


# --- benchmarks ------------------------------------------------------------

def benchmark_single_row_inserts(client_con: duckdb.DuckDBPyConnection, num_rows: int):
    """Insert ``num_rows`` rows, one INSERT statement each (one commit/round-trip per row)."""
    rows = generate_rows(num_rows)
    start = time.perf_counter()
    total = 0
    for first, last, email, user_date in rows:
        sql = (
            f"INSERT INTO {CATALOG_ALIAS}.{TABLE} VALUES "
            f"({_sql_literal(first)}, {_sql_literal(last)}, "
            f"{_sql_literal(email)}, DATE '{user_date.isoformat()}')"
        )
        total += _remote_insert(client_con, sql)
    elapsed = time.perf_counter() - start
    assert total == num_rows, f"inserted {total}, expected {num_rows}"
    return elapsed


def benchmark_multi_row_insert(client_con: duckdb.DuckDBPyConnection, num_rows: int):
    """Insert ``num_rows`` rows in a single multi-row INSERT statement."""
    rows = generate_rows(num_rows)
    values = ", ".join(
        f"({_sql_literal(first)}, {_sql_literal(last)}, "
        f"{_sql_literal(email)}, DATE '{user_date.isoformat()}')"
        for first, last, email, user_date in rows
    )
    sql = f"INSERT INTO {CATALOG_ALIAS}.{TABLE} VALUES {values}"
    start = time.perf_counter()
    total = _remote_insert(client_con, sql)
    elapsed = time.perf_counter() - start
    assert total == num_rows, f"inserted {total}, expected {num_rows}"
    return elapsed


# --- reporting -------------------------------------------------------------

def _print_result(label: str, num_rows: int, elapsed: float) -> None:
    rate = num_rows / elapsed if elapsed > 0 else float("inf")
    print(
        f"  {label:<28} {num_rows:>7} rows | "
        f"{elapsed:8.3f} s | {rate:12.1f} rows/s"
    )


def _data_dir_summary() -> None:
    parquet = []
    for root, _dirs, files in os.walk(DATA_DIR):
        for f in files:
            if f.endswith(".parquet"):
                parquet.append(os.path.join(root, f))
    print(f"  parquet data files written under '{DATA_DIR}/': {len(parquet)}")
    if parquet:
        print(f"  example path: {parquet[0]}")


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
    print(f" DuckLake INSERT benchmark over the Quack protocol  (num_rows = {n})")
    print("=" * 74)

    server_con = start_server()
    client_con = None
    try:
        client_con = connect_client()

        print("\nStrategy                      rows       time            throughput")
        print("-" * 74)

        reset_table(server_con)
        t_single = benchmark_single_row_inserts(client_con, n)
        _print_result("one row per INSERT", n, t_single)
        print(f"    -> server now holds {_remote_count(client_con)} rows")

        reset_table(server_con)
        t_multi = benchmark_multi_row_insert(client_con, n)
        _print_result("single multi-row INSERT", n, t_multi)
        print(f"    -> server now holds {_remote_count(client_con)} rows")

        print("-" * 74)
        if t_multi > 0:
            print(f"  speed-up (single-row / multi-row): {t_single / t_multi:7.1f}x")

        print()
        _data_dir_summary()
    finally:
        if client_con is not None:
            try:
                client_con.execute("DETACH remote_db")
            except Exception:
                pass
        stop_server(server_con)


if __name__ == "__main__":
    main()
