"""Shared INSERT benchmark logic for the DuckLake benchmark suite.

Both benchmarks in the suite (``benchmark.py`` – DuckDB-file catalog over the
Quack client–server protocol, and ``benchmark_postgres.py`` – Postgres catalog,
direct attach) build on the same methodology defined here:

  * synthetic row generation (Faker names/emails, last-30-day dates),
  * construction of the single-row and single-multi-row INSERT statements,
  * timing of each strategy,
  * result / data-dir printing.

The transport (how a SQL string reaches the DuckLake table) is abstracted by a
:class:`Runner`:

  * :class:`QuackRunner`  – sends SQL through the Quack ``remote_db.query()``
    macro on a client connection.
  * :class:`DirectRunner` – runs SQL straight on a local DuckDB connection that
    has the catalog attached.

Both transports return the *same* result shapes (INSERT -> ``[(count,)]``,
DDL -> ``[]``, ``SELECT count(*)`` -> ``[(n,)]``), so everything above the
runner is transport-agnostic. This is the "separate Python file" the two
benchmark scripts import their INSERT logic from.
"""

from __future__ import annotations

import os
import time
from datetime import date, timedelta

# --- schema (single source of truth, shared with server.py) ----------------

TABLE = "user"
TABLE_SCHEMA = "first_name VARCHAR, last_name VARCHAR, email VARCHAR, user_date DATE"
PARTITION_BY = "user_date"  # identity partition -> one folder per calendar date


# --- data generation + SQL building ----------------------------------------

def generate_rows(num_rows: int, *, seed: int = 42):
    """Return ``num_rows`` synthetic ``(first_name, last_name, email, date)`` tuples.

    First/last names and emails are generated with Faker; the date is a random
    day within the last 30 days. ``seed`` makes the output reproducible.
    """
    from faker import Faker  # lazy import keeps server.py's import light

    fake = Faker()
    fake.seed_instance(seed)

    today = date.today()
    rows = []
    for _ in range(num_rows):
        first = fake.first_name()
        last = fake.last_name()
        email = fake.email()
        user_date = today - timedelta(days=fake.random_int(min=0, max=29))
        rows.append((first, last, email, user_date))
    return rows


def _sql_literal(value: str) -> str:
    """Quote a Python string as a DuckDB SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def _row_values(row) -> str:
    first, last, email, user_date = row
    return (
        f"({_sql_literal(first)}, {_sql_literal(last)}, "
        f"{_sql_literal(email)}, DATE '{user_date.isoformat()}')"
    )


def build_single_insert(qualified_table: str, row) -> str:
    return f"INSERT INTO {qualified_table} VALUES {_row_values(row)}"


def build_multi_insert(qualified_table: str, rows) -> str:
    return (
        f"INSERT INTO {qualified_table} VALUES "
        + ", ".join(_row_values(row) for row in rows)
    )


# --- transport abstraction --------------------------------------------------

class Runner:
    """How SQL reaches the target DuckLake table (``qualified_table`` = ``alias.table``)."""

    def __init__(self, qualified_table: str):
        self.table = qualified_table

    def execute(self, sql: str):  # pragma: no cover - interface
        raise NotImplementedError

    def insert(self, sql: str):
        return self.execute(sql)

    def count(self) -> int:
        return int(self.execute(f"SELECT count(*) FROM {self.table}")[0][0])

    def reset(self) -> None:
        """Drop and recreate the partitioned table so each run starts clean."""
        for sql in (
            f"DROP TABLE IF EXISTS {self.table}",
            f"CREATE TABLE {self.table} ({TABLE_SCHEMA})",
            f"ALTER TABLE {self.table} SET PARTITIONED BY ({PARTITION_BY})",
        ):
            self.execute(sql)


class QuackRunner(Runner):
    """Run SQL on the remote server via the Quack ``remote_db.query()`` macro."""

    def __init__(self, client_con, alias: str, table: str = TABLE):
        super().__init__(f"{alias}.{table}")
        self.con = client_con

    def execute(self, sql: str):
        return self.con.execute("SELECT * FROM remote_db.query(?)", [sql]).fetchall()


class DirectRunner(Runner):
    """Run SQL directly on a local DuckDB connection that has the catalog attached."""

    def __init__(self, con, alias: str, table: str = TABLE):
        super().__init__(f"{alias}.{table}")
        self.con = con

    def execute(self, sql: str):
        return self.con.execute(sql).fetchall()


# --- timed strategies -------------------------------------------------------

def time_single_row_inserts(runner: Runner, num_rows: int) -> float:
    """Insert ``num_rows`` rows, one INSERT statement each."""
    rows = generate_rows(num_rows)
    start = time.perf_counter()
    for row in rows:
        runner.insert(build_single_insert(runner.table, row))
    elapsed = time.perf_counter() - start
    assert runner.count() == num_rows, "row count mismatch after single-row inserts"
    return elapsed


def time_multi_row_insert(runner: Runner, num_rows: int) -> float:
    """Insert ``num_rows`` rows in a single multi-row INSERT statement."""
    rows = generate_rows(num_rows)
    sql = build_multi_insert(runner.table, rows)
    start = time.perf_counter()
    runner.insert(sql)
    elapsed = time.perf_counter() - start
    assert runner.count() == num_rows, "row count mismatch after multi-row insert"
    return elapsed


# --- reporting --------------------------------------------------------------

def print_result(label: str, num_rows: int, elapsed: float) -> None:
    rate = num_rows / elapsed if elapsed > 0 else float("inf")
    print(
        f"  {label:<28} {num_rows:>7} rows | "
        f"{elapsed:8.3f} s | {rate:12.1f} rows/s"
    )


def run_insert_benchmark(runner: Runner, num_rows: int) -> dict:
    """Run both INSERT strategies against ``runner``, printing and returning timings."""
    print("\nStrategy                      rows       time            throughput")
    print("-" * 74)

    runner.reset()
    t_single = time_single_row_inserts(runner, num_rows)
    print_result("one row per INSERT", num_rows, t_single)
    print(f"    -> table now holds {runner.count()} rows")

    runner.reset()
    t_multi = time_multi_row_insert(runner, num_rows)
    print_result("single multi-row INSERT", num_rows, t_multi)
    print(f"    -> table now holds {runner.count()} rows")

    print("-" * 74)
    if t_multi > 0:
        print(f"  speed-up (single-row / multi-row): {t_single / t_multi:7.1f}x")
    return {"single_row": t_single, "multi_row": t_multi}


def data_dir_summary(data_dir, base_dir=None) -> None:
    """Print how many parquet files were written under ``data_dir``."""
    parquet = []
    for root, _dirs, files in os.walk(data_dir):
        for f in files:
            if f.endswith(".parquet"):
                parquet.append(os.path.join(root, f))
    shown = data_dir if base_dir is None else os.path.relpath(data_dir, base_dir)
    print(f"  parquet data files written under '{shown}/': {len(parquet)}")
    if parquet:
        example = parquet[0] if base_dir is None else os.path.relpath(parquet[0], base_dir)
        print(f"  example path: {example}")
