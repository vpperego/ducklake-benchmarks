"""DuckLake INSERT benchmark over the DuckDB Quack client–server protocol.

Catalog backend: a local **DuckDB file** (``server_catalog.ducklake``).

The benchmark runs as the **client** process. It launches ``server.py`` as a
separate process (the **server**), waits for it to start listening, runs the two
insertion strategies against the remote DuckLake table, and then terminates the
server process.

Two independent DuckDB *processes* talk over Quack (HTTP on localhost):

  * **server.py** (subprocess) – attaches a DuckLake catalog whose ``DATA_PATH``
    is ``data_files_server/``, creates the date-partitioned ``user`` table, and
    serves it over Quack until the benchmark terminates it.
  * **benchmark.py** (this process, client) – attaches the remote via Quack
    (``ATTACH 'quack:localhost'``) and inserts rows through the
    ``remote_db.query()`` table macro. Every statement is one HTTP round-trip to
    the server, so the benchmark measures the real client–server INSERT cost.

The INSERT methodology itself (row generation, statement building, timing,
printing) is shared with the rest of the suite via :mod:`bench_common`.

Usage::

    uv run python benchmark.py [num_rows]      # default num_rows = 100
"""

from __future__ import annotations

import argparse
import os
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path

import duckdb

from bench_common import QuackRunner, run_insert_benchmark, data_dir_summary

# --- configuration ---------------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parent
SERVER_SCRIPT = PROJECT_DIR / "server.py"
DATA_DIR = PROJECT_DIR / "data_files_server"     # DuckLake parquet storage
CATALOG_FILE = PROJECT_DIR / "server_catalog.ducklake"  # DuckLake metadata catalog
CATALOG_ALIAS = "lake"                           # server-side catalog alias
TOKEN = "benchtoken1234"                         # shared Quack auth token
HOST = "localhost"                               # Quack binds here (plain HTTP locally)
READY_PREFIX = "QUACK_READY"                     # server readiness line prefix
READY_TIMEOUT = 180.0                            # allows first-time extension download


# --- server (subprocess) lifecycle -----------------------------------------

def start_server() -> subprocess.Popen:
    """Launch server.py as a subprocess and block until it signals readiness."""
    cmd = [
        sys.executable, str(SERVER_SCRIPT),
        "--data-dir", str(DATA_DIR),
        "--catalog", str(CATALOG_FILE),
        "--catalog-alias", CATALOG_ALIAS,
        "--token", TOKEN,
    ]
    env = dict(os.environ, PYTHONUNBUFFERED="1")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=str(PROJECT_DIR),
        env=env,
    )

    # Drain stdout on a thread so we can enforce a real readiness timeout
    # (the server is otherwise quiet until it prints the readiness line).
    lines: queue.Queue[str | None] = queue.Queue()

    def _reader() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            lines.put(line)
        lines.put(None)  # EOF -> process exited

    threading.Thread(target=_reader, daemon=True).start()

    captured: list[str] = []
    deadline = time.monotonic() + READY_TIMEOUT
    while time.monotonic() < deadline:
        try:
            line = lines.get(timeout=1.0)
        except queue.Empty:
            if proc.poll() is not None:
                break
            continue
        if line is None:
            break  # server exited before becoming ready
        captured.append(line)
        if line.startswith(READY_PREFIX):
            return proc

    # Not ready in time, or exited unexpectedly.
    stop_server(proc)
    raise RuntimeError(
        "Quack server did not become ready. Captured output:\n"
        + "".join(captured)
    )


def stop_server(proc: subprocess.Popen) -> None:
    """Terminate the server subprocess gracefully, then force if needed."""
    if proc.poll() is not None:
        return
    proc.terminate()  # SIGTERM -> server.py stops quack and exits 0
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


# --- client side -----------------------------------------------------------

def connect_client() -> duckdb.DuckDBPyConnection:
    """Start the client DuckDB instance and attach the Quack remote."""
    con = duckdb.connect(":memory:")
    con.execute("INSTALL quack; LOAD quack;")
    last_err: Exception | None = None
    for _ in range(50):
        try:
            con.execute(f"ATTACH 'quack:{HOST}' AS remote_db (TOKEN '{TOKEN}')")
            return con
        except Exception as err:  # server not ready yet
            last_err = err
            time.sleep(0.1)
    raise RuntimeError(f"could not attach Quack remote: {last_err}")


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
    print(f" DuckLake INSERT benchmark — DuckDB-file catalog over Quack  (num_rows = {n})")
    print("=" * 74)
    print(" starting server process (server.py) ...")

    server_proc = start_server()
    client_con: duckdb.DuckDBPyConnection | None = None
    try:
        client_con = connect_client()
        print(f" client attached to server via 'quack:{HOST}'")

        runner = QuackRunner(client_con, CATALOG_ALIAS)
        run_insert_benchmark(runner, n)

        print()
        data_dir_summary(DATA_DIR, PROJECT_DIR)
    finally:
        if client_con is not None:
            try:
                client_con.execute("DETACH remote_db")
            except Exception:
                pass
            client_con.close()
        print("\n stopping server process ...", flush=True)
        stop_server(server_proc)
        print(" server stopped.")


if __name__ == "__main__":
    main()
