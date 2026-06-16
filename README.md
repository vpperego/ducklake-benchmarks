# ducklake-tests

A small suite of benchmarks for **INSERT performance into a DuckLake table**:

| Benchmark | Catalog backend | Transport | Data dir |
| --- | --- | --- | --- |
| `benchmark.py` | DuckDB file (`server_catalog.ducklake`) | **Quack** client–server (HTTP) | `data_files_server/` |
| `benchmark_postgres.py` | **PostgreSQL** (Docker container) | direct attach | `data_files_pg/` |
| `benchmark_multi.py` | **PostgreSQL** (Docker container) | **N concurrent processes** + batched inserts | `data_files_pg/` |

All insert into the same `user(first_name, last_name, email, user_date)` table,
partitioned by `user_date`. The single-process benchmarks time two strategies for
a given `number of rows`:

1. **one row per INSERT** – N statements (N commits / round-trips).
2. **single multi-row INSERT** – one statement carrying all N rows.

The multi-process benchmark instead writes N rows in **batches** across
`--processes` concurrent writers.

The INSERT logic (row generation, statement building, timing, printing) lives in
**`bench_common.py`**; the Postgres container lifecycle + DuckLake-attach helper
live in **`pg_setup.py`**. Both are imported by the benchmarks that need them.

---

## `benchmark.py` — DuckDB-file catalog over Quack

Two separate DuckDB **processes** talk over Quack (HTTP on `localhost:9494`).
`benchmark.py` (the client) launches `server.py` (the server) as a subprocess,
waits for it to listen, runs the strategies, and terminates it:

```
┌────────────────────────────┐         Quack / HTTP          ┌──────────────────────────┐
│  server.py  (subprocess)   │  ◀──────────────────────────▶ │  benchmark.py (client)   │
│  • ATTACH ducklake         │     remote_db.query(SQL)       │  • ATTACH 'quack:...'    │
│    DATA_PATH               │   ◀── launched & terminated ──│  • INSERT via client     │
│    'data_files_server/'    │          by benchmark.py       │                          │
│  • table: user(...)        │                                │                          │
│    PARTITIONED BY date     │                                │                          │
│  • CALL quack_serve(...)   │                                │                          │
│  • prints "QUACK_READY"    │                                │                          │
└────────────────────────────┘                                └──────────────────────────┘
```

The client inserts through the Quack `remote_db.query()` macro, so every
statement is a real HTTP round-trip to the server. (DuckLake has no
`INSERT ... RETURNING`, so inserts are issued via the query macro, which returns
the affected-row count.)

## `benchmark_postgres.py` — PostgreSQL catalog

The DuckLake **metadata catalog** lives in PostgreSQL, running as a Docker
container whose data is persisted in the named volume `pg_data` (see
`postgres/`). The benchmark attaches a single DuckLake instance **directly** to
that catalog — no Quack layer — and inserts into it.

```
┌─────────────────────────┐        ┌─────────────────────────────────────┐
│  benchmark_postgres.py  │        │  postgres/  (Docker)                │
│  • ATTACH ducklake:     │ ──────▶│  PostgreSQL 17                      │
│    postgres:...         │ attach │  • db: ducklake_catalog             │
│    AS ducklake_pg       │        │  • pg_data volume                   │
│  • INSERT (direct)      │        │  (metadata: tables/snapshots/files) │
└─────────────────────────┘        └─────────────────────────────────────┘
        │
        └── parquet data ──▶ data_files_pg/   (on host, not in the container)
```

The benchmark manages the container lifecycle for you: `docker compose up
--wait` to start it (and wait for health), then `docker compose down` at the end
(the `pg_data` volume is kept, so the catalog persists across runs). Requires
Docker + Docker Compose.

## `benchmark_multi.py` — Postgres catalog, many concurrent writers + batches

The same Postgres catalog, but **N DuckLake processes write to the `user` table
concurrently**, each writing its share of the rows in **batches** (one multi-row
INSERT per batch, each its own commit) instead of a single burst:

```
                    ┌─ worker 0 ─┐  ┌─ worker 1 ─┐  ┌─ worker N ─┐
   benchmark_multi  │ ATTACH     │  │ ATTACH     │  │ ATTACH     │
   (orchestrator)   │ batched    │  │ batched    │  │ batched    │
        │  spawns N │ INSERTs    │  │ INSERTs    │  │ INSERTs    │
        └──────────▶│            │  │            │  │            │
                    └─────┬──────┘  └─────┬──────┘  └─────┬──────┘
                          └────────┬──────┴────────┬────────┘
                                   ▼               ▼
                         ┌─────────────────────────────┐
                         │  PostgreSQL (Docker)         │
                         │  shared ducklake_catalog      │
                         │  + pg_data volume             │
                         └─────────────────────────────┘
```

The orchestrator ensures Postgres, resets the table once, spawns `--processes`
worker subprocesses, waits for all of them, verifies the total row count, and
reports per-worker timings + aggregate throughput. Each worker re-invokes this
script in `--worker` mode, attaches its own DuckLake instance, and inserts its
share of `num_rows` in batches of `--batch-size`. Concurrent catalog commits are
handled with a bounded per-batch retry. Requires Docker + Docker Compose.

---

## Files

```
benchmark.py            benchmark 1: DuckDB-file catalog over Quack (client)
server.py               standalone Quack server (launched by benchmark.py)
benchmark_postgres.py   benchmark 2: Postgres catalog (direct attach, single process)
benchmark_multi.py      benchmark 3: Postgres catalog, N concurrent writers + batches
bench_common.py         SHARED INSERT logic (row gen, statements, timing, runners)
pg_setup.py             SHARED Postgres container lifecycle + DuckLake-attach helper
postgres/
├── Dockerfile          postgres:17 + catalog db + healthcheck
├── docker-compose.yml  service + pg_data volume + port 5432
└── README.md           how to drive the container manually
```

In `bench_common.py` a `Runner` abstracts how SQL reaches the table:
`QuackRunner` (via `remote_db.query()`) for benchmark 1, `DirectRunner` (local
connection) for benchmarks 2 and 3. Both return identical result shapes, so the
timing code is transport-agnostic.

## Run

```bash
# benchmark 1 — DuckDB-file catalog over Quack
uv run python benchmark.py [num_rows]            # default 100

# benchmark 2 — Postgres catalog, single process (needs Docker)
uv run python benchmark_postgres.py [num_rows]   # default 100

# benchmark 3 — Postgres catalog, N concurrent writers + batches (needs Docker)
uv run python benchmark_multi.py [num_rows] [-p PROCESSES] [-b BATCH_SIZE]
# defaults: num_rows=1000, processes=4, batch-size=100
```

You can also run the Quack server standalone (blocks until `Ctrl-C`/`SIGTERM`):

```bash
uv run python server.py [--token TOKEN] [--data-dir DIR] [--catalog FILE]
```

Drive the Postgres container manually:

```bash
docker compose -f postgres/docker-compose.yml up -d --wait
docker compose -f postgres/docker-compose.yml down      # keep pg_data volume
docker compose -f postgres/docker-compose.yml down -v   # wipe the volume too
```

## Example output

`benchmark.py` (100 rows):

```
 one row per INSERT               100 rows |    3.089 s |         32.4 rows/s
 single multi-row INSERT          100 rows |    0.249 s |        402.2 rows/s
 speed-up (single-row / multi-row):    12.4x
 parquet data files written under 'data_files_server/': 98
```

`benchmark_postgres.py` (100 rows):

```
 one row per INSERT               100 rows |    2.269 s |         44.1 rows/s
 single multi-row INSERT          100 rows |    0.287 s |        348.5 rows/s
 speed-up (single-row / multi-row):     7.9x
 parquet data files written under 'data_files_pg/': 98
```

`benchmark_multi.py` (2000 rows, 4 processes, batch 200):

```
 spawning 4 worker processes; row shares = [500, 500, 500, 500]

worker  rows   batches  retries    time     throughput
--------------------------------------------------------------
  0       500        3        0    0.342s     1461.7 rows/s
  1       500        3        0    0.598s      836.7 rows/s
  2       500        3        0    0.475s     1052.3 rows/s
  3       500        3        0    0.745s      671.2 rows/s
--------------------------------------------------------------

 total rows : 2000  | table holds: 2000  [OK]
 wall time  :   1.560 s   (spawn -> all workers done)
 throughput :    1282.0 rows/s   (4 processes)
 retries    : 0 total across workers
```

Parquet files land under `<data_dir>/main/user/user_date=YYYY-MM-DD/...`,
confirming the `user_date` partitioning.
