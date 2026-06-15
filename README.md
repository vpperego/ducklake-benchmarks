# ducklake-tests

Benchmark for INSERT performance into a **DuckLake** table running in a
**client–server** setup built on DuckDB's **Quack** remote protocol.

## Architecture

Two independent DuckDB instances talk to each other over Quack (HTTP on
`localhost:9494`):

```
┌───────────────────────────┐         Quack / HTTP          ┌──────────────────────────┐
│  Server DuckDB instance   │  ◀──────────────────────────▶ │  Client DuckDB instance  │
│                           │     remote_db.query(SQL)       │                          │
│  • ATTACH ducklake        │                                │  • ATTACH 'quack:...'    │
│    DATA_PATH              │                                │  • INSERT via client     │
│    'data_files_server/'   │                                │                          │
│  • table: user(first,     │                                │                          │
│    last, email, date)     │                                │                          │
│    PARTITIONED BY date    │                                │                          │
│  • CALL quack_serve(...)  │                                │                          │
└───────────────────────────┘                                └──────────────────────────┘
```

* **Server** – attaches a DuckLake catalog whose `DATA_PATH` is
  `data_files_server/`, creates the `user(first_name, last_name, email,
  user_date)` table partitioned by `user_date` (one partition per calendar
  date), and starts the Quack
  server.
* **Client** – attaches the remote via Quack and inserts rows through the
  `remote_db.query()` table macro. Every statement is one HTTP round-trip to
  the server, so the benchmark measures the real client–server INSERT cost.

The two DuckDB instances are separate (`:memory:` databases), and all data
movement between them goes over the Quack HTTP protocol on localhost.

## Two insertion strategies

For a given `number of rows`, the benchmark times both:

1. **One row per INSERT statement** – N statements, each its own commit and
   HTTP round-trip (the naïve pattern).
2. **Single multi-row INSERT** – one statement carrying all N rows.

DuckLake does not support `INSERT ... RETURNING`, so inserts are issued via
`remote_db.query('INSERT INTO lake.user VALUES ...')`, which returns the
affected-row count.

## Run

```bash
uv run python benchmark.py          # default: 100 rows
uv run python benchmark.py 500      # custom row count
```

Example output (100 rows):

```
 one row per INSERT               100 rows |    3.003 s |         33.3 rows/s
 single multi-row INSERT          100 rows |    0.140 s |        714.9 rows/s
 speed-up (single-row / multi-row):    21.5x
 parquet data files written under 'data_files_server/': 30
```

Parquet files land under `data_files_server/main/user/user_date=YYYY-MM-DD/...`,
confirming the `user_date` partitioning.
