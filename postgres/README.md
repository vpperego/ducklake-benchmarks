# Postgres catalog container

PostgreSQL used as the **DuckLake metadata catalog** for `benchmark_postgres.py`.
The catalog metadata (tables, snapshots, file pointers) lives in the
`ducklake_catalog` database; the actual Parquet data files are written by the
benchmark to `../data_files_pg/` on the host.

`benchmark_postgres.py` manages this container's lifecycle automatically
(`docker compose up --wait` / `docker compose down`), but you can also drive it
manually:

```bash
docker compose up -d --wait      # build + start, wait until healthy
docker compose ps                # check status
docker compose down              # stop + remove container (keeps the pg_data volume)
docker compose down -v           # also wipe the pg_data volume (fresh catalog next run)
```

Connection defaults (set in the `Dockerfile`, exposed on `localhost:5432`):

| setting | value             |
| ------- | ----------------- |
| user    | `ducklake`        |
| password| `ducklake`        |
| db      | `ducklake_catalog`|

The `pg_data` named volume is mounted at `/var/lib/postgresql/data` (the
container's PGDATA), so the catalog survives container recreation. DuckLake
requires the catalog database to pre-exist; the image creates it via
`POSTGRES_DB=ducklake_catalog`.
