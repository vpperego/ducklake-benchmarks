# PostgreSQL image used as the DuckLake metadata catalog.
#
# DuckLake (via the DuckDB `postgres` extension) requires PostgreSQL >= 12.
# The catalog metadata lives in its own database, `ducklake_catalog`, which the
# benchmark expects to find ready before it attaches DuckLake.

FROM postgres:17

# Credentials + catalog database (baked in so the image is self-describing and
# matches the connection defaults used by benchmark_postgres.py).
ENV POSTGRES_USER=ducklake \
    POSTGRES_PASSWORD=ducklake \
    POSTGRES_DB=ducklake_catalog

# The catalog DB is only "ready" once it actually accepts connections.
HEALTHCHECK --interval=3s --timeout=3s --retries=30 --start-period=5s \
    CMD pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" || exit 1
