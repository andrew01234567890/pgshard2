# PostgreSQL 18 with pgBackRest bundled — the database image for every shard
# instance. The pgshard-agent binary is injected at pod start by an init
# container (CNPG-style) and runs as PID 1.
FROM postgres:18-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends pgbackrest \
    && rm -rf /var/lib/apt/lists/*
