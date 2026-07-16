# PostgreSQL 18 with pgBackRest bundled — the database image for every shard
# instance. The pgshard-agent binary is injected at pod start by an init
# container (CNPG-style) and runs as PID 1.
FROM postgres:18-bookworm@sha256:1961f96e6029a02c3812d7cb329a3b03a3ac2bb067058dec17b0f5596aca9296

RUN apt-get update \
    && apt-get install -y --no-install-recommends pgbackrest \
    && rm -rf /var/lib/apt/lists/*
