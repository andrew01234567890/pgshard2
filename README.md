# pgshard

Sharded PostgreSQL on Kubernetes: a Rust data plane (router, per-pod agent) and a Go operator
managing the full lifecycle — provisioning, HA/failover, online resharding, online DDL,
backups/PITR, and a unified cross-shard CDC stream.

## Layout

| Path | What |
|---|---|
| `crates/` | Rust workspace: `pgshard-router`, `pgshard-agent`, `pgshard-ctl` and libraries |
| `operator/` | Go Kubernetes operator (kubebuilder) |
| `proto/` | gRPC contract between operator, agents, and routers |
| `images/` | Container image builds |
| `test/` | KIND e2e, chaos, and load test suites |

## Requirements

- PostgreSQL 18+
- Kubernetes (KIND for local development)
- Rust (pinned in `rust-toolchain.toml`), Go (pinned in `operator/go.mod`)

## Development

```sh
make build   # build everything
make test    # unit + envtest suites
make lint    # rustfmt, clippy, golangci-lint
```

Status: pre-release, milestone 1 in progress.
