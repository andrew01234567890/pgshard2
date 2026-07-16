//! Test fixtures shared by pgshard integration tests: containerized
//! PostgreSQL 18 configured the way pgshard expects (logical WAL).

use anyhow::Context;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

pub const PG_IMAGE_TAG: &str = "18-bookworm";

/// A running PostgreSQL 18 container with `wal_level=logical` and enough
/// slot/sender headroom for replication tests. Stops on drop.
pub struct Pg {
    container: ContainerAsync<Postgres>,
    host: String,
    port: u16,
}

impl Pg {
    pub async fn start() -> anyhow::Result<Self> {
        let container = Postgres::default()
            .with_tag(PG_IMAGE_TAG)
            .with_cmd([
                "postgres",
                "-c",
                "wal_level=logical",
                "-c",
                "max_wal_senders=16",
                "-c",
                "max_replication_slots=16",
            ])
            .start()
            .await
            .context("starting postgres container")?;
        let host = container.get_host().await?.to_string();
        let port = container.get_host_port_ipv4(5432).await?;
        Ok(Pg {
            container,
            host,
            port,
        })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Connection string for the default superuser database.
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user=postgres password=postgres dbname=postgres",
            self.host, self.port
        )
    }

    /// Connects and spawns the connection driver task.
    pub async fn connect(&self) -> anyhow::Result<tokio_postgres::Client> {
        let (client, connection) =
            tokio_postgres::connect(&self.connection_string(), tokio_postgres::NoTls)
                .await
                .context("connecting to postgres container")?;
        tokio::spawn(connection);
        Ok(client)
    }

    pub fn container(&self) -> &ContainerAsync<Postgres> {
        &self.container
    }
}
