//! The `pgshard-router` binary: a PostgreSQL wire proxy that routes each query
//! to the right shard database using a compiled topology.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use pgshard_router::Router;
use pgshard_router::wire::{Backend, Handlers, Proxy};
use pgshard_topo::Topology;
use tokio::net::TcpListener;

#[derive(Parser)]
#[command(about = "pgshard PostgreSQL wire router")]
struct Args {
    /// Address to listen for client connections on.
    #[arg(long, env = "PGSHARD_ROUTER_LISTEN", default_value = "0.0.0.0:6432")]
    listen: SocketAddr,

    /// Path to the compiled topology (JSON) to route against.
    #[arg(long, env = "PGSHARD_ROUTER_TOPOLOGY")]
    topology: std::path::PathBuf,

    /// Backend role the router connects to shards as.
    #[arg(long, env = "PGSHARD_ROUTER_BACKEND_USER", default_value = "postgres")]
    backend_user: String,

    /// Password for the backend role.
    #[arg(long, env = "PGSHARD_ROUTER_BACKEND_PASSWORD", default_value = "")]
    backend_password: String,

    /// Name of the unsharded system database.
    #[arg(
        long,
        env = "PGSHARD_ROUTER_SYSTEM_DATABASE",
        default_value = "pgshard_system"
    )]
    system_database: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    let topology_json = std::fs::read_to_string(&args.topology).context("reading topology file")?;
    let topology: Topology =
        serde_json::from_str(&topology_json).context("parsing topology JSON")?;
    let router = Arc::new(Router::build(&topology).context("building router from topology")?);
    tracing::info!(epoch = router.epoch(), "loaded topology");

    let proxy = Arc::new(Proxy::new(
        router,
        Backend {
            user: args.backend_user,
            password: args.backend_password,
            system_database: args.system_database,
        },
    ));

    let listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    tracing::info!(addr = %args.listen, "router listening");

    loop {
        let (socket, peer) = listener.accept().await.context("accepting connection")?;
        let handlers = Handlers::new(proxy.clone());
        tokio::spawn(async move {
            if let Err(e) = pgwire::tokio::process_socket(socket, None, handlers).await {
                tracing::warn!(%peer, error = %e, "connection ended with error");
            }
        });
    }
}
