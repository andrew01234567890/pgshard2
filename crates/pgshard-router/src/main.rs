//! The `pgshard-router` binary: a PostgreSQL wire proxy that routes each query
//! to the right shard database using a compiled topology.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use pgshard_router::Router;
use pgshard_router::wire::{Backend, Handlers, Proxy};
use pgshard_topo::{FileWatcher, TopologyWatcher};
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

    /// How often to re-read the topology file for a higher epoch, in seconds.
    #[arg(long, env = "PGSHARD_ROUTER_POLL_SECONDS", default_value = "5")]
    poll_seconds: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    // Watch the topology file: the initial snapshot must build, and higher
    // epochs are applied online (see pgshard_router::watch_topology).
    let watcher = FileWatcher::start(&args.topology, Duration::from_secs(args.poll_seconds))
        .await
        .context("starting topology watcher")?;
    let updates = watcher.subscribe();
    let initial = updates.borrow().clone();
    let router = pgshard_router::shared(
        Router::build(&initial).context("building router from the initial topology")?,
    );
    tracing::info!(epoch = router.load().epoch(), "loaded initial topology");
    tokio::spawn(pgshard_router::watch_topology(router.clone(), updates));

    let proxy = std::sync::Arc::new(Proxy::new(
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
