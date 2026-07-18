//! The `pgshard-router` binary: a PostgreSQL wire proxy that routes each query
//! to the right shard database using a compiled topology.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use pgshard_router::Router;
use pgshard_router::sequence::PgBlockReserver;
use pgshard_router::wire::{Backend, Handlers, Proxy};
use pgshard_seq::SequenceCache;
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

    // The write lease renews only when the ACTIVE router's view is
    // re-confirmed (see watch_topology_leased). A poll interval at or beyond
    // the lease would make writes flap off between polls even when everything
    // is healthy — refuse the misconfiguration at startup.
    let lease = router.load().write_lease();
    if Duration::from_secs(args.poll_seconds) >= lease {
        anyhow::bail!(
            "poll interval ({}s) must be shorter than the topology's writeLeaseSeconds ({}s): \
             a healthy router could not renew its write lease between polls",
            args.poll_seconds,
            lease.as_secs()
        );
    }
    // Seed from the initial validation's own stamps: construction time here
    // could be arbitrarily later than the read that produced the snapshot.
    let freshness = pgshard_topo::Freshness::new();
    freshness.install(&watcher.subscribe_validated().borrow());
    tokio::spawn(pgshard_router::watch_topology_leased(
        router.clone(),
        updates,
        Some(pgshard_router::LeaseWiring {
            validated: watcher.subscribe_validated(),
            freshness: freshness.clone(),
            poll_interval: Duration::from_secs(args.poll_seconds),
        }),
    ));

    let backend = Backend {
        user: args.backend_user,
        password: args.backend_password,
        system_database: args.system_database,
    };
    // Reserve sequence blocks from the system database, if the topology names
    // one. Built from the initial endpoint; a system-shard endpoint change needs
    // a router restart to pick up (a follow-up), though the reserver reconnects
    // through transient failures on its own.
    // Typed setters, never a formatted conninfo string: a credential containing
    // whitespace or quotes must not split into extra connection options.
    let system_conn = router.load().system_endpoint().map(|ep| {
        pgshard_router::sequence::reserver_config(
            &ep.host,
            ep.port,
            &backend.user,
            &backend.password,
            &backend.system_database,
        )
    });
    let proxy = std::sync::Arc::new(
        match system_conn {
            Some(conn) => {
                let seq = std::sync::Arc::new(SequenceCache::new(PgBlockReserver::new(conn)));
                tracing::info!("sequence allocation enabled via the system database");
                Proxy::with_sequences(router, backend, seq)
            }
            None => Proxy::new(router, backend),
        }
        .with_freshness(freshness),
    );

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
