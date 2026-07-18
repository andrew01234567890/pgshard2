use std::net::SocketAddr;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use tonic::transport::Server;

use pgshard_agent::pg::PgInstance;
use pgshard_agent::service::AgentSvc;
use pgshard_proto::v1::agent_service_server::AgentServiceServer;

#[derive(Parser)]
#[command(
    name = "pgshard-agent",
    version,
    about = "pgshard PostgreSQL instance agent"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Supervise the local PostgreSQL instance and serve the AgentService.
    Run {
        /// gRPC listen address.
        #[arg(long, env = "PGSHARD_AGENT_LISTEN", default_value = "0.0.0.0:9090")]
        listen: SocketAddr,
        /// libpq connection string for the local instance.
        #[arg(
            long,
            env = "PGSHARD_PG_CONN",
            default_value = "host=/var/run/postgresql user=postgres dbname=postgres"
        )]
        pg_conn: String,
        /// This instance's pod name (a Promote aimed elsewhere is refused).
        #[arg(long, env = "PGSHARD_POD")]
        pod: String,
        /// This pod's Kubernetes UID (downward API); identity-sensitive
        /// requests aimed at another pod uid are refused.
        #[arg(long, env = "PGSHARD_POD_UID", default_value = "")]
        pod_uid: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    match Cli::parse().command {
        Command::Run {
            listen,
            pg_conn,
            pod,
            pod_uid,
        } => {
            let svc = AgentSvc::with_pod_uid(Arc::new(PgInstance::new(pg_conn)), pod, pod_uid);
            tracing::info!(%listen, "pgshard-agent serving");
            Server::builder()
                .add_service(AgentServiceServer::new(svc))
                .serve(listen)
                .await?;
            Ok(())
        }
    }
}
