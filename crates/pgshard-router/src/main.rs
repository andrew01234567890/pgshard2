use clap::Parser;
use pgshard_wire::ProxyConfig;

/// pgshard-router — phase-1 passthrough spike.
#[derive(Parser)]
#[command(version)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:6432")]
    listen: std::net::SocketAddr,
    #[arg(long)]
    backend_host: String,
    #[arg(long, default_value_t = 5432)]
    backend_port: u16,
    /// Router credential for the backend; prefer the env var so the secret
    /// stays out of process listings.
    #[arg(long, env = "PGSHARD_BACKEND_PASSWORD", hide_env_values = true)]
    backend_password: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    pgshard_wire::run(ProxyConfig {
        listen: args.listen,
        backend_host: args.backend_host,
        backend_port: args.backend_port,
        backend_password: args.backend_password,
    })
    .await?;
    Ok(())
}
