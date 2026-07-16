use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::backend::{self, BackendError};
use crate::frame::write_frame;
use crate::startup::{Initial, StartupError, read_initial};

#[derive(Debug, Clone)]
pub struct ProxyConfig {
    pub listen: SocketAddr,
    pub backend_host: String,
    pub backend_port: u16,
    /// Router-terminated auth: the router's own credential for the backend.
    /// The spike accepts any frontend connection without a password check.
    pub backend_password: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Startup(#[from] StartupError),
    #[error(transparent)]
    Backend(#[from] BackendError),
    #[error("client sent no startup message")]
    NoStartup,
}

/// Accept loop; runs until the listener fails.
pub async fn run(config: ProxyConfig) -> Result<(), ProxyError> {
    let listener = TcpListener::bind(config.listen).await?;
    info!(listen = %config.listen, "pgshard-router (passthrough spike) listening");
    let config = Arc::new(config);
    loop {
        let (client, peer) = listener.accept().await?;
        let config = Arc::clone(&config);
        tokio::spawn(async move {
            if let Err(err) = handle_session(client, &config).await {
                warn!(%peer, error = %err, "session ended with error");
            }
        });
    }
}

/// Bind address helper for tests: run on an ephemeral port, report it back.
pub async fn run_on_listener(listener: TcpListener, config: ProxyConfig) -> Result<(), ProxyError> {
    let config = Arc::new(config);
    loop {
        let (client, peer) = listener.accept().await?;
        let config = Arc::clone(&config);
        tokio::spawn(async move {
            if let Err(err) = handle_session(client, &config).await {
                warn!(%peer, error = %err, "session ended with error");
            }
        });
    }
}

async fn handle_session(mut client: TcpStream, config: &ProxyConfig) -> Result<(), ProxyError> {
    client.set_nodelay(true)?;
    // Negotiation loop: refuse TLS/GSS (spike), then expect a startup packet.
    let startup = loop {
        match read_initial(&mut client).await? {
            Initial::SslRequest | Initial::GssEncRequest => {
                client.write_all(b"N").await?;
            }
            Initial::CancelRequest { .. } => {
                // Spike: no cancel-key routing yet.
                return Ok(());
            }
            Initial::Startup(params) => break params,
        }
    };

    let user = startup.get("user").unwrap_or("postgres").to_string();
    let database = startup.get("database").unwrap_or(&user).to_string();
    debug!(user, database, "client startup");

    let backend = backend::connect(
        &config.backend_host,
        config.backend_port,
        &user,
        &database,
        &config.backend_password,
    )
    .await?;

    // AuthenticationOk, then replay the backend's startup parameters and
    // cancel key, then the backend's ReadyForQuery.
    write_frame(&mut client, b'R', &0i32.to_be_bytes()).await?;
    let mut backend_stream = backend.stream;
    for frame in &backend.startup_frames {
        write_frame(&mut client, frame.tag, &frame.body).await?;
    }
    write_frame(&mut client, backend.ready.tag, &backend.ready.body).await?;

    // Transparent relay from here on: the spike measures raw passthrough
    // cost; frame-aware routing replaces this in the real router.
    let (c2b, b2c) = tokio::io::copy_bidirectional(&mut client, &mut backend_stream).await?;
    debug!(c2b, b2c, "session closed");
    Ok(())
}
