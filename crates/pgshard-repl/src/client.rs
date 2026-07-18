//! A minimal PostgreSQL logical-replication client.
//!
//! tokio-postgres has no replication support and pgwire's client-api cannot send
//! the `replication=database` startup parameter or expose the `CopyBoth` stream,
//! so this is the design's "own minimal backend client": a hand-rolled connection
//! that authenticates with SCRAM (via `postgres-protocol`), creates a logical
//! slot, issues `START_REPLICATION`, and streams the `CopyData` frames — whose
//! payloads it hands to [`crate::stream`] and [`crate::pgoutput`].
//!
//! It owns just enough of the wire protocol for logical replication; it is not a
//! general SQL client. TLS is a follow-up (M1 clusters are reached over the pod
//! network); connect over plain TCP for now.

use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use pgshard_core::Lsn;
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use postgres_protocol::message::frontend;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::pgoutput::DecodeError;
use crate::stream::{self, MicrosSince2000, ReplicationMessage, StandbyStatusUpdate};

/// Connection parameters for a replication connection.
#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

/// Why a replication operation failed.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("connection closed by the server")]
    Closed,
    #[error("server error: {0}")]
    Server(String),
    #[error("authentication failed: {0}")]
    Auth(String),
    #[error("unexpected server message type byte {0:#04x}")]
    Unexpected(u8),
    #[error("malformed server message: {0}")]
    Malformed(String),
    #[error("pgoutput decode error: {0}")]
    Decode(#[from] DecodeError),
}

type Result<T> = std::result::Result<T, ClientError>;

/// Reject any server message whose length header exceeds this. Real PostgreSQL
/// bounds messages near 1 GiB; a larger claim is an adversarial length that would
/// otherwise drive an unbounded buffer growth or a stalled read.
const MAX_MESSAGE_LEN: usize = 1 << 30;

/// How long a handshake or command read may block before it is treated as a dead
/// or hostile peer. The streaming read ([`ReplicationClient::next`]) is exempt —
/// it legitimately waits for the next WAL record.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);

/// Whether `name` is safe to interpolate into a replication command. Slot and
/// publication names reach the wire as simple-query text, and replication mode
/// also accepts SQL, so an unvalidated name is an injection vector. Accept only a
/// conservative identifier charset (a superset of PostgreSQL's slot-name rule of
/// lower-case letters, digits, and underscore).
fn is_safe_ident(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// One chunk of the logical-replication stream: an `XLogData` message whose
/// `data` is a pgoutput payload to feed to a [`crate::pgoutput::PgOutputDecoder`].
#[derive(Debug, Clone)]
pub struct XLogData {
    pub wal_start: Lsn,
    pub wal_end: Lsn,
    pub server_time: MicrosSince2000,
    pub data: Bytes,
}

/// A logical-replication connection in streaming mode.
pub struct ReplicationClient {
    stream: TcpStream,
    read_buf: BytesMut,
    /// The highest end-of-WAL the server has reported; reported as the *written*
    /// position in standby status updates.
    last_wal_end: Lsn,
    /// The consumer's durable position, reported as the *flushed* position so the
    /// server advances the slot (and releases WAL) only up to what the consumer
    /// has committed. Left at 0 until the consumer sets it via [`Self::confirm`],
    /// so an un-checkpointed consumer never lets the slot move past a replayable
    /// point.
    confirmed_lsn: Lsn,
}

impl ReplicationClient {
    /// Connect, run the startup + SCRAM handshake in replication mode, and wait
    /// for the server to become ready.
    pub async fn connect(config: &Config) -> Result<Self> {
        let stream = TcpStream::connect((config.host.as_str(), config.port)).await?;
        let mut client = ReplicationClient {
            stream,
            read_buf: BytesMut::with_capacity(16 * 1024),
            last_wal_end: Lsn(0),
            confirmed_lsn: Lsn(0),
        };
        client.startup(config).await?;
        Ok(client)
    }

    async fn startup(&mut self, config: &Config) -> Result<()> {
        let mut buf = BytesMut::new();
        // `replication=database` enters logical replication while still allowing
        // SQL and the CREATE_REPLICATION_SLOT / START_REPLICATION commands.
        frontend::startup_message(
            [
                ("user", config.user.as_str()),
                ("database", config.database.as_str()),
                ("replication", "database"),
                ("client_encoding", "UTF8"),
            ],
            &mut buf,
        )
        .map_err(|e| ClientError::Malformed(e.to_string()))?;
        self.stream.write_all(&buf).await?;

        self.authenticate(config).await?;

        // Drain parameter statuses / backend key data until the server is ready.
        loop {
            let (tag, _body) = self.read_control().await?;
            match tag {
                b'Z' => return Ok(()),   // ReadyForQuery
                b'S' | b'K' => continue, // ParameterStatus / BackendKeyData
                b'E' => return Err(server_error(&_body)),
                other => return Err(ClientError::Unexpected(other)),
            }
        }
    }

    async fn authenticate(&mut self, config: &Config) -> Result<()> {
        let (tag, mut body) = self.read_control().await?;
        if tag == b'E' {
            return Err(server_error(&body));
        }
        if tag != b'R' {
            return Err(ClientError::Unexpected(tag));
        }
        match read_i32(&mut body)? {
            // AuthenticationOk with no challenge. A server that skips SCRAM has not
            // proven it knows the password, so accepting this while a password is
            // configured is a trust downgrade a spoofed endpoint could exploit —
            // refuse it. Trust is only honoured for a passwordless config.
            0 => {
                if config.password.is_empty() {
                    return Ok(());
                }
                return Err(ClientError::Auth(
                    "server requested no authentication but a password is configured; \
                     refusing the trust downgrade"
                        .to_owned(),
                ));
            }
            10 => {} // AuthenticationSASL
            other => {
                return Err(ClientError::Auth(format!(
                    "unsupported authentication method {other} (only SCRAM-SHA-256)"
                )));
            }
        }
        // The body lists the offered SASL mechanisms as NUL-terminated strings.
        if !mechanisms(&body).iter().any(|m| m == "SCRAM-SHA-256") {
            return Err(ClientError::Auth(
                "server did not offer SCRAM-SHA-256".to_owned(),
            ));
        }

        let mut scram = ScramSha256::new(config.password.as_bytes(), ChannelBinding::unrequested());
        let mut buf = BytesMut::new();
        frontend::sasl_initial_response("SCRAM-SHA-256", scram.message(), &mut buf)
            .map_err(|e| ClientError::Malformed(e.to_string()))?;
        self.stream.write_all(&buf).await?;

        // AuthenticationSASLContinue.
        let (tag, mut body) = self.read_control().await?;
        if tag == b'E' {
            return Err(server_error(&body));
        }
        if tag != b'R' || read_i32(&mut body)? != 11 {
            return Err(ClientError::Auth("expected SASLContinue".to_owned()));
        }
        scram
            .update(&body)
            .map_err(|e| ClientError::Auth(e.to_string()))?;
        let mut buf = BytesMut::new();
        frontend::sasl_response(scram.message(), &mut buf)
            .map_err(|e| ClientError::Malformed(e.to_string()))?;
        self.stream.write_all(&buf).await?;

        // AuthenticationSASLFinal.
        let (tag, mut body) = self.read_control().await?;
        if tag == b'E' {
            return Err(server_error(&body));
        }
        if tag != b'R' || read_i32(&mut body)? != 12 {
            return Err(ClientError::Auth("expected SASLFinal".to_owned()));
        }
        scram
            .finish(&body)
            .map_err(|e| ClientError::Auth(e.to_string()))?;

        // AuthenticationOk.
        let (tag, mut body) = self.read_control().await?;
        if tag == b'E' {
            return Err(server_error(&body));
        }
        if tag != b'R' || read_i32(&mut body)? != 0 {
            return Err(ClientError::Auth("expected AuthenticationOk".to_owned()));
        }
        Ok(())
    }

    /// Create a logical replication slot with the `pgoutput` plugin. `temporary`
    /// slots are dropped when the connection ends (used by tests and one-shot
    /// consumers). Runs to `ReadyForQuery`; the slot's contents are consumed by a
    /// later [`Self::start_replication`].
    pub async fn create_logical_slot(&mut self, name: &str, temporary: bool) -> Result<()> {
        if !is_safe_ident(name) {
            return Err(ClientError::Malformed(format!(
                "unsafe replication slot name {name:?}"
            )));
        }
        let temp = if temporary { "TEMPORARY " } else { "" };
        let sql =
            format!("CREATE_REPLICATION_SLOT {name} {temp}LOGICAL pgoutput NOEXPORT_SNAPSHOT");
        self.send_query(&sql).await?;
        // RowDescription / DataRow / CommandComplete, ending at ReadyForQuery.
        loop {
            let (tag, body) = self.read_control().await?;
            match tag {
                b'Z' => return Ok(()),
                b'T' | b'D' | b'C' | b'S' => continue,
                b'E' => return Err(server_error(&body)),
                other => return Err(ClientError::Unexpected(other)),
            }
        }
    }

    /// Begin streaming `slot` for `publication` at protocol v4. After this the
    /// connection is in `CopyBoth` mode; use [`Self::next`].
    pub async fn start_replication(&mut self, slot: &str, publication: &str) -> Result<()> {
        if !is_safe_ident(slot) || !is_safe_ident(publication) {
            return Err(ClientError::Malformed(format!(
                "unsafe slot {slot:?} or publication {publication:?} name"
            )));
        }
        let sql = format!(
            "START_REPLICATION SLOT {slot} LOGICAL 0/0 \
             (proto_version '4', publication_names '\"{publication}\"')"
        );
        self.send_query(&sql).await?;
        loop {
            let (tag, body) = self.read_control().await?;
            match tag {
                b'W' => return Ok(()), // CopyBothResponse: streaming has begun
                b'S' => continue,
                b'E' => return Err(server_error(&body)),
                other => return Err(ClientError::Unexpected(other)),
            }
        }
    }

    /// The next `XLogData` from the stream, or `None` when the server ends the
    /// copy. Primary keepalives are answered internally (a standby status update
    /// when the server asks for a reply) rather than surfaced.
    ///
    /// This awaits the next record with no deadline — an idle-but-live server
    /// still sends periodic keepalives, but a silent connection blocks here, so a
    /// caller that needs bounded waiting should wrap this in a timeout.
    pub async fn next(&mut self) -> Result<Option<XLogData>> {
        loop {
            let (tag, body) = self.read_message().await?;
            match tag {
                b'd' => match stream::decode(&body)? {
                    ReplicationMessage::XLogData(x) => {
                        if x.wal_end > self.last_wal_end {
                            self.last_wal_end = x.wal_end;
                        }
                        return Ok(Some(XLogData {
                            wal_start: x.wal_start,
                            wal_end: x.wal_end,
                            server_time: x.server_time,
                            data: Bytes::copy_from_slice(x.data),
                        }));
                    }
                    ReplicationMessage::PrimaryKeepalive(k) => {
                        if k.wal_end > self.last_wal_end {
                            self.last_wal_end = k.wal_end;
                        }
                        if k.reply_requested {
                            self.send_standby_status().await?;
                        }
                    }
                },
                b'c' => return Ok(None), // CopyDone
                b'E' => return Err(server_error(&body)),
                b'N' | b'S' => continue,
                other => return Err(ClientError::Unexpected(other)),
            }
        }
    }

    /// Record the consumer's durable position. Reported as the flushed/applied
    /// LSN in the next standby status, so the server advances the slot (and frees
    /// WAL) only up to what the consumer has committed — the invariant that makes
    /// a restart replay exactly the un-applied tail. Never let this run ahead of a
    /// durable checkpoint.
    pub fn confirm(&mut self, lsn: Lsn) {
        if lsn > self.confirmed_lsn {
            self.confirmed_lsn = lsn;
        }
    }

    /// Send a standby status update: the WAL received as the write position, and
    /// the consumer's confirmed position (see [`Self::confirm`]) as the flush and
    /// apply positions the server advances the slot to.
    pub async fn send_standby_status(&mut self) -> Result<()> {
        let status = StandbyStatusUpdate {
            write_lsn: self.last_wal_end,
            flush_lsn: self.confirmed_lsn,
            apply_lsn: self.confirmed_lsn,
            client_time: 0,
            reply_requested: false,
        };
        self.send_copy_data(&status.encode()).await
    }

    async fn send_query(&mut self, sql: &str) -> Result<()> {
        let mut buf = BytesMut::new();
        frontend::query(sql, &mut buf).map_err(|e| ClientError::Malformed(e.to_string()))?;
        self.stream.write_all(&buf).await?;
        Ok(())
    }

    async fn send_copy_data(&mut self, payload: &[u8]) -> Result<()> {
        let len = i32::try_from(4 + payload.len())
            .map_err(|_| ClientError::Malformed("copy-data payload too large".to_owned()))?;
        let mut buf = BytesMut::with_capacity(5 + payload.len());
        buf.put_u8(b'd');
        buf.put_i32(len);
        buf.put_slice(payload);
        self.stream.write_all(&buf).await?;
        Ok(())
    }

    /// Read one control-path message: a handshake/command reply. Bounded by
    /// [`CONTROL_TIMEOUT`] so a silent or hostile peer fails fast, and skips an
    /// asynchronous `NoticeResponse` a server may interleave.
    async fn read_control(&mut self) -> Result<(u8, BytesMut)> {
        loop {
            let msg = tokio::time::timeout(CONTROL_TIMEOUT, self.read_message())
                .await
                .map_err(|_| {
                    ClientError::Malformed("timed out waiting for a server reply".to_owned())
                })??;
            if msg.0 == b'N' {
                continue; // NoticeResponse: informational, not part of the exchange
            }
            return Ok(msg);
        }
    }

    /// Read one backend message, returning its type byte and body (the bytes
    /// after the 5-byte tag+length header).
    async fn read_message(&mut self) -> Result<(u8, BytesMut)> {
        // A message is 1 tag byte + Int32 length (which covers itself + body).
        while self.read_buf.len() < 5 {
            self.fill().await?;
        }
        let len = i32::from_be_bytes([
            self.read_buf[1],
            self.read_buf[2],
            self.read_buf[3],
            self.read_buf[4],
        ]);
        let len = usize::try_from(len)
            .ok()
            .filter(|&l| (4..=MAX_MESSAGE_LEN).contains(&l))
            .ok_or_else(|| ClientError::Malformed(format!("bad message length {len}")))?;
        let total = 1 + len;
        while self.read_buf.len() < total {
            self.fill().await?;
        }
        let mut msg = self.read_buf.split_to(total);
        let tag = msg[0];
        msg.advance(5);
        Ok((tag, msg))
    }

    async fn fill(&mut self) -> Result<()> {
        let mut chunk = [0u8; 16 * 1024];
        let n = self.stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(ClientError::Closed);
        }
        self.read_buf.extend_from_slice(&chunk[..n]);
        Ok(())
    }
}

/// Read a big-endian `i32` from the front of a message body.
fn read_i32(body: &mut BytesMut) -> Result<i32> {
    if body.len() < 4 {
        return Err(ClientError::Malformed("truncated int32".to_owned()));
    }
    Ok(body.get_i32())
}

/// The NUL-terminated mechanism names in an `AuthenticationSASL` body.
fn mechanisms(body: &[u8]) -> Vec<String> {
    body.split(|&b| b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect()
}

/// Extract the human-readable message from an `ErrorResponse` body (a set of
/// NUL-terminated `field-code + value` entries; `M` is the primary message).
fn server_error(body: &[u8]) -> ClientError {
    for field in body.split(|&b| b == 0) {
        if let Some((&b'M', rest)) = field.split_first() {
            return ClientError::Server(String::from_utf8_lossy(rest).into_owned());
        }
    }
    ClientError::Server("unknown server error".to_owned())
}
