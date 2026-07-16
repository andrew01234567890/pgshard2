use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::frame::{Frame, FrameError, read_frame, write_frame};
use crate::startup::encode_startup;

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error("backend error during startup: {0}")]
    Startup(String),
    #[error("unsupported authentication request code {0}")]
    UnsupportedAuth(i32),
    #[error("sasl: {0}")]
    Sasl(std::io::Error),
    #[error("unexpected message {0:?} during startup")]
    Unexpected(u8),
}

/// An authenticated backend connection that has reached ReadyForQuery.
pub struct BackendConn {
    pub stream: TcpStream,
    /// ParameterStatus and BackendKeyData frames captured during startup,
    /// in arrival order — the frontend replays these to the client.
    pub startup_frames: Vec<Frame>,
    /// The final ReadyForQuery frame.
    pub ready: Frame,
}

/// Connects and authenticates (SCRAM-SHA-256 or cleartext) as `user` to
/// `database`, driving the startup phase to ReadyForQuery.
pub async fn connect(
    host: &str,
    port: u16,
    user: &str,
    database: &str,
    password: &str,
) -> Result<BackendConn, BackendError> {
    let mut stream = TcpStream::connect((host, port)).await?;
    stream.set_nodelay(true)?;
    let startup = encode_startup(&[("user", user), ("database", database)]);
    stream.write_all(&startup).await?;

    let mut startup_frames = Vec::new();
    let mut scram: Option<ScramSha256> = None;
    loop {
        let frame = read_frame(&mut stream).await?;
        match frame.tag {
            b'R' => {
                let code = auth_code(&frame)?;
                match code {
                    0 => {} // AuthenticationOk
                    3 => {
                        // Cleartext password.
                        let mut body = password.as_bytes().to_vec();
                        body.push(0);
                        write_frame(&mut stream, b'p', &body).await?;
                    }
                    10 => {
                        // SASL: pick SCRAM-SHA-256 (no channel binding on
                        // plain TCP).
                        let s =
                            ScramSha256::new(password.as_bytes(), ChannelBinding::unsupported());
                        let first = s.message().to_vec();
                        let mut body = b"SCRAM-SHA-256".to_vec();
                        body.push(0);
                        body.extend_from_slice(&(first.len() as i32).to_be_bytes());
                        body.extend_from_slice(&first);
                        write_frame(&mut stream, b'p', &body).await?;
                        scram = Some(s);
                    }
                    11 => {
                        // SASLContinue.
                        let s = scram.as_mut().ok_or(BackendError::UnsupportedAuth(11))?;
                        s.update(&frame.body[4..]).map_err(BackendError::Sasl)?;
                        write_frame(&mut stream, b'p', s.message()).await?;
                    }
                    12 => {
                        // SASLFinal.
                        let s = scram.as_mut().ok_or(BackendError::UnsupportedAuth(12))?;
                        s.finish(&frame.body[4..]).map_err(BackendError::Sasl)?;
                    }
                    other => return Err(BackendError::UnsupportedAuth(other)),
                }
            }
            b'S' | b'K' => startup_frames.push(frame),
            b'Z' => {
                return Ok(BackendConn {
                    stream,
                    startup_frames,
                    ready: frame,
                });
            }
            b'E' => return Err(BackendError::Startup(error_message(&frame))),
            b'N' => {} // NoticeResponse: ignore during startup
            other => return Err(BackendError::Unexpected(other)),
        }
    }
}

fn auth_code(frame: &Frame) -> Result<i32, BackendError> {
    frame
        .body
        .get(0..4)
        .map(|b| i32::from_be_bytes(b.try_into().expect("length checked")))
        .ok_or(BackendError::UnsupportedAuth(-1))
}

/// Best-effort human-readable rendering of an ErrorResponse.
fn error_message(frame: &Frame) -> String {
    let mut out = String::new();
    let mut rest = &frame.body[..];
    while let Some((&code, tail)) = rest.split_first() {
        if code == 0 {
            break;
        }
        let Some(nul) = tail.iter().position(|&b| b == 0) else {
            break;
        };
        if code == b'S' || code == b'M' || code == b'C' {
            if !out.is_empty() {
                out.push_str(": ");
            }
            out.push_str(&String::from_utf8_lossy(&tail[..nul]));
        }
        rest = &tail[nul + 1..];
    }
    out
}
