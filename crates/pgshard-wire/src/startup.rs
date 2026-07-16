use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::frame::{FrameError, MAX_FRAME_LEN};

pub const PROTOCOL_3_0: i32 = 196608;
const SSL_REQUEST: i32 = 80877103;
const GSSENC_REQUEST: i32 = 80877104;
const CANCEL_REQUEST: i32 = 80877102;

/// The first, untagged packet a client sends.
#[derive(Debug)]
pub enum Initial {
    SslRequest,
    GssEncRequest,
    CancelRequest { process_id: i32, secret_key: i32 },
    Startup(StartupParams),
}

#[derive(Debug, Default)]
pub struct StartupParams {
    pub protocol: i32,
    pub params: Vec<(String, String)>,
}

impl StartupParams {
    pub fn get(&self, key: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StartupError {
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("malformed startup packet")]
    Malformed,
    #[error("unsupported protocol version {0}")]
    UnsupportedProtocol(i32),
}

pub async fn read_initial<R: AsyncRead + Unpin>(r: &mut R) -> Result<Initial, StartupError> {
    let len = r.read_i32().await?;
    let body_len = i64::from(len) - 4;
    if !(4..=MAX_FRAME_LEN as i64).contains(&body_len) {
        return Err(FrameError::BadLength(i64::from(len)).into());
    }
    let mut body = vec![0u8; body_len as usize];
    r.read_exact(&mut body).await?;
    let code = i32::from_be_bytes(body[0..4].try_into().expect("length checked"));
    match code {
        SSL_REQUEST => Ok(Initial::SslRequest),
        GSSENC_REQUEST => Ok(Initial::GssEncRequest),
        CANCEL_REQUEST => {
            if body.len() != 12 {
                return Err(StartupError::Malformed);
            }
            Ok(Initial::CancelRequest {
                process_id: i32::from_be_bytes(body[4..8].try_into().expect("length checked")),
                secret_key: i32::from_be_bytes(body[8..12].try_into().expect("length checked")),
            })
        }
        PROTOCOL_3_0 => {
            let mut params = Vec::new();
            let mut rest = &body[4..];
            loop {
                match rest.first() {
                    None => return Err(StartupError::Malformed),
                    Some(0) => break,
                    Some(_) => {}
                }
                let key = take_cstr(&mut rest).ok_or(StartupError::Malformed)?;
                let value = take_cstr(&mut rest).ok_or(StartupError::Malformed)?;
                params.push((key, value));
            }
            Ok(Initial::Startup(StartupParams {
                protocol: code,
                params,
            }))
        }
        other => Err(StartupError::UnsupportedProtocol(other)),
    }
}

fn take_cstr(rest: &mut &[u8]) -> Option<String> {
    let nul = rest.iter().position(|&b| b == 0)?;
    let s = String::from_utf8(rest[..nul].to_vec()).ok()?;
    *rest = &rest[nul + 1..];
    Some(s)
}

/// Encodes a protocol-3.0 startup packet.
pub fn encode_startup(params: &[(&str, &str)]) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(0); // length placeholder
    body.put_i32(PROTOCOL_3_0);
    for (k, v) in params {
        body.put_slice(k.as_bytes());
        body.put_u8(0);
        body.put_slice(v.as_bytes());
        body.put_u8(0);
    }
    body.put_u8(0);
    let len = body.len() as i32;
    body[0..4].copy_from_slice(&len.to_be_bytes());
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn startup_round_trips() {
        let encoded = encode_startup(&[("user", "app"), ("database", "orders")]);
        let mut cursor = std::io::Cursor::new(encoded.to_vec());
        let Initial::Startup(params) = read_initial(&mut cursor).await.unwrap() else {
            panic!("expected startup");
        };
        assert_eq!(params.protocol, PROTOCOL_3_0);
        assert_eq!(params.get("user"), Some("app"));
        assert_eq!(params.get("database"), Some("orders"));
        assert_eq!(params.get("missing"), None);
    }

    #[tokio::test]
    async fn rejects_garbage() {
        let mut cursor = std::io::Cursor::new(vec![0, 0, 0, 3]);
        assert!(read_initial(&mut cursor).await.is_err());
    }
}
