use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Upper bound on a single wire message; larger frames are treated as a
/// protocol violation rather than an allocation request.
pub const MAX_FRAME_LEN: usize = 1 << 30;

/// A typed protocol-3 message: one tag byte, then i32 length (including
/// itself), then the body.
#[derive(Debug, Clone)]
pub struct Frame {
    pub tag: u8,
    pub body: Bytes,
}

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame length {0} out of bounds")]
    BadLength(i64),
}

pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Frame, FrameError> {
    let tag = r.read_u8().await?;
    let len = r.read_i32().await?;
    let body_len = i64::from(len) - 4;
    if !(0..=MAX_FRAME_LEN as i64).contains(&body_len) {
        return Err(FrameError::BadLength(i64::from(len)));
    }
    let mut body = BytesMut::zeroed(body_len as usize);
    r.read_exact(&mut body).await?;
    Ok(Frame {
        tag,
        body: body.freeze(),
    })
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    tag: u8,
    body: &[u8],
) -> std::io::Result<()> {
    let mut header = [0u8; 5];
    header[0] = tag;
    header[1..].copy_from_slice(&(body.len() as i32 + 4).to_be_bytes());
    w.write_all(&header).await?;
    w.write_all(body).await?;
    Ok(())
}
