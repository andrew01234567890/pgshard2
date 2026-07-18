//! The streaming-replication CopyData wrapper.
//!
//! During `START_REPLICATION`, the server and client exchange `CopyData` frames
//! whose payloads are the small messages defined here — *not* pgoutput itself.
//! The server sends [`XLogData`] (a chunk of the WAL stream, whose bytes are the
//! pgoutput message) and [`PrimaryKeepalive`]; the client replies with
//! [`StandbyStatusUpdate`] (the LSN feedback that lets the server advance and
//! release WAL) and optionally [`HotStandbyFeedback`].
//!
//! This layer only splits the wrapper: an [`XLogData`]'s `data` is handed to
//! [`crate::pgoutput::PgOutputDecoder`] separately. Decoding is bounds-checked
//! and never panics; encoding produces the exact bytes to place in a `CopyData`.

use pgshard_core::Lsn;

use crate::pgoutput::{DecodeError, Reader};

/// Microseconds since `2000-01-01 00:00:00 UTC` (PostgreSQL `TimestampTz`).
pub type MicrosSince2000 = i64;

/// A server-to-client streaming-replication message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationMessage<'a> {
    XLogData(XLogData<'a>),
    PrimaryKeepalive(PrimaryKeepalive),
}

/// A chunk of the WAL stream. `data` is the pgoutput payload for logical
/// replication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XLogData<'a> {
    /// The WAL location of the start of `data`.
    pub wal_start: Lsn,
    /// The server's current end of WAL.
    pub wal_end: Lsn,
    pub server_time: MicrosSince2000,
    pub data: &'a [u8],
}

/// A liveness ping; `reply_requested` asks the client to send a status update
/// promptly so the server does not time the connection out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimaryKeepalive {
    pub wal_end: Lsn,
    pub server_time: MicrosSince2000,
    pub reply_requested: bool,
}

/// Decode one server-to-client message from a `CopyData` payload.
pub fn decode(bytes: &[u8]) -> Result<ReplicationMessage<'_>, DecodeError> {
    let mut r = Reader::new(bytes);
    let msg = match r.u8()? {
        b'w' => ReplicationMessage::XLogData(XLogData {
            wal_start: r.lsn()?,
            wal_end: r.lsn()?,
            server_time: r.i64()?,
            // The rest of the frame is the WAL/pgoutput payload.
            data: r.rest(),
        }),
        b'k' => ReplicationMessage::PrimaryKeepalive(PrimaryKeepalive {
            wal_end: r.lsn()?,
            server_time: r.i64()?,
            reply_requested: r.u8()? != 0,
        }),
        other => return Err(DecodeError::UnknownMessage(other)),
    };
    // XLogData consumes the rest; a keepalive with extra bytes is malformed.
    if r.remaining() != 0 {
        return Err(DecodeError::TrailingBytes(r.remaining()));
    }
    Ok(msg)
}

/// The client's acknowledgement of how far it has received, flushed, and applied
/// the WAL. The server advances its slot — and releases retained WAL — from the
/// flush LSN, so a consumer must not report a flush position past what it has
/// durably checkpointed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandbyStatusUpdate {
    pub write_lsn: Lsn,
    pub flush_lsn: Lsn,
    pub apply_lsn: Lsn,
    pub client_time: MicrosSince2000,
    /// Ask the server to reply immediately (a ping).
    pub reply_requested: bool,
}

impl StandbyStatusUpdate {
    /// The bytes to place in a `CopyData` frame.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(34);
        buf.push(b'r');
        buf.extend_from_slice(&self.write_lsn.0.to_be_bytes());
        buf.extend_from_slice(&self.flush_lsn.0.to_be_bytes());
        buf.extend_from_slice(&self.apply_lsn.0.to_be_bytes());
        buf.extend_from_slice(&self.client_time.to_be_bytes());
        buf.push(u8::from(self.reply_requested));
        buf
    }
}

/// The client's `hot_standby_feedback`: the xmin the server must not vacuum past
/// while this consumer is connected, preventing recently-needed rows from being
/// removed out from under a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HotStandbyFeedback {
    pub client_time: MicrosSince2000,
    pub global_xmin: u32,
    pub global_xmin_epoch: u32,
    pub catalog_xmin: u32,
    pub catalog_xmin_epoch: u32,
}

impl HotStandbyFeedback {
    /// The bytes to place in a `CopyData` frame.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(25);
        buf.push(b'h');
        buf.extend_from_slice(&self.client_time.to_be_bytes());
        buf.extend_from_slice(&self.global_xmin.to_be_bytes());
        buf.extend_from_slice(&self.global_xmin_epoch.to_be_bytes());
        buf.extend_from_slice(&self.catalog_xmin.to_be_bytes());
        buf.extend_from_slice(&self.catalog_xmin_epoch.to_be_bytes());
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn be64(v: u64) -> [u8; 8] {
        v.to_be_bytes()
    }

    #[test]
    fn decodes_xlogdata_and_exposes_the_payload() {
        let mut frame = vec![b'w'];
        frame.extend_from_slice(&be64(0x1000)); // wal_start
        frame.extend_from_slice(&be64(0x2000)); // wal_end
        frame.extend_from_slice(&725_000_000i64.to_be_bytes()); // server_time
        frame.extend_from_slice(b"pgoutput-bytes"); // payload
        let ReplicationMessage::XLogData(x) = decode(&frame).unwrap() else {
            panic!("expected XLogData");
        };
        assert_eq!(x.wal_start, Lsn(0x1000));
        assert_eq!(x.wal_end, Lsn(0x2000));
        assert_eq!(x.server_time, 725_000_000);
        assert_eq!(x.data, b"pgoutput-bytes");
    }

    #[test]
    fn decodes_an_empty_payload_xlogdata() {
        let mut frame = vec![b'w'];
        frame.extend_from_slice(&be64(0x1000));
        frame.extend_from_slice(&be64(0x1000));
        frame.extend_from_slice(&0i64.to_be_bytes());
        let ReplicationMessage::XLogData(x) = decode(&frame).unwrap() else {
            panic!("expected XLogData");
        };
        assert_eq!(x.data, b"");
    }

    #[test]
    fn decodes_primary_keepalive() {
        let mut frame = vec![b'k'];
        frame.extend_from_slice(&be64(0x2000));
        frame.extend_from_slice(&725i64.to_be_bytes());
        frame.push(1);
        assert_eq!(
            decode(&frame).unwrap(),
            ReplicationMessage::PrimaryKeepalive(PrimaryKeepalive {
                wal_end: Lsn(0x2000),
                server_time: 725,
                reply_requested: true,
            })
        );
    }

    #[test]
    fn rejects_unknown_type_truncation_and_trailing_bytes() {
        assert_eq!(decode(b"z").unwrap_err(), DecodeError::UnknownMessage(b'z'));
        assert!(matches!(
            decode(b"w\x00\x00").unwrap_err(),
            DecodeError::Truncated { .. }
        ));
        // A keepalive with an extra trailing byte is malformed.
        let mut frame = vec![b'k'];
        frame.extend_from_slice(&be64(1));
        frame.extend_from_slice(&1i64.to_be_bytes());
        frame.push(0);
        frame.push(0xFF);
        assert_eq!(decode(&frame).unwrap_err(), DecodeError::TrailingBytes(1));
    }

    #[test]
    fn standby_status_update_encodes_the_exact_wire_bytes() {
        let ssu = StandbyStatusUpdate {
            write_lsn: Lsn(0x11),
            flush_lsn: Lsn(0x22),
            apply_lsn: Lsn(0x33),
            client_time: 725,
            reply_requested: true,
        };
        let mut expected = vec![b'r'];
        expected.extend_from_slice(&be64(0x11));
        expected.extend_from_slice(&be64(0x22));
        expected.extend_from_slice(&be64(0x33));
        expected.extend_from_slice(&725i64.to_be_bytes());
        expected.push(1);
        assert_eq!(ssu.encode(), expected);
        assert_eq!(ssu.encode().len(), 34);
    }

    #[test]
    fn hot_standby_feedback_encodes_the_exact_wire_bytes() {
        let hsf = HotStandbyFeedback {
            client_time: 725,
            global_xmin: 100,
            global_xmin_epoch: 1,
            catalog_xmin: 90,
            catalog_xmin_epoch: 1,
        };
        let mut expected = vec![b'h'];
        expected.extend_from_slice(&725i64.to_be_bytes());
        expected.extend_from_slice(&100u32.to_be_bytes());
        expected.extend_from_slice(&1u32.to_be_bytes());
        expected.extend_from_slice(&90u32.to_be_bytes());
        expected.extend_from_slice(&1u32.to_be_bytes());
        assert_eq!(hsf.encode(), expected);
        assert_eq!(hsf.encode().len(), 25);
    }
}
