//! Decoder for PostgreSQL's `pgoutput` logical-replication message format.
//!
//! A logical-replication walsender wraps each of these messages in a `CopyData`
//! frame during `START_REPLICATION`. [`PgOutputDecoder::decode`] turns one such
//! frame body into a [`LogicalRepMsg`]. Decoding is zero-copy — string and value
//! fields borrow the input frame — because every replicated row flows through
//! here on the reshard/CDC hot path.
//!
//! The decoder is stateful in exactly one respect: protocol v2+ prefixes the
//! per-change messages of a *streamed* (in-progress) transaction with their xid,
//! and that prefix is present only between a [`StreamStart`] and a Stream Stop.
//! The decoder tracks that bracket so it reads the prefix exactly when the wire
//! carries it.
//!
//! Every decode is fallible and never panics: a truncated or malformed frame
//! returns a [`DecodeError`] rather than indexing out of bounds.

use pgshard_core::Lsn;
use thiserror::Error;

/// A PostgreSQL transaction id.
pub type Xid = u32;
/// A PostgreSQL object id.
pub type Oid = u32;
/// Microseconds since `2000-01-01 00:00:00 UTC` (PostgreSQL `TimestampTz`).
pub type TimestampTz = i64;

/// The highest `pgoutput` protocol version this decoder understands.
pub const MAX_SUPPORTED_PROTOCOL_VERSION: u32 = 4;

/// Why a `pgoutput` frame could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DecodeError {
    #[error("truncated pgoutput message: need {needed} more byte(s) at offset {offset}")]
    Truncated { offset: usize, needed: usize },
    #[error("unknown pgoutput message type byte {0:#04x}")]
    UnknownMessage(u8),
    #[error("unknown tuple column kind byte {0:#04x}")]
    UnknownTupleKind(u8),
    #[error("expected a tuple marker (K/O/N) but found byte {0:#04x}")]
    UnexpectedTupleMarker(u8),
    #[error("invalid negative field length {0}")]
    NegativeLength(i32),
    #[error("string field is not valid UTF-8")]
    InvalidUtf8,
    #[error("{0} unexpected trailing byte(s) after the pgoutput message")]
    TrailingBytes(usize),
}

/// One decoded `pgoutput` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalRepMsg<'a> {
    Begin(Begin),
    Message(Message<'a>),
    Commit(Commit),
    Origin(Origin<'a>),
    Relation(Relation<'a>),
    Type(TypeDecl<'a>),
    Insert(Insert<'a>),
    Update(Update<'a>),
    Delete(Delete<'a>),
    Truncate(Truncate),
    StreamStart(StreamStart),
    StreamStop,
    StreamCommit(StreamCommit),
    StreamAbort(StreamAbort),
    BeginPrepare(PreparedTxn<'a>),
    Prepare(PreparedTxn<'a>),
    CommitPrepared(PreparedTxn<'a>),
    StreamPrepare(PreparedTxn<'a>),
    RollbackPrepared(RollbackPrepared<'a>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Begin {
    pub final_lsn: Lsn,
    pub commit_timestamp: TimestampTz,
    pub xid: Xid,
}

/// A logical decoding message (`pg_logical_emit_message`). pgshard emits these as
/// reshard-journal and barrier markers, so decoding them is load-bearing for CDC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message<'a> {
    /// Present only for a change inside a streamed transaction (protocol v2+).
    pub xid: Option<Xid>,
    pub transactional: bool,
    pub lsn: Lsn,
    pub prefix: &'a str,
    pub content: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub commit_lsn: Lsn,
    pub end_lsn: Lsn,
    pub commit_timestamp: TimestampTz,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Origin<'a> {
    pub commit_lsn: Lsn,
    pub name: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation<'a> {
    pub xid: Option<Xid>,
    pub oid: Oid,
    pub namespace: &'a str,
    pub name: &'a str,
    /// `relreplident` from `pg_class` (`d`/`n`/`f`/`i`).
    pub replica_identity: u8,
    pub columns: Vec<RelationColumn<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationColumn<'a> {
    pub flags: u8,
    pub name: &'a str,
    pub type_oid: Oid,
    pub type_modifier: i32,
}

impl RelationColumn<'_> {
    /// Whether the column is part of the replica-identity key.
    pub fn is_key(&self) -> bool {
        self.flags & 1 != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeDecl<'a> {
    pub xid: Option<Xid>,
    pub oid: Oid,
    pub namespace: &'a str,
    pub name: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Insert<'a> {
    pub xid: Option<Xid>,
    pub rel_oid: Oid,
    pub new_tuple: TupleData<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Update<'a> {
    pub xid: Option<Xid>,
    pub rel_oid: Oid,
    /// The old row's replica-identity key columns (`REPLICA IDENTITY` index or
    /// changed key), sent as a `K` sub-message. Mutually exclusive with `old`.
    pub key: Option<TupleData<'a>>,
    /// The full old row, sent as an `O` sub-message under `REPLICA IDENTITY FULL`.
    pub old: Option<TupleData<'a>>,
    pub new_tuple: TupleData<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delete<'a> {
    pub xid: Option<Xid>,
    pub rel_oid: Oid,
    /// Replica-identity key of the deleted row (`K`). Mutually exclusive with `old`.
    pub key: Option<TupleData<'a>>,
    /// Full deleted row (`O`, under `REPLICA IDENTITY FULL`).
    pub old: Option<TupleData<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Truncate {
    pub xid: Option<Xid>,
    pub options: u8,
    pub relations: Vec<Oid>,
}

impl Truncate {
    pub fn cascade(&self) -> bool {
        self.options & 1 != 0
    }

    pub fn restart_identity(&self) -> bool {
        self.options & 2 != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamStart {
    pub xid: Xid,
    pub first_segment: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamCommit {
    pub xid: Xid,
    pub commit_lsn: Lsn,
    pub end_lsn: Lsn,
    pub commit_timestamp: TimestampTz,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamAbort {
    pub xid: Xid,
    pub subxid: Xid,
    /// Present only under protocol v4 with parallel streaming.
    pub abort_lsn: Option<Lsn>,
    /// Present only under protocol v4 with parallel streaming.
    pub abort_timestamp: Option<TimestampTz>,
}

/// The shared shape of Begin Prepare / Prepare / Commit Prepared / Stream Prepare.
/// The leading `flags` byte (unused, always 0) is consumed and dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedTxn<'a> {
    pub lsn: Lsn,
    pub end_lsn: Lsn,
    pub timestamp: TimestampTz,
    pub xid: Xid,
    pub gid: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackPrepared<'a> {
    pub prepared_end_lsn: Lsn,
    pub rollback_end_lsn: Lsn,
    pub prepare_timestamp: TimestampTz,
    pub rollback_timestamp: TimestampTz,
    pub xid: Xid,
    pub gid: &'a str,
}

/// A row image: one entry per published column, in table column order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleData<'a> {
    pub columns: Vec<TupleColumn<'a>>,
}

/// One column of a [`TupleData`]. Text and binary values borrow the input frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleColumn<'a> {
    /// SQL `NULL` (`n`).
    Null,
    /// An unchanged TOASTed value not shipped in this row (`u`); the value must be
    /// carried over from the row's prior state.
    UnchangedToast,
    /// A text-format value (`t`).
    Text(&'a [u8]),
    /// A binary-format value (`b`).
    Binary(&'a [u8]),
}

/// The `streaming` option negotiated at `START_REPLICATION`. It determines
/// whether a Stream Abort carries the protocol-v4 abort LSN and timestamp, which
/// are sent only under parallel streaming.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamMode {
    /// In-progress transactions are not streamed (`streaming off`).
    Off,
    /// In-progress transactions are streamed (`streaming on`, protocol v2+).
    On,
    /// Streamed and applied in parallel (`streaming parallel`, protocol v4+);
    /// Stream Abort additionally carries the abort LSN and timestamp.
    Parallel,
}

/// Decodes a stream of `pgoutput` frames, tracking the streamed-transaction
/// bracket so per-change xid prefixes are read exactly when present.
#[derive(Debug, Clone)]
pub struct PgOutputDecoder {
    protocol_version: u32,
    stream_mode: StreamMode,
    in_stream: bool,
}

impl PgOutputDecoder {
    /// A decoder for the given negotiated `pgoutput` protocol version, without
    /// streaming of in-progress transactions.
    pub fn new(protocol_version: u32) -> Self {
        Self::with_stream_mode(protocol_version, StreamMode::Off)
    }

    /// A decoder for the given negotiated protocol version and streaming mode.
    pub fn with_stream_mode(protocol_version: u32, stream_mode: StreamMode) -> Self {
        Self {
            protocol_version,
            stream_mode,
            in_stream: false,
        }
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn stream_mode(&self) -> StreamMode {
        self.stream_mode
    }

    /// Whether the decoder is currently between a Stream Start and Stream Stop.
    pub fn in_stream(&self) -> bool {
        self.in_stream
    }

    /// Decode one `pgoutput` frame body.
    pub fn decode<'a>(&mut self, bytes: &'a [u8]) -> Result<LogicalRepMsg<'a>, DecodeError> {
        let mut r = Reader::new(bytes);
        let tag = r.u8()?;
        let in_stream = self.in_stream;
        // The stream-bracket transition is applied only once the whole frame is
        // accepted, so a decode error never leaves the decoder in a half-updated
        // state that would misread a following change's xid prefix.
        let mut next_in_stream = in_stream;
        let parallel = self.stream_mode == StreamMode::Parallel;
        let msg = match tag {
            b'B' => LogicalRepMsg::Begin(begin(&mut r)?),
            b'M' => LogicalRepMsg::Message(logical_message(&mut r, in_stream)?),
            b'C' => LogicalRepMsg::Commit(commit(&mut r)?),
            b'O' => LogicalRepMsg::Origin(origin(&mut r)?),
            b'R' => LogicalRepMsg::Relation(relation(&mut r, in_stream)?),
            b'Y' => LogicalRepMsg::Type(type_decl(&mut r, in_stream)?),
            b'I' => LogicalRepMsg::Insert(insert(&mut r, in_stream)?),
            b'U' => LogicalRepMsg::Update(update(&mut r, in_stream)?),
            b'D' => LogicalRepMsg::Delete(delete(&mut r, in_stream)?),
            b'T' => LogicalRepMsg::Truncate(truncate(&mut r, in_stream)?),
            b'S' => {
                let start = stream_start(&mut r)?;
                next_in_stream = true;
                LogicalRepMsg::StreamStart(start)
            }
            b'E' => {
                next_in_stream = false;
                LogicalRepMsg::StreamStop
            }
            b'c' => LogicalRepMsg::StreamCommit(stream_commit(&mut r)?),
            b'A' => LogicalRepMsg::StreamAbort(stream_abort(&mut r, parallel)?),
            b'b' => LogicalRepMsg::BeginPrepare(prepared(&mut r, false)?),
            b'P' => LogicalRepMsg::Prepare(prepared(&mut r, true)?),
            b'K' => LogicalRepMsg::CommitPrepared(prepared(&mut r, true)?),
            b'p' => LogicalRepMsg::StreamPrepare(prepared(&mut r, true)?),
            b'r' => LogicalRepMsg::RollbackPrepared(rollback_prepared(&mut r)?),
            other => return Err(DecodeError::UnknownMessage(other)),
        };
        if r.remaining() != 0 {
            return Err(DecodeError::TrailingBytes(r.remaining()));
        }
        self.in_stream = next_in_stream;
        Ok(msg)
    }
}

fn begin(r: &mut Reader) -> Result<Begin, DecodeError> {
    Ok(Begin {
        final_lsn: r.lsn()?,
        commit_timestamp: r.i64()?,
        xid: r.u32()?,
    })
}

fn logical_message<'a>(r: &mut Reader<'a>, in_stream: bool) -> Result<Message<'a>, DecodeError> {
    let xid = stream_xid(r, in_stream)?;
    let flags = r.u8()?;
    let lsn = r.lsn()?;
    let prefix = r.cstr()?;
    let content = read_sized(r)?;
    Ok(Message {
        xid,
        transactional: flags & 1 != 0,
        lsn,
        prefix,
        content,
    })
}

fn commit(r: &mut Reader) -> Result<Commit, DecodeError> {
    let _flags = r.u8()?;
    Ok(Commit {
        commit_lsn: r.lsn()?,
        end_lsn: r.lsn()?,
        commit_timestamp: r.i64()?,
    })
}

fn origin<'a>(r: &mut Reader<'a>) -> Result<Origin<'a>, DecodeError> {
    Ok(Origin {
        commit_lsn: r.lsn()?,
        name: r.cstr()?,
    })
}

fn relation<'a>(r: &mut Reader<'a>, in_stream: bool) -> Result<Relation<'a>, DecodeError> {
    let xid = stream_xid(r, in_stream)?;
    let oid = r.u32()?;
    let namespace = r.cstr()?;
    let name = r.cstr()?;
    let replica_identity = r.u8()?;
    let count = count(r.i16()? as i32)?;
    // Each column is at least flags(1) + name terminator(1) + oid(4) + typmod(4),
    // so `remaining / 10` bounds how many can exist — cap the pre-allocation there.
    let mut columns = Vec::with_capacity(count.min(r.remaining() / 10));
    for _ in 0..count {
        columns.push(RelationColumn {
            flags: r.u8()?,
            name: r.cstr()?,
            type_oid: r.u32()?,
            type_modifier: r.i32()?,
        });
    }
    Ok(Relation {
        xid,
        oid,
        namespace,
        name,
        replica_identity,
        columns,
    })
}

fn type_decl<'a>(r: &mut Reader<'a>, in_stream: bool) -> Result<TypeDecl<'a>, DecodeError> {
    Ok(TypeDecl {
        xid: stream_xid(r, in_stream)?,
        oid: r.u32()?,
        namespace: r.cstr()?,
        name: r.cstr()?,
    })
}

fn insert<'a>(r: &mut Reader<'a>, in_stream: bool) -> Result<Insert<'a>, DecodeError> {
    let xid = stream_xid(r, in_stream)?;
    let rel_oid = r.u32()?;
    expect_marker(r, b'N')?;
    Ok(Insert {
        xid,
        rel_oid,
        new_tuple: tuple_data(r)?,
    })
}

fn update<'a>(r: &mut Reader<'a>, in_stream: bool) -> Result<Update<'a>, DecodeError> {
    let xid = stream_xid(r, in_stream)?;
    let rel_oid = r.u32()?;
    // An update carries an optional old-row image (`K` key or `O` full) before the
    // mandatory new-row image (`N`).
    let (key, old, marker) = match r.u8()? {
        b'K' => (Some(tuple_data(r)?), None, r.u8()?),
        b'O' => (None, Some(tuple_data(r)?), r.u8()?),
        marker => (None, None, marker),
    };
    if marker != b'N' {
        return Err(DecodeError::UnexpectedTupleMarker(marker));
    }
    Ok(Update {
        xid,
        rel_oid,
        key,
        old,
        new_tuple: tuple_data(r)?,
    })
}

fn delete<'a>(r: &mut Reader<'a>, in_stream: bool) -> Result<Delete<'a>, DecodeError> {
    let xid = stream_xid(r, in_stream)?;
    let rel_oid = r.u32()?;
    let (key, old) = match r.u8()? {
        b'K' => (Some(tuple_data(r)?), None),
        b'O' => (None, Some(tuple_data(r)?)),
        marker => return Err(DecodeError::UnexpectedTupleMarker(marker)),
    };
    Ok(Delete {
        xid,
        rel_oid,
        key,
        old,
    })
}

fn truncate<'a>(r: &mut Reader<'a>, in_stream: bool) -> Result<Truncate, DecodeError> {
    let xid = stream_xid(r, in_stream)?;
    let count = count(r.i32()?)?;
    let options = r.u8()?;
    // Each relation is a 4-byte oid, so the frame cannot hold more than
    // `remaining / 4` of them — cap the pre-allocation to that, not the byte count.
    let mut relations = Vec::with_capacity(count.min(r.remaining() / 4));
    for _ in 0..count {
        relations.push(r.u32()?);
    }
    Ok(Truncate {
        xid,
        options,
        relations,
    })
}

fn stream_start(r: &mut Reader) -> Result<StreamStart, DecodeError> {
    Ok(StreamStart {
        xid: r.u32()?,
        first_segment: r.u8()? != 0,
    })
}

fn stream_commit(r: &mut Reader) -> Result<StreamCommit, DecodeError> {
    let xid = r.u32()?;
    let _flags = r.u8()?;
    Ok(StreamCommit {
        xid,
        commit_lsn: r.lsn()?,
        end_lsn: r.lsn()?,
        commit_timestamp: r.i64()?,
    })
}

fn stream_abort(r: &mut Reader, parallel: bool) -> Result<StreamAbort, DecodeError> {
    let xid = r.u32()?;
    let subxid = r.u32()?;
    // The abort LSN and timestamp are sent only under parallel streaming (which
    // implies protocol v4). Reading them is driven by the negotiated stream mode,
    // not by guessing from the remaining length; the strict trailing-bytes check
    // then rejects a frame that carries them unexpectedly, or omits them.
    let (abort_lsn, abort_timestamp) = if parallel {
        (Some(r.lsn()?), Some(r.i64()?))
    } else {
        (None, None)
    };
    Ok(StreamAbort {
        xid,
        subxid,
        abort_lsn,
        abort_timestamp,
    })
}

fn prepared<'a>(r: &mut Reader<'a>, has_flags: bool) -> Result<PreparedTxn<'a>, DecodeError> {
    if has_flags {
        let _flags = r.u8()?;
    }
    Ok(PreparedTxn {
        lsn: r.lsn()?,
        end_lsn: r.lsn()?,
        timestamp: r.i64()?,
        xid: r.u32()?,
        gid: r.cstr()?,
    })
}

fn rollback_prepared<'a>(r: &mut Reader<'a>) -> Result<RollbackPrepared<'a>, DecodeError> {
    let _flags = r.u8()?;
    Ok(RollbackPrepared {
        prepared_end_lsn: r.lsn()?,
        rollback_end_lsn: r.lsn()?,
        prepare_timestamp: r.i64()?,
        rollback_timestamp: r.i64()?,
        xid: r.u32()?,
        gid: r.cstr()?,
    })
}

fn tuple_data<'a>(r: &mut Reader<'a>) -> Result<TupleData<'a>, DecodeError> {
    let count = count(r.i16()? as i32)?;
    let mut columns = Vec::with_capacity(count.min(r.remaining()));
    for _ in 0..count {
        let column = match r.u8()? {
            b'n' => TupleColumn::Null,
            b'u' => TupleColumn::UnchangedToast,
            b't' => TupleColumn::Text(read_sized(r)?),
            b'b' => TupleColumn::Binary(read_sized(r)?),
            other => return Err(DecodeError::UnknownTupleKind(other)),
        };
        columns.push(column);
    }
    Ok(TupleData { columns })
}

/// Read a change's optional streamed-transaction xid prefix (protocol v2+).
fn stream_xid(r: &mut Reader, in_stream: bool) -> Result<Option<Xid>, DecodeError> {
    if in_stream {
        Ok(Some(r.u32()?))
    } else {
        Ok(None)
    }
}

/// Read an `Int32` length followed by that many value bytes.
fn read_sized<'a>(r: &mut Reader<'a>) -> Result<&'a [u8], DecodeError> {
    let len = count(r.i32()?)?;
    r.take(len)
}

fn expect_marker(r: &mut Reader, want: u8) -> Result<(), DecodeError> {
    match r.u8()? {
        got if got == want => Ok(()),
        got => Err(DecodeError::UnexpectedTupleMarker(got)),
    }
}

/// Convert a wire count/length to a `usize`, rejecting a negative value.
fn count(value: i32) -> Result<usize, DecodeError> {
    usize::try_from(value).map_err(|_| DecodeError::NegativeLength(value))
}

/// A bounds-checked cursor over a frame. Every read either advances within the
/// buffer or returns [`DecodeError::Truncated`]; it never panics.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&end| end <= self.buf.len())
            .ok_or(DecodeError::Truncated {
                offset: self.pos,
                needed: n,
            })?;
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn i16(&mut self) -> Result<i16, DecodeError> {
        let b = self.take(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn i32(&mut self) -> Result<i32, DecodeError> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn i64(&mut self) -> Result<i64, DecodeError> {
        let b = self.take(8)?;
        Ok(i64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn lsn(&mut self) -> Result<Lsn, DecodeError> {
        let b = self.take(8)?;
        Ok(Lsn(u64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ])))
    }

    /// Read a null-terminated string field, borrowing the frame.
    ///
    /// pgoutput string fields (schema/table/column/type names, origin name,
    /// message prefix, GID) are in the source server's encoding. This assumes
    /// that encoding is UTF-8 — which pgshard-provisioned clusters are — and
    /// fails closed (`InvalidUtf8`, halting the stream) rather than mis-decoding
    /// if it is not. Supporting a non-UTF-8 source (lossy or raw-bytes
    /// identifiers) is a follow-up. Tuple values and message content are never
    /// routed through here; they stay raw `&[u8]`.
    fn cstr(&mut self) -> Result<&'a str, DecodeError> {
        let rest = &self.buf[self.pos..];
        let nul = rest
            .iter()
            .position(|&b| b == 0)
            .ok_or(DecodeError::Truncated {
                offset: self.pos,
                needed: 1,
            })?;
        let text = std::str::from_utf8(&rest[..nul]).map_err(|_| DecodeError::InvalidUtf8)?;
        self.pos += nul + 1;
        Ok(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A little-endian-free big-endian frame builder for hand-written fixtures.
    #[derive(Default)]
    struct Frame(Vec<u8>);

    impl Frame {
        fn tag(tag: u8) -> Self {
            Frame(vec![tag])
        }
        fn u8(mut self, v: u8) -> Self {
            self.0.push(v);
            self
        }
        fn i16(mut self, v: i16) -> Self {
            self.0.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn u32(mut self, v: u32) -> Self {
            self.0.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn i32(mut self, v: i32) -> Self {
            self.0.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn i64(mut self, v: i64) -> Self {
            self.0.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn u64(mut self, v: u64) -> Self {
            self.0.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn cstr(mut self, s: &str) -> Self {
            self.0.extend_from_slice(s.as_bytes());
            self.0.push(0);
            self
        }
        fn raw(mut self, b: &[u8]) -> Self {
            self.0.extend_from_slice(b);
            self
        }
        fn build(self) -> Vec<u8> {
            self.0
        }
    }

    fn dec() -> PgOutputDecoder {
        PgOutputDecoder::new(MAX_SUPPORTED_PROTOCOL_VERSION)
    }

    #[test]
    fn decodes_begin_and_commit() {
        let begin = Frame::tag(b'B')
            .u64(0x16_B374_D848)
            .i64(725_000_000)
            .u32(910);
        assert_eq!(
            dec().decode(&begin.build()).unwrap(),
            LogicalRepMsg::Begin(Begin {
                final_lsn: Lsn(0x16_B374_D848),
                commit_timestamp: 725_000_000,
                xid: 910,
            })
        );
        let commit = Frame::tag(b'C')
            .u8(0)
            .u64(0x100)
            .u64(0x200)
            .i64(725_000_001);
        assert_eq!(
            dec().decode(&commit.build()).unwrap(),
            LogicalRepMsg::Commit(Commit {
                commit_lsn: Lsn(0x100),
                end_lsn: Lsn(0x200),
                commit_timestamp: 725_000_001,
            })
        );
    }

    #[test]
    fn decodes_insert_with_text_null_and_toast_columns() {
        // I, rel_oid=42, N, tuple{ "abc", NULL, unchanged-toast }
        let frame = Frame::tag(b'I')
            .u32(42)
            .u8(b'N')
            .i16(3)
            .u8(b't')
            .i32(3)
            .raw(b"abc")
            .u8(b'n')
            .u8(b'u')
            .build();
        let LogicalRepMsg::Insert(ins) = dec().decode(&frame).unwrap() else {
            panic!("expected insert");
        };
        assert_eq!(ins.xid, None);
        assert_eq!(ins.rel_oid, 42);
        assert_eq!(
            ins.new_tuple.columns,
            vec![
                TupleColumn::Text(b"abc"),
                TupleColumn::Null,
                TupleColumn::UnchangedToast,
            ]
        );
    }

    #[test]
    fn decodes_update_with_key_full_and_new_only() {
        let new_only = Frame::tag(b'U')
            .u32(7)
            .u8(b'N')
            .i16(1)
            .u8(b't')
            .i32(1)
            .raw(b"z")
            .build();
        let LogicalRepMsg::Update(u) = dec().decode(&new_only).unwrap() else {
            panic!("expected update");
        };
        assert_eq!((u.key.is_some(), u.old.is_some()), (false, false));

        let with_key = Frame::tag(b'U')
            .u32(7)
            .u8(b'K')
            .i16(1)
            .u8(b't')
            .i32(1)
            .raw(b"1")
            .u8(b'N')
            .i16(1)
            .u8(b't')
            .i32(1)
            .raw(b"2")
            .build();
        let LogicalRepMsg::Update(u) = dec().decode(&with_key).unwrap() else {
            panic!("expected update");
        };
        assert_eq!(u.key.unwrap().columns, vec![TupleColumn::Text(b"1")]);
        assert!(u.old.is_none());
        assert_eq!(u.new_tuple.columns, vec![TupleColumn::Text(b"2")]);

        let with_old = Frame::tag(b'U')
            .u32(7)
            .u8(b'O')
            .i16(1)
            .u8(b'n')
            .u8(b'N')
            .i16(1)
            .u8(b't')
            .i32(1)
            .raw(b"2")
            .build();
        let LogicalRepMsg::Update(u) = dec().decode(&with_old).unwrap() else {
            panic!("expected update");
        };
        assert!(u.key.is_none());
        assert_eq!(u.old.unwrap().columns, vec![TupleColumn::Null]);
    }

    #[test]
    fn decodes_delete_with_key() {
        let frame = Frame::tag(b'D')
            .u32(9)
            .u8(b'K')
            .i16(1)
            .u8(b't')
            .i32(2)
            .raw(b"42")
            .build();
        let LogicalRepMsg::Delete(d) = dec().decode(&frame).unwrap() else {
            panic!("expected delete");
        };
        assert_eq!(d.rel_oid, 9);
        assert_eq!(d.key.unwrap().columns, vec![TupleColumn::Text(b"42")]);
        assert!(d.old.is_none());
    }

    #[test]
    fn decodes_relation_with_key_flag() {
        let frame = Frame::tag(b'R')
            .u32(16400)
            .cstr("public")
            .cstr("orders")
            .u8(b'd')
            .i16(2)
            .u8(1)
            .cstr("id")
            .u32(23)
            .i32(-1)
            .u8(0)
            .cstr("note")
            .u32(25)
            .i32(-1)
            .build();
        let LogicalRepMsg::Relation(rel) = dec().decode(&frame).unwrap() else {
            panic!("expected relation");
        };
        assert_eq!(
            (rel.oid, rel.namespace, rel.name),
            (16400, "public", "orders")
        );
        assert_eq!(rel.replica_identity, b'd');
        assert_eq!(rel.columns.len(), 2);
        assert!(rel.columns[0].is_key());
        assert_eq!(rel.columns[0].name, "id");
        assert_eq!(rel.columns[0].type_oid, 23);
        assert!(!rel.columns[1].is_key());
    }

    #[test]
    fn decodes_truncate_options() {
        let frame = Frame::tag(b'T').i32(2).u8(3).u32(100).u32(200).build();
        let LogicalRepMsg::Truncate(t) = dec().decode(&frame).unwrap() else {
            panic!("expected truncate");
        };
        assert_eq!(t.relations, vec![100, 200]);
        assert!(t.cascade());
        assert!(t.restart_identity());
    }

    #[test]
    fn decodes_logical_message_transactional_flag() {
        let frame = Frame::tag(b'M')
            .u8(1)
            .u64(0xABCD)
            .cstr("pgshard")
            .i32(4)
            .raw(b"jrnl")
            .build();
        let LogicalRepMsg::Message(m) = dec().decode(&frame).unwrap() else {
            panic!("expected message");
        };
        assert!(m.transactional);
        assert_eq!(m.xid, None);
        assert_eq!(m.lsn, Lsn(0xABCD));
        assert_eq!(m.prefix, "pgshard");
        assert_eq!(m.content, b"jrnl");
    }

    #[test]
    fn decodes_origin_and_type() {
        let origin = Frame::tag(b'O').u64(0x50).cstr("pgshard_reverse").build();
        assert_eq!(
            dec().decode(&origin).unwrap(),
            LogicalRepMsg::Origin(Origin {
                commit_lsn: Lsn(0x50),
                name: "pgshard_reverse",
            })
        );
        let ty = Frame::tag(b'Y')
            .u32(1234)
            .cstr("public")
            .cstr("mood")
            .build();
        assert_eq!(
            dec().decode(&ty).unwrap(),
            LogicalRepMsg::Type(TypeDecl {
                xid: None,
                oid: 1234,
                namespace: "public",
                name: "mood",
            })
        );
    }

    #[test]
    fn tracks_the_stream_bracket_for_the_xid_prefix() {
        let mut d = dec();
        let start = Frame::tag(b'S').u32(555).u8(1).build();
        assert_eq!(
            d.decode(&start).unwrap(),
            LogicalRepMsg::StreamStart(StreamStart {
                xid: 555,
                first_segment: true,
            })
        );
        assert!(d.in_stream());

        // Inside the stream, an insert carries the xid prefix before rel_oid.
        let streamed_insert = Frame::tag(b'I')
            .u32(555)
            .u32(42)
            .u8(b'N')
            .i16(1)
            .u8(b't')
            .i32(2)
            .raw(b"hi")
            .build();
        let LogicalRepMsg::Insert(ins) = d.decode(&streamed_insert).unwrap() else {
            panic!("expected insert");
        };
        assert_eq!(ins.xid, Some(555));
        assert_eq!(ins.rel_oid, 42);

        assert_eq!(d.decode(b"E").unwrap(), LogicalRepMsg::StreamStop);
        assert!(!d.in_stream());

        // Outside the stream the same layout has no prefix.
        let plain_insert = Frame::tag(b'I')
            .u32(42)
            .u8(b'N')
            .i16(1)
            .u8(b't')
            .i32(2)
            .raw(b"hi")
            .build();
        let LogicalRepMsg::Insert(ins) = d.decode(&plain_insert).unwrap() else {
            panic!("expected insert");
        };
        assert_eq!(ins.xid, None);
    }

    #[test]
    fn stream_abort_reads_abort_fields_per_negotiated_stream_mode() {
        let with_extra = Frame::tag(b'A').u32(1).u32(2).u64(0x99).i64(725).build();
        let bare = Frame::tag(b'A').u32(1).u32(2).build();

        // Parallel streaming: the abort LSN + timestamp are read.
        assert_eq!(
            PgOutputDecoder::with_stream_mode(4, StreamMode::Parallel)
                .decode(&with_extra)
                .unwrap(),
            LogicalRepMsg::StreamAbort(StreamAbort {
                xid: 1,
                subxid: 2,
                abort_lsn: Some(Lsn(0x99)),
                abort_timestamp: Some(725),
            })
        );
        // Non-parallel: only the two xids, no guessing from length.
        assert_eq!(
            PgOutputDecoder::with_stream_mode(4, StreamMode::On)
                .decode(&bare)
                .unwrap(),
            LogicalRepMsg::StreamAbort(StreamAbort {
                xid: 1,
                subxid: 2,
                abort_lsn: None,
                abort_timestamp: None,
            })
        );
        // The mode makes it exact rather than heuristic: a non-parallel frame that
        // carries 16 stray bytes is rejected, and a parallel frame missing them is
        // truncated — neither is silently misread.
        assert_eq!(
            PgOutputDecoder::with_stream_mode(4, StreamMode::On)
                .decode(&with_extra)
                .unwrap_err(),
            DecodeError::TrailingBytes(16)
        );
        assert!(matches!(
            PgOutputDecoder::with_stream_mode(4, StreamMode::Parallel)
                .decode(&bare)
                .unwrap_err(),
            DecodeError::Truncated { .. }
        ));
    }

    #[test]
    fn a_decode_error_does_not_corrupt_the_stream_bracket() {
        // A malformed Stream Start (trailing byte) must not leave in_stream set;
        // otherwise a following plain change would be misread as carrying a prefix.
        let mut d = dec();
        let bad_start = Frame::tag(b'S').u32(9).u8(1).u8(0xFF).build();
        assert_eq!(
            d.decode(&bad_start).unwrap_err(),
            DecodeError::TrailingBytes(1)
        );
        assert!(!d.in_stream());
    }

    #[test]
    fn decodes_two_phase_prepare_and_rollback() {
        let prepare = Frame::tag(b'P')
            .u8(0)
            .u64(0x10)
            .u64(0x20)
            .i64(725)
            .u32(77)
            .cstr("gid-1")
            .build();
        assert_eq!(
            dec().decode(&prepare).unwrap(),
            LogicalRepMsg::Prepare(PreparedTxn {
                lsn: Lsn(0x10),
                end_lsn: Lsn(0x20),
                timestamp: 725,
                xid: 77,
                gid: "gid-1",
            })
        );
        let rollback = Frame::tag(b'r')
            .u8(0)
            .u64(0x10)
            .u64(0x30)
            .i64(725)
            .i64(726)
            .u32(77)
            .cstr("gid-1")
            .build();
        assert_eq!(
            dec().decode(&rollback).unwrap(),
            LogicalRepMsg::RollbackPrepared(RollbackPrepared {
                prepared_end_lsn: Lsn(0x10),
                rollback_end_lsn: Lsn(0x30),
                prepare_timestamp: 725,
                rollback_timestamp: 726,
                xid: 77,
                gid: "gid-1",
            })
        );
    }

    #[test]
    fn rejects_malformed_frames() {
        // Empty frame: no tag byte.
        assert_eq!(
            dec().decode(&[]).unwrap_err(),
            DecodeError::Truncated {
                offset: 0,
                needed: 1
            }
        );
        // Unknown top-level message type.
        assert_eq!(
            dec().decode(b"Z").unwrap_err(),
            DecodeError::UnknownMessage(b'Z')
        );
        // Insert missing its 'N' marker.
        let bad_marker = Frame::tag(b'I').u32(1).u8(b'X').build();
        assert_eq!(
            dec().decode(&bad_marker).unwrap_err(),
            DecodeError::UnexpectedTupleMarker(b'X')
        );
        // Unknown tuple column kind.
        let bad_kind = Frame::tag(b'I').u32(1).u8(b'N').i16(1).u8(b'?').build();
        assert_eq!(
            dec().decode(&bad_kind).unwrap_err(),
            DecodeError::UnknownTupleKind(b'?')
        );
        // A field length that overruns the frame.
        let overrun = Frame::tag(b'I')
            .u32(1)
            .u8(b'N')
            .i16(1)
            .u8(b't')
            .i32(99)
            .raw(b"ab")
            .build();
        assert!(matches!(
            dec().decode(&overrun).unwrap_err(),
            DecodeError::Truncated { .. }
        ));
        // Trailing bytes after an otherwise complete message.
        let trailing = Frame::tag(b'B').u64(0).i64(0).u32(0).raw(b"extra").build();
        assert_eq!(
            dec().decode(&trailing).unwrap_err(),
            DecodeError::TrailingBytes(5)
        );
    }
}
