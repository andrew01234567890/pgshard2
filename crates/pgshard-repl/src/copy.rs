//! Filtered snapshot copy — the initial data load of a reshard seed.
//!
//! Before streaming changes, the seeder copies the source table's existing rows
//! that belong to the target key range, as of the replication slot's exported
//! snapshot, so the stream picks up exactly where the copy ends — no gap, no
//! overlap at the seam.
//!
//! The copy runs `COPY … TO STDOUT` on the source and passes each matching row,
//! byte for byte, into `COPY … FROM STDIN` on the target. Only the shard-key
//! column is decoded (to decide keep-or-skip); every other column is streamed
//! verbatim in PostgreSQL's own text format, so the copy is lossless for any
//! column type. The keep decision uses the SAME keyspace-id logic as the stream
//! ([`crate::filter::text_keyspace_id`]), so a row is copied iff it would also be
//! kept by the stream.

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, TryStreamExt, pin_mut};
use pgshard_core::{KeyRange, ScalarType, ShardFunction};
use thiserror::Error;
use tokio_postgres::Client;

use crate::filter::{FilterError, text_keyspace_id};

/// Why a filtered snapshot copy failed.
#[derive(Debug, Error)]
pub enum CopyError {
    #[error("source database error: {0}")]
    Source(tokio_postgres::Error),
    #[error("target database error: {0}")]
    Target(tokio_postgres::Error),
    #[error("filtering a row's shard key: {0}")]
    Filter(FilterError),
    #[error("shard-key column {0:?} is not among the copied columns")]
    NoShardKeyColumn(String),
    #[error("malformed COPY data: {0}")]
    Malformed(String),
    #[error("snapshot id {0:?} is not a valid PostgreSQL snapshot identifier")]
    BadSnapshot(String),
}

/// What to copy and how to filter it. `columns` fixes the COPY column order, so
/// the shard-key column's position is known; the source and target COPY use the
/// same list, so the raw rows pass straight through.
pub struct CopySpec<'a> {
    pub schema: &'a str,
    pub table: &'a str,
    pub columns: &'a [String],
    pub shard_key_column: &'a str,
    pub shard_key_type: ScalarType,
    pub target_range: KeyRange,
}

/// Copy the rows of `spec`'s table that fall in `spec.target_range`, as of
/// `snapshot`, from `source` to `target`. Returns the number of rows copied.
///
/// `snapshot` must be the snapshot exported by the slot the stream will resume
/// from (see [`crate::client::ReplicationClient::create_logical_slot_exported`]),
/// and this must run while that replication connection is still idle.
///
/// The `target` role must be allowed to set `session_replication_role`
/// (superuser, or `GRANT SET ON PARAMETER session_replication_role`): the copy
/// runs under replica session semantics so the target's ordinary triggers do
/// not fire on seeded rows. A role without that privilege fails the copy
/// loudly before any row moves.
///
/// The target COPY runs inside an explicit transaction committed only after the
/// COPY has fully completed, so an error — or this future being dropped — never
/// leaves partially or invisibly committed rows behind: an uncommitted target
/// transaction aborts when the session ends. The one irreducible ambiguity is a
/// drop during the final COMMIT itself (the classic in-doubt commit); a caller
/// that cancels this future must therefore discard both connections and have the
/// seeding workflow decide completion from its own durable marker, never from
/// the absence of an `Ok` here.
pub async fn copy_filtered(
    source: &Client,
    target: &Client,
    snapshot: &str,
    spec: &CopySpec<'_>,
    shard_fn: &dyn ShardFunction,
) -> Result<u64, CopyError> {
    // Snapshot ids are hyphen-separated hex; refuse anything that could break out
    // of the SET TRANSACTION SNAPSHOT literal (it is DB-generated, but cheap to
    // pin).
    if snapshot.is_empty() || !snapshot.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
        return Err(CopyError::BadSnapshot(snapshot.to_owned()));
    }

    let key_index = spec
        .columns
        .iter()
        .position(|c| c == spec.shard_key_column)
        .ok_or_else(|| CopyError::NoShardKeyColumn(spec.shard_key_column.to_owned()))?;

    let table = format!("{}.{}", quote_ident(spec.schema), quote_ident(spec.table));
    let col_list = spec
        .columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    // Bind the exported snapshot so the copy sees exactly the slot's consistent
    // point. SET TRANSACTION SNAPSHOT must precede the first query; bytea_output
    // is pinned to hex so a bytea shard key decodes the same as the stream's.
    // BEGIN runs separately so a failure to import the snapshot (e.g. the
    // exporting connection died) can still roll the source session back instead
    // of stranding it in an aborted transaction.
    source
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .map_err(CopyError::Source)?;
    // row_security=off makes RLS fail closed for the seed: if a policy would
    // filter the table for this role, COPY errors instead of silently omitting
    // rows that the stream (which reads WAL, not queries) would then never
    // deliver either. The output-shaping GUCs mirror the replication client's
    // startup pins so the copied text is byte-identical to the streamed text.
    let setup = source
        .batch_execute(&format!(
            "SET TRANSACTION SNAPSHOT '{snapshot}'; \
             SET LOCAL row_security = off; \
             SET LOCAL bytea_output = 'hex'; \
             SET LOCAL DateStyle = 'ISO'; \
             SET LOCAL IntervalStyle = 'postgres'; \
             SET LOCAL extra_float_digits = 3"
        ))
        .await
        .map_err(CopyError::Source);

    // From here on, no early return: every path falls through to the source
    // ROLLBACK below, so a failure part-way (e.g. the target role lacking the
    // session_replication_role privilege after its BEGIN already ran) cannot
    // strand either caller-owned session in an open or aborted transaction.
    let result = match setup {
        Ok(()) => {
            // replica role keeps the target's ordinary triggers from firing on
            // seeded rows (COPY FROM invokes triggers); the source already
            // materialized their effects. DateStyle=ISO parses the source's
            // pinned ISO output unambiguously whatever the target's default.
            let target_setup = target
                .batch_execute(
                    "BEGIN; \
                     SET LOCAL session_replication_role = replica; \
                     SET LOCAL DateStyle = 'ISO'; \
                     SET LOCAL IntervalStyle = 'postgres'",
                )
                .await
                .map_err(CopyError::Target);
            match target_setup {
                Ok(()) => {
                    let copied = stream_filtered(
                        source, target, &table, &col_list, key_index, spec, shard_fn,
                    )
                    .await;
                    match copied {
                        Ok(n) => {
                            // Rows become visible only here, after the COPY fully
                            // completed — never part-way through a dropped future.
                            target
                                .batch_execute("COMMIT")
                                .await
                                .map_err(CopyError::Target)
                                .map(|()| n)
                        }
                        Err(e) => {
                            let _ = target.batch_execute("ROLLBACK").await;
                            Err(e)
                        }
                    }
                }
                Err(e) => {
                    // BEGIN may have succeeded before a later statement in the
                    // batch failed, leaving the target in an aborted transaction.
                    let _ = target.batch_execute("ROLLBACK").await;
                    Err(e)
                }
            }
        }
        Err(e) => Err(e),
    };

    // Always end the read-only source transaction; ROLLBACK also clears an
    // aborted state after a failed snapshot import. Best-effort: on the success
    // path the data is already committed on the target, and failing the whole
    // copy over a source-session cleanup hiccup would be a false failure.
    let _ = source.batch_execute("ROLLBACK").await;
    result
}

async fn stream_filtered(
    source: &Client,
    target: &Client,
    table: &str,
    col_list: &str,
    key_index: usize,
    spec: &CopySpec<'_>,
    shard_fn: &dyn ShardFunction,
) -> Result<u64, CopyError> {
    let out = source
        .copy_out(&format!("COPY {table} ({col_list}) TO STDOUT"))
        .await
        .map_err(CopyError::Source)?;
    let sink = target
        .copy_in::<_, Bytes>(&format!("COPY {table} ({col_list}) FROM STDIN"))
        .await
        .map_err(CopyError::Target)?;
    pin_mut!(out);
    pin_mut!(sink);

    let mut buf = BytesMut::new();
    let mut copied = 0u64;
    while let Some(chunk) = out.try_next().await.map_err(CopyError::Source)? {
        buf.extend_from_slice(&chunk);
        // COPY text format terminates every row with an unescaped newline
        // (embedded newlines are escaped), so a raw \n is always a row boundary.
        while let Some(nl) = buf.iter().position(|&b| b == b'\n') {
            let row = buf.split_to(nl + 1).freeze();
            if row_in_range(&row, key_index, spec, shard_fn)? {
                sink.send(row).await.map_err(CopyError::Target)?;
                copied += 1;
            }
        }
    }
    if !buf.is_empty() {
        return Err(CopyError::Malformed(
            "COPY stream ended without a final row terminator".to_owned(),
        ));
    }
    sink.finish().await.map_err(CopyError::Target)?;
    Ok(copied)
}

/// Whether one COPY row's shard-key column falls in the target range. `row`
/// includes its trailing newline.
fn row_in_range(
    row: &[u8],
    key_index: usize,
    spec: &CopySpec<'_>,
    shard_fn: &dyn ShardFunction,
) -> Result<bool, CopyError> {
    let line = row.strip_suffix(b"\n").unwrap_or(row);
    let field = line.split(|&b| b == b'\t').nth(key_index).ok_or_else(|| {
        CopyError::Malformed(format!("row has fewer than {} columns", key_index + 1))
    })?;
    let value = copy_field_unescape(field).ok_or(CopyError::Filter(FilterError::UnroutableCell))?;
    let text =
        std::str::from_utf8(&value).map_err(|_| CopyError::Filter(FilterError::InvalidUtf8))?;
    let id = text_keyspace_id(text, spec.shard_key_type, shard_fn).map_err(CopyError::Filter)?;
    Ok(spec.target_range.contains(id))
}

/// Decode one COPY-text field to its raw bytes, or `None` for the NULL marker
/// `\N`. COPY TO emits only the letter escapes and `\\`, never octal/hex, so this
/// recovers the value's output-function text exactly — which is what the stream
/// hashes too.
fn copy_field_unescape(field: &[u8]) -> Option<Vec<u8>> {
    if field == br"\N" {
        return None;
    }
    let mut out = Vec::with_capacity(field.len());
    let mut i = 0;
    while i < field.len() {
        if field[i] == b'\\' && i + 1 < field.len() {
            let mapped = match field[i + 1] {
                b'b' => 0x08,
                b'f' => 0x0C,
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'v' => 0x0B,
                b'\\' => b'\\',
                // Any other backslashed character is taken literally.
                other => other,
            };
            out.push(mapped);
            i += 2;
        } else {
            out.push(field[i]);
            i += 1;
        }
    }
    Some(out)
}

/// Quote a SQL identifier by doubling embedded double quotes.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unescapes_copy_fields() {
        // Plain values pass through.
        assert_eq!(copy_field_unescape(b"123"), Some(b"123".to_vec()));
        assert_eq!(copy_field_unescape(b""), Some(Vec::new()));
        // Letter escapes decode to their control bytes.
        assert_eq!(copy_field_unescape(br"a\tb"), Some(b"a\tb".to_vec()));
        assert_eq!(copy_field_unescape(br"a\nb"), Some(b"a\nb".to_vec()));
        // A doubled backslash is one backslash — so a hex bytea value, shipped as
        // `\\xdead`, recovers to `\xdead`, exactly what the stream hashes.
        assert_eq!(copy_field_unescape(br"a\\b"), Some(br"a\b".to_vec()));
        assert_eq!(
            copy_field_unescape(br"\\xdeadbeef"),
            Some(br"\xdeadbeef".to_vec())
        );
        // The bare `\N` is NULL; an escaped backslash-then-N is the literal value.
        assert_eq!(copy_field_unescape(br"\N"), None);
        assert_eq!(copy_field_unescape(br"\\N"), Some(br"\N".to_vec()));
    }

    #[test]
    fn quotes_identifiers() {
        assert_eq!(quote_ident("orders"), "\"orders\"");
        assert_eq!(quote_ident("weird\"name"), "\"weird\"\"name\"");
    }
}
