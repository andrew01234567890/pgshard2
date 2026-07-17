//! The Postgres reservation backend for global sequences.
//!
//! [`pgshard_seq::SequenceCache`] hands ids out of reserved blocks; this
//! reserves those blocks from the authoritative `pgshard.sequences` row in the
//! system database. The one thing sequences must never do is duplicate an id,
//! and the one thing that prevents it is that every reserved range is disjoint:
//! a single atomic `UPDATE pgshard.sequences SET next_id = next_id + block_size
//! ... RETURNING` advances the row and returns the range it just claimed, so two
//! concurrent reservations — on any routers — can never claim the same id.
//!
//! [`pgshard_seq::BlockReserver`] is synchronous (a reservation happens off the
//! hot path, amortized across a whole block), so this uses the blocking
//! `postgres` client on a dedicated connection rather than the async query pool.
//!
//! Because that client drives its own runtime internally, [`PgBlockReserver::reserve`]
//! must be called from a blocking context — a [`tokio::task::spawn_blocking`]
//! closure or the background refill task — never directly on an async worker,
//! where the nested `block_on` would panic.

use std::sync::Mutex;

use pgshard_seq::{BlockReserver, SeqError};
use postgres::{Client, NoTls};

/// Reserves the next range and returns `(start, block_size)`: `next_id` in the
/// `RETURNING` is the already-incremented value, so `next_id - block_size` is
/// the start of the range this statement just claimed.
const RESERVE_SQL: &str = "\
    UPDATE pgshard.sequences \
       SET next_id = next_id + block_size \
     WHERE name = $1 \
    RETURNING next_id - block_size, block_size";

/// Reserves sequence blocks from the system database over one blocking
/// connection, reconnecting on error. Cloneable connection config; the live
/// connection is lazily established on the first reservation.
pub struct PgBlockReserver {
    conn: String,
    client: Mutex<Option<Client>>,
}

impl PgBlockReserver {
    /// `conn` is a libpq connection string pointing at the system database.
    pub fn new(conn: impl Into<String>) -> Self {
        Self {
            conn: conn.into(),
            client: Mutex::new(None),
        }
    }
}

impl BlockReserver for PgBlockReserver {
    fn reserve(&self, sequence: &str) -> Result<(i64, i64), SeqError> {
        // Recover a poisoned lock rather than wedge every future reservation:
        // the guarded value is just a reconnectable client.
        let mut guard = self.client.lock().unwrap_or_else(|e| e.into_inner());
        if guard.is_none() {
            *guard = Some(
                Client::connect(&self.conn, NoTls).map_err(|e| SeqError::Backend(e.to_string()))?,
            );
        }
        let client = guard.as_mut().expect("just connected");

        match client.query_opt(RESERVE_SQL, &[&sequence]) {
            Ok(Some(row)) => {
                let start: i64 = row.get(0);
                let size: i64 = row.get(1);
                // A non-positive block would hand back an empty range forever;
                // reject a misconfigured row loudly instead of looping.
                if size <= 0 {
                    return Err(SeqError::Backend(format!(
                        "sequence {sequence:?} has a non-positive block_size {size}"
                    )));
                }
                Ok((start, size))
            }
            // No row updated: the sequence is not registered in the system DB.
            Ok(None) => Err(SeqError::UnknownSequence(sequence.to_owned())),
            Err(e) => {
                // Drop a possibly-broken connection so the next call reconnects.
                *guard = None;
                Err(SeqError::Backend(e.to_string()))
            }
        }
    }
}
