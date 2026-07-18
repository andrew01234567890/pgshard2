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

/// Advances the row by `block_size` and returns the post-update `next_id` and
/// the `block_size`, so the caller derives the claimed range as
/// `[next_id - block_size, next_id)`. The `CASE` makes the advance a no-op for a
/// non-positive `block_size`: a misconfigured row is reported (not silently
/// used) *without* first moving `next_id` — otherwise a bad reservation would
/// shift the row backward and a later repair could hand out overlapping ids.
const RESERVE_SQL: &str = "\
    UPDATE pgshard.sequences \
       SET next_id = next_id + CASE WHEN block_size > 0 THEN block_size ELSE 0 END \
     WHERE name = $1 \
    RETURNING next_id, block_size";

/// Reserves sequence blocks from the system database over one blocking
/// connection, reconnecting on error. Cloneable connection config; the live
/// connection is lazily established on the first reservation.
pub struct PgBlockReserver {
    conn: postgres::Config,
    client: Mutex<Option<Client>>,
}

impl PgBlockReserver {
    /// `conn` is a typed connection config pointing at the system database.
    /// Typed — not a libpq conninfo string — so a credential containing
    /// whitespace or quotes can never split into extra connection options or
    /// redirect the reservation (overlapping ID blocks would follow).
    pub fn new(conn: postgres::Config) -> Self {
        Self {
            conn,
            client: Mutex::new(None),
        }
    }
}

/// The production construction of the reservation config — typed setters only,
/// so hostile credential content is inert. Kept here (not in the binary) so the
/// unit test guards the exact code the router runs.
pub fn reserver_config(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    dbname: &str,
) -> postgres::Config {
    let mut cfg = postgres::Config::new();
    cfg.host(host)
        .port(port)
        .user(user)
        .password(password)
        .dbname(dbname);
    cfg
}

impl BlockReserver for PgBlockReserver {
    fn reserve(&self, sequence: &str) -> Result<(i64, i64), SeqError> {
        // Recover a poisoned lock rather than wedge every future reservation:
        // the guarded value is just a reconnectable client.
        let mut guard = self.client.lock().unwrap_or_else(|e| e.into_inner());
        if guard.is_none() {
            *guard = Some(connect(&self.conn)?);
        }
        let client = guard.as_mut().expect("just connected");

        match client.query_opt(RESERVE_SQL, &[&sequence]) {
            Ok(Some(row)) => {
                let next_id: i64 = row.get(0);
                let size: i64 = row.get(1);
                // The CASE in RESERVE_SQL left `next_id` unmoved for a
                // non-positive block_size, so rejecting here has corrupted
                // nothing — report the misconfigured row loudly.
                if size <= 0 {
                    return Err(SeqError::Backend(format!(
                        "sequence {sequence:?} has a non-positive block_size {size}"
                    )));
                }
                Ok((next_id - size, size))
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

/// Opens the reservation connection with a bounded `statement_timeout`, so a
/// hung system database fails a reservation loudly (a `Backend` error) instead
/// of blocking every sequence's refill on the single shared connection forever.
/// Reservations are single-row updates that complete in well under this bound.
fn connect(conn: &postgres::Config) -> Result<Client, SeqError> {
    let mut client = conn
        .connect(NoTls)
        .map_err(|e| SeqError::Backend(e.to_string()))?;
    client
        .batch_execute("SET statement_timeout = '10s'")
        .map_err(|e| SeqError::Backend(e.to_string()))?;
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::reserver_config;

    #[test]
    fn hostile_password_stays_one_credential() {
        // The production builder keeps a password containing conninfo syntax
        // inert: one credential, one host, nothing reinterpreted.
        let cfg = reserver_config(
            "sys",
            5432,
            "router",
            "p ss'word host=evil port=9999",
            "pgshard_system",
        );
        assert_eq!(
            cfg.get_password(),
            Some("p ss'word host=evil port=9999".as_bytes())
        );
        assert_eq!(cfg.get_hosts().len(), 1);
        assert_eq!(cfg.get_ports(), &[5432]);
        assert_eq!(cfg.get_user(), Some("router"));
        assert_eq!(cfg.get_dbname(), Some("pgshard_system"));
    }
}
