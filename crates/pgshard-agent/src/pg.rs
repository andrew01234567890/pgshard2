//! The real [`Instance`]: the local PostgreSQL the agent supervises, reached
//! over libpq. This first step implements the read-only status path and
//! promotion (the failover path the operator drives); fencing and standby
//! rejoin need the process-supervision half of the agent and are wired later.

use async_trait::async_trait;
use tokio_postgres::NoTls;
use tokio_postgres::error::SqlState;

use crate::instance::{Instance, RestorePoint, Snapshot};

pub struct PgInstance {
    conn_string: String,
}

impl PgInstance {
    pub fn new(conn_string: String) -> Self {
        Self { conn_string }
    }

    async fn connect(&self) -> anyhow::Result<tokio_postgres::Client> {
        let (client, connection) = tokio_postgres::connect(&self.conn_string, NoTls).await?;
        // The connection drives the protocol; drop it when the client is done.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = %e, "postgres connection closed");
            }
        });
        Ok(client)
    }
}

/// Quote a PostgreSQL identifier: wrap in double quotes and double any embedded
/// double quote. Database names come from the operator (Kubernetes object
/// names), but quoting keeps the generated DDL correct for names with hyphens
/// and safe against injection regardless of the source.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// PostgreSQL text LSN ("X/Y", hex) to the u64 the wire protocol uses.
fn parse_lsn(text: &str) -> u64 {
    match text.split_once('/') {
        Some((hi, lo)) => {
            let hi = u64::from_str_radix(hi, 16).unwrap_or(0);
            let lo = u64::from_str_radix(lo, 16).unwrap_or(0);
            (hi << 32) | lo
        }
        None => 0,
    }
}

#[async_trait]
impl Instance for PgInstance {
    async fn snapshot(&self) -> anyhow::Result<Snapshot> {
        let client = self.connect().await?;
        // One round trip; LSNs come back as text so we never lose the low bits to
        // a numeric cast, and each side's LSN is NULL on the other role.
        let row = client
            .query_one(
                "SELECT pg_is_in_recovery(),
                        (SELECT timeline_id FROM pg_control_checkpoint()),
                        CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_wal_lsn()::text END,
                        CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn()::text ELSE NULL END,
                        CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()::text ELSE NULL END,
                        EXISTS (SELECT 1 FROM pg_stat_wal_receiver),
                        current_setting('server_version'),
                        (SELECT system_identifier FROM pg_control_system())",
                &[],
            )
            .await?;

        let in_recovery: bool = row.get(0);
        let timeline: i32 = row.get(1);
        let write_lsn: Option<String> = row.get(2);
        let receive_lsn: Option<String> = row.get(3);
        let replay_lsn: Option<String> = row.get(4);
        let receiver_active: bool = row.get(5);
        let version: String = row.get(6);
        let system_id: i64 = row.get(7);

        Ok(Snapshot {
            in_recovery,
            // A successful query means the instance accepts connections.
            accepting: true,
            timeline: timeline as u32,
            write_lsn: write_lsn.as_deref().map(parse_lsn).unwrap_or(0),
            receive_lsn: receive_lsn.as_deref().map(parse_lsn).unwrap_or(0),
            replay_lsn: replay_lsn.as_deref().map(parse_lsn).unwrap_or(0),
            receiver_active,
            postgres_version: version,
            system_id: system_id as u64,
            fenced: false,
        })
    }

    async fn promote(&self) -> anyhow::Result<u32> {
        let client = self.connect().await?;
        // pg_promote waits for the promotion to complete (default 60s).
        let promoted: bool = client
            .query_one("SELECT pg_promote(true)", &[])
            .await?
            .get(0);
        if !promoted {
            anyhow::bail!("pg_promote timed out before the standby became primary");
        }
        // Read the LIVE timeline from the current WAL file name, not
        // pg_control_checkpoint(): the latter reflects the last completed
        // checkpoint, and the post-promotion checkpoint is asynchronous, so it
        // would still report the pre-promotion timeline for a while.
        let timeline: i32 = client
            .query_one(
                "SELECT ('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::int",
                &[],
            )
            .await?
            .get(0);
        Ok(timeline as u32)
    }

    async fn set_fenced(&self, _fenced: bool) -> anyhow::Result<()> {
        anyhow::bail!("fencing requires the process-supervising agent (not yet implemented)")
    }

    async fn rejoin(&self, _upstream: &str, _allow_rewind: bool) -> anyhow::Result<bool> {
        anyhow::bail!("standby rejoin requires the process-supervising agent (not yet implemented)")
    }

    async fn exec_sql(&self, sql: &str) -> anyhow::Result<()> {
        // Simple-query protocol so statements that cannot run inside a
        // transaction block (CREATE DATABASE, CREATE INDEX CONCURRENTLY) are not
        // implicitly wrapped in one.
        self.connect().await?.batch_execute(sql).await?;
        Ok(())
    }

    async fn create_database(&self, name: &str, owner: &str) -> anyhow::Result<()> {
        let client = self.connect().await?;
        // Fast path: skip the DDL (and its error) when the database is present.
        let exists: bool = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)",
                &[&name],
            )
            .await?
            .get(0);
        if exists {
            return Ok(());
        }
        let mut stmt = format!("CREATE DATABASE {}", quote_ident(name));
        if !owner.is_empty() {
            stmt.push_str(&format!(" OWNER {}", quote_ident(owner)));
        }
        // CREATE DATABASE cannot run inside a transaction block; simple-query.
        match client.batch_execute(&stmt).await {
            Ok(()) => Ok(()),
            // Lost the check-then-create race with a concurrent caller: the
            // database now exists, which is the requested end state.
            Err(e) if e.code() == Some(&SqlState::DUPLICATE_DATABASE) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn drop_database(&self, name: &str) -> anyhow::Result<()> {
        // IF EXISTS makes the drop idempotent; WITH (FORCE) terminates any
        // sessions still on the database (PG13+) so it can be removed at once.
        let stmt = format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", quote_ident(name));
        self.connect().await?.batch_execute(&stmt).await?;
        Ok(())
    }

    async fn create_restore_point(&self, name: &str) -> anyhow::Result<RestorePoint> {
        let client = self.connect().await?;
        // pg_create_restore_point returns the point's LSN; the timeline comes
        // from the control file. On a standby PostgreSQL errors ("recovery is in
        // progress"), which propagates — the caller targets the primary.
        let row = client
            .query_one(
                "SELECT pg_create_restore_point($1)::text,
                        (SELECT timeline_id FROM pg_control_checkpoint())",
                &[&name],
            )
            .await?;
        let lsn: String = row.get(0);
        let timeline: i32 = row.get(1);
        Ok(RestorePoint {
            lsn: parse_lsn(&lsn),
            timeline: timeline as u32,
        })
    }

    async fn switch_wal(&self, wait_archived: bool) -> anyhow::Result<u64> {
        // Waiting for the switched segment to be confirmed archived (so the point
        // is immediately restorable) needs a bounded poll of pg_stat_archiver
        // against a correctly-identified segment; that lands with the barrier
        // controller and its archiving-enabled e2e. Reject rather than silently
        // return an unconfirmed LSN.
        anyhow::ensure!(
            !wait_archived,
            "switch_wal with wait_archived is not implemented yet"
        );
        let lsn: String = self
            .connect()
            .await?
            .query_one("SELECT pg_switch_wal()::text", &[])
            .await?
            .get(0);
        Ok(parse_lsn(&lsn))
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_lsn, quote_ident};

    #[test]
    fn parses_hex_lsn() {
        assert_eq!(parse_lsn("0/0"), 0);
        assert_eq!(parse_lsn("0/5000000"), 0x5000000);
        assert_eq!(parse_lsn("1/0"), 1u64 << 32);
        assert_eq!(parse_lsn("garbage"), 0);
    }

    #[test]
    fn quotes_identifiers_and_escapes_embedded_quotes() {
        assert_eq!(quote_ident("mycl-x40-x80"), "\"mycl-x40-x80\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }
}
