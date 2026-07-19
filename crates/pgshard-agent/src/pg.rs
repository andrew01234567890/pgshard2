//! The real [`Instance`]: the local PostgreSQL the agent supervises, reached
//! over libpq. This first step implements the read-only status path and
//! promotion (the failover path the operator drives); fencing and standby
//! rejoin need the process-supervision half of the agent and are wired later.

use async_trait::async_trait;
use tokio_postgres::NoTls;
use tokio_postgres::error::SqlState;

use crate::instance::{ForeignDatabase, Instance, RestorePoint, Snapshot, provenance_marker};

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

    /// Connect into a specific database on this instance (a node hosts one
    /// shard per DATABASE; publication DDL is database-scoped).
    async fn connect_to(&self, database: &str) -> anyhow::Result<tokio_postgres::Client> {
        let mut config: tokio_postgres::Config = self.conn_string.parse()?;
        config.dbname(database);
        let (client, connection) = config.connect(NoTls).await?;
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

/// Quote a PostgreSQL string literal: wrap in single quotes and double any
/// embedded single quote. The service layer restricts the characters that
/// reach this; quoting keeps the statement correct regardless.
fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

async fn stamp_provenance(
    client: &tokio_postgres::Client,
    name: &str,
    provenance: &str,
) -> anyhow::Result<()> {
    let stmt = format!(
        "COMMENT ON DATABASE {} IS {}",
        quote_ident(name),
        quote_literal(&provenance_marker(provenance))
    );
    client.batch_execute(&stmt).await?;
    Ok(())
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
                // On a primary the live timeline comes from the current WAL file
                // name (pg_control_checkpoint lags a promotion until its async
                // checkpoint); on a standby pg_current_wal_lsn errors, so the
                // control file's replayed timeline is used.
                "SELECT pg_is_in_recovery(),
                        CASE WHEN pg_is_in_recovery()
                             THEN (SELECT timeline_id FROM pg_control_checkpoint())
                             ELSE ('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::int
                        END,
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

    async fn create_database(
        &self,
        name: &str,
        owner: &str,
        provenance: &str,
        adopt: bool,
    ) -> anyhow::Result<()> {
        let client = self.connect().await?;
        // Bounded verify-or-create: each retry is a lost check-then-create
        // race, and a create/drop flap must surface as a retriable error
        // rather than hold the RPC (and its operator worker) forever.
        for _ in 0..3 {
            // The provenance marker lives in the database's comment
            // (pg_shdescription), the one annotation CREATE DATABASE-adjacent
            // metadata offers without connecting into the new database.
            let existing = client
                .query_opt(
                    "SELECT sd.description
                     FROM pg_database d
                     LEFT JOIN pg_shdescription sd
                       ON sd.objoid = d.oid AND sd.classoid = 'pg_database'::regclass
                     WHERE d.datname = $1",
                    &[&name],
                )
                .await?;
            if let Some(row) = existing {
                if provenance.is_empty() {
                    return Ok(());
                }
                let found: Option<String> = row.get(0);
                return match found {
                    Some(ref marker) if *marker == provenance_marker(provenance) => Ok(()),
                    _ if adopt => stamp_provenance(&client, name, provenance).await,
                    found => Err(ForeignDatabase {
                        name: name.to_owned(),
                        found,
                    }
                    .into()),
                };
            }
            let mut stmt = format!("CREATE DATABASE {}", quote_ident(name));
            if !owner.is_empty() {
                stmt.push_str(&format!(" OWNER {}", quote_ident(owner)));
            }
            // CREATE DATABASE cannot run inside a transaction block; simple-query.
            match client.batch_execute(&stmt).await {
                Ok(()) => {
                    if !provenance.is_empty() {
                        // Not atomic with the create: a crash inside this window
                        // leaves the database unstamped, and the retry then fails
                        // closed (ForeignDatabase) until an explicit adopt — safer
                        // than ever mistaking foreign data for our own.
                        stamp_provenance(&client, name, provenance).await?;
                    }
                    return Ok(());
                }
                // Lost the check-then-create race: loop back to verify the
                // winner's provenance marker instead of assuming success.
                Err(e) if e.code() == Some(&SqlState::DUPLICATE_DATABASE) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        anyhow::bail!("database {name:?} keeps flapping between absent and present; retry later")
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
        // pg_create_restore_point returns the point's LSN; the timeline is read
        // from that LSN's WAL file name, not pg_control_checkpoint() — the latter
        // reflects the last completed checkpoint, so right after a promotion it
        // would report the pre-promotion timeline while the new point sits on the
        // new one (the same trap `promote` avoids). On a standby PostgreSQL errors
        // ("recovery is in progress"), which propagates — the caller targets the
        // primary.
        let row = client
            .query_one(
                "SELECT lsn::text, ('x' || substr(pg_walfile_name(lsn), 1, 8))::bit(32)::int
                 FROM pg_create_restore_point($1) AS lsn",
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

    async fn fence_writes(&self, database: &str) -> anyhow::Result<u32> {
        let client = self.connect_to(database).await?;
        // New sessions (including a router's post-termination reconnect)
        // become read-only. This affects sessions established AFTER the ALTER,
        // so the in-flight ones are drained next.
        client
            .batch_execute(&format!(
                "ALTER DATABASE {} SET default_transaction_read_only = on",
                quote_ident(database)
            ))
            .await?;
        // Terminate every client backend still inside a transaction on this
        // database: a terminated write transaction rolls back, so it can
        // never commit after the barrier. Replication walsenders
        // (backend_type <> 'client backend') and this session are left alone.
        let terminated: i64 = client
            .query_one(
                "SELECT count(pg_terminate_backend(pid))
                 FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND pid <> pg_backend_pid()
                   AND backend_type = 'client backend'
                   AND xact_start IS NOT NULL",
                &[],
            )
            .await?
            .get(0);
        Ok(terminated.max(0) as u32)
    }

    async fn emit_journal(&self, database: &str, payload: &[u8]) -> anyhow::Result<u64> {
        let mut client = self.connect_to(database).await?;
        // TRANSACTIONAL message: it decodes inside a Commit, so it reaches
        // every slot of this database through the ordinary stream. The
        // returned barrier is the MESSAGE's OWN WAL position — the same value
        // the consumer sees in its Message frame and acknowledges as
        // journal_lsn — never an instance-global flush position, which other
        // databases' commits (frameless on this slot) can push past the
        // journal forever. synchronous_commit is forced: 'off' would let
        // COMMIT return before the record is durable.
        let tx = client.transaction().await?;
        tx.batch_execute("SET LOCAL synchronous_commit = 'local'")
            .await?;
        let lsn: String = tx
            .query_one(
                "SELECT pg_logical_emit_message(true, 'pgshard', $1::bytea)::text",
                &[&payload],
            )
            .await?
            .get(0);
        tx.commit().await?;
        Ok(parse_lsn(&lsn))
    }

    async fn prepare_source(
        &self,
        database: &str,
        publication: &str,
        tables: &[(String, String)],
    ) -> anyhow::Result<Option<u64>> {
        let client = self.connect_to(database).await?;
        // The runner rejects generated columns outright; provisioning a
        // publication over such a table would only defer the failure to the
        // workflow's preflight — honor the cross-contract here instead.
        for (schema, name) in tables {
            let has_generated: bool = client
                .query_one(
                    "SELECT EXISTS (
                         SELECT 1 FROM pg_attribute a
                         JOIN pg_class c ON c.oid = a.attrelid
                         JOIN pg_namespace n ON n.oid = c.relnamespace
                         WHERE n.nspname = $1 AND c.relname = $2
                           AND a.attnum > 0 AND NOT a.attisdropped
                           AND a.attgenerated <> ''
                     )",
                    &[&schema, &name],
                )
                .await?
                .get(0);
            anyhow::ensure!(
                !has_generated,
                "table {schema}.{name} has generated columns; the seeding runner cannot stream them"
            );
        }
        // No-op when the publication already has the exact shape the seeding
        // runner's preflight demands: a reconcile retry must never rewrite the
        // catalog rows a live consumer's drift poll has pinned.
        if !publication_matches(&client, publication, tables).await? {
            // Converge by drop+recreate: a mismatched same-name publication is
            // drift or misconfiguration, and every CREATE-time property
            // (membership mode, via_partition_root) converges this way where
            // ALTER could not. Any live consumer fails loudly and re-seeds.
            client
                .execute(
                    &format!("DROP PUBLICATION IF EXISTS {}", quote_ident(publication)),
                    &[],
                )
                .await?;
            let list = tables
                .iter()
                .map(|(schema, name)| format!("{}.{}", quote_ident(schema), quote_ident(name)))
                .collect::<Vec<_>>()
                .join(", ");
            client
                .execute(
                    &format!(
                        "CREATE PUBLICATION {} FOR TABLE {list}
                         WITH (publish = 'insert, update, delete, truncate')",
                        quote_ident(publication)
                    ),
                    &[],
                )
                .await?;
        }
        let keep: String = client
            .query_one("SELECT current_setting('max_slot_wal_keep_size')", &[])
            .await?
            .get(0);
        if keep == "-1" {
            return Ok(None);
        }
        let bytes: i64 = client
            .query_one("SELECT pg_size_bytes($1)", &[&keep])
            .await?
            .get(0);
        Ok(Some(bytes.max(0) as u64))
    }
}

/// Does `publication` already publish EXACTLY `tables` in the shape the
/// seeding runner accepts (static FOR TABLE membership, every DML kind, no
/// partition-root routing, no generated columns, no row filters, no column
/// lists)?
async fn publication_matches(
    client: &tokio_postgres::Client,
    publication: &str,
    tables: &[(String, String)],
) -> anyhow::Result<bool> {
    let Some(row) = client
        .query_opt(
            "SELECT pubinsert AND pubupdate AND pubdelete AND pubtruncate
                    AND NOT puballtables AND NOT pubviaroot
                    AND pubgencols::text = 'n'
                    AND NOT EXISTS (SELECT 1 FROM pg_publication_namespace pn
                                    WHERE pn.pnpubid = p.oid)
             FROM pg_publication p WHERE pubname = $1",
            &[&publication],
        )
        .await?
    else {
        return Ok(false);
    };
    if !row.get::<_, bool>(0) {
        return Ok(false);
    }
    // prattrs/prqual are the UNDERLYING catalog state: an explicit column
    // list that happens to name every current column is indistinguishable
    // from no list in pg_publication_tables.attnames, yet it FREEZES the
    // published set — a later ADD COLUMN would silently vanish from the
    // stream. Only prattrs IS NULL proves there is no list.
    let members: Vec<(String, String, bool)> = client
        .query(
            "SELECT pt.schemaname::text, pt.tablename::text,
                    pr.prattrs IS NULL AND pr.prqual IS NULL
             FROM pg_publication_tables pt
             JOIN pg_publication p ON p.pubname = pt.pubname
             JOIN pg_namespace n ON n.nspname = pt.schemaname
             JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
             JOIN pg_publication_rel pr ON pr.prpubid = p.oid AND pr.prrelid = c.oid
             WHERE pt.pubname = $1",
            &[&publication],
        )
        .await?
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    if members.len() != tables.len() {
        return Ok(false);
    }
    Ok(tables.iter().all(|(schema, name)| {
        members
            .iter()
            .any(|(s, n, clean)| s == schema && n == name && *clean)
    }))
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
