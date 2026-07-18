//! The seeding-workflow runner: wires the pgshard-repl primitives —
//! exported-snapshot slot, filtered copy, pgoutput stream, keyspace filter,
//! exactly-once applier — into the long-running worker the operator drives
//! through StartWorkflow/StopWorkflow/WatchWorkflows during a reshard.
//!
//! The worker runs on the TARGET agent and pulls from the source (vreplication
//! style). Phases mirror the wire enum: INIT (recreate the slot, export its
//! snapshot) → COPY (per-table filtered seed under that snapshot) → STREAMING
//! (apply the slot's changes with the transactional checkpoint) until stopped
//! or failed. A restart re-seeds from scratch — the slot is dropped and
//! recreated, target tables are truncated, and the consumer's checkpoint row
//! is cleared — which is safe precisely because seeding targets are
//! non-serving shards; once a target serves, the reshard has cut over and no
//! seeding workflow may touch it again.
//!
//! Because seeding TRUNCATES target tables, every destructive step is behind a
//! preflight: the target database must carry the expected provenance marker,
//! the publication must cover exactly the mapped tables, and each table's
//! shard-key column must have a PostgreSQL type matching its declared wire
//! type and be covered by the replica identity. Applied transactions carry no
//! replication origin yet: M1 never runs forward and reverse workflows over
//! the same keyspace concurrently (reverse replication lands with the cutover
//! slice, which brings origins), and the registry admits only one running
//! workflow per target database.

use std::collections::HashMap;

use tokio::sync::{Mutex, watch};
use tokio_postgres::NoTls;

use pgshard_core::{KeyRange, ScalarType, shard_function};
use pgshard_proto::v1;
use pgshard_repl::apply::Applier;
use pgshard_repl::client::{Config as ReplConfig, ReplicationClient};
use pgshard_repl::copy::{CopySpec, copy_filtered};
use pgshard_repl::filter::{shard_key_index, tuple_in_range};
use pgshard_repl::pgoutput::{LogicalRepMsg, PgOutputDecoder};

/// How the runner reaches its databases. The target config points at the
/// LOCAL PostgreSQL this agent supervises (the worker overrides only the
/// database name per spec); the source credentials are this agent's
/// replication user — they come from agent configuration, never the wire.
#[derive(Clone)]
pub struct WorkflowConfig {
    pub target: tokio_postgres::Config,
    pub source_user: String,
    pub source_password: String,
}

/// One table's resolved seeding parameters.
struct TablePlan {
    schema: String,
    name: String,
    shard_key_column: String,
    shard_key_type: ScalarType,
}

/// The validated, owned form of a WorkflowSpec the runner executes.
struct RunPlan {
    id: String,
    source: ReplConfig,
    slot: String,
    publication: String,
    target_database: String,
    expect_provenance: String,
    tables: Vec<TablePlan>,
    range: KeyRange,
    hash_function: String,
}

pub struct WorkflowHandle {
    /// Serialized spec, for idempotent StartWorkflow retries.
    spec_bytes: Vec<u8>,
    /// The local database this workflow seeds — at most one RUNNING workflow
    /// may hold a target database (seeding truncates; two workers would
    /// destroy each other's copy).
    target_database: String,
    status: watch::Receiver<v1::WorkflowStatus>,
    stop: watch::Sender<bool>,
    join: tokio::task::JoinHandle<()>,
}

#[derive(Default)]
pub struct WorkflowRegistry {
    inner: Mutex<HashMap<String, WorkflowHandle>>,
}

#[derive(Debug, thiserror::Error)]
pub enum WorkflowError {
    #[error("invalid workflow spec: {0}")]
    Invalid(String),
    #[error("workflow {0} is already running with a different spec")]
    Conflict(String),
    #[error("workflow {0} is already seeding target database {1}")]
    TargetBusy(String, String),
    #[error("{0} is not implemented in M1: {1}")]
    Unimplemented(&'static str, String),
}

fn is_safe_ident(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn scalar_type(wire: &str) -> Result<ScalarType, WorkflowError> {
    match wire {
        "int" => Ok(ScalarType::Int),
        "text" => Ok(ScalarType::Text),
        "uuid" => Ok(ScalarType::Uuid),
        "bytea" => Ok(ScalarType::Bytea),
        other => Err(WorkflowError::Invalid(format!(
            "unknown shard key type {other:?} (expected int|text|uuid|bytea)"
        ))),
    }
}

/// Validate a spec into the owned plan the runner executes. Everything a
/// worker would only discover mid-run is rejected up front, loudly.
fn plan(spec: &v1::WorkflowSpec, config: &WorkflowConfig) -> Result<RunPlan, WorkflowError> {
    let invalid = |msg: String| Err(WorkflowError::Invalid(msg));
    if spec.id.is_empty() {
        return invalid("id is required".into());
    }
    // Only the reshard runner exists; UNSPECIFIED or DDL_SHADOW must not fall
    // through into a destructive re-seed.
    if spec.kind != v1::WorkflowKind::Reshard as i32 {
        return invalid(format!(
            "workflow kind {} is not runnable here (only WORKFLOW_KIND_RESHARD)",
            spec.kind
        ));
    }
    if !is_safe_ident(&spec.slot) {
        return invalid(format!("slot {:?} is not a safe identifier", spec.slot));
    }
    // The pgshard_ prefix reserves a slot namespace: INIT drops an inactive
    // slot by this name, and it must never be able to name another system's
    // slot (a legitimately disconnected consumer is also "inactive").
    if !spec.slot.starts_with("pgshard_") {
        return invalid(format!(
            "slot {:?} must carry the pgshard_ prefix (INIT drops a stale slot by name)",
            spec.slot
        ));
    }
    if !is_safe_ident(&spec.publication) {
        return invalid(format!(
            "publication {:?} is not a safe identifier",
            spec.publication
        ));
    }
    if spec.target_database.is_empty() || spec.target_database.len() > 63 {
        return invalid("target_database is required (and at most 63 bytes)".into());
    }
    if spec.expect_provenance.is_empty() {
        return invalid(
            "expect_provenance is required: seeding truncates the target, so the \
             target database's provenance marker must be verified first"
                .into(),
        );
    }
    let source = spec
        .source_primary
        .as_ref()
        .filter(|e| !e.host.is_empty() && e.port != 0 && !e.database.is_empty())
        .ok_or_else(|| {
            WorkflowError::Invalid("source_primary host/port/database are required".into())
        })?;
    if source.port > u16::MAX as u32 {
        return invalid(format!(
            "source_primary port {} is out of range",
            source.port
        ));
    }
    // Standby sourcing needs the anchor-slot machinery; be honest until then.
    if spec.source_policy != v1::SourcePolicy::Primary as i32
        && spec.source_policy != v1::SourcePolicy::Unspecified as i32
    {
        return Err(WorkflowError::Unimplemented(
            "source policy",
            "standby sourcing lands with anchor slots; use SOURCE_POLICY_PRIMARY".into(),
        ));
    }
    if spec.tables.is_empty() {
        return invalid("at least one table mapping is required".into());
    }
    let (range, hash) = match spec.filter.as_ref().and_then(|f| f.filter.as_ref()) {
        Some(v1::row_filter::Filter::All(true)) => (KeyRange::FULL, "xxhash64_v1".to_owned()),
        Some(v1::row_filter::Filter::KeyRange(kr)) => {
            let raw = kr
                .range
                .as_ref()
                .ok_or_else(|| WorkflowError::Invalid("key_range.range is required".into()))?;
            let range = KeyRange::new(raw.start, raw.end)
                .map_err(|e| WorkflowError::Invalid(e.to_string()))?;
            if shard_function(&kr.hash_function).is_err() {
                return invalid(format!("unknown hash function {:?}", kr.hash_function));
            }
            (range, kr.hash_function.clone())
        }
        _ => return invalid("filter must be set (all, or a key range)".into()),
    };
    let mut tables = Vec::with_capacity(spec.tables.len());
    for mapping in &spec.tables {
        let src = mapping
            .source
            .as_ref()
            .filter(|t| !t.schema.is_empty() && !t.name.is_empty())
            .ok_or_else(|| WorkflowError::Invalid("table source schema/name required".into()))?;
        if let Some(target) = &mapping.target
            && (target.schema != src.schema || target.name != src.name)
            && !(target.schema.is_empty() && target.name.is_empty())
        {
            return Err(WorkflowError::Unimplemented(
                "table renaming",
                format!("{}.{} must map to itself", src.schema, src.name),
            ));
        }
        if !mapping.column_map.is_empty() {
            return Err(WorkflowError::Unimplemented(
                "column mapping",
                format!("{}.{}", src.schema, src.name),
            ));
        }
        if mapping.shard_key_column.is_empty() {
            return invalid(format!(
                "table {}.{} has no shard key column",
                src.schema, src.name
            ));
        }
        tables.push(TablePlan {
            schema: src.schema.clone(),
            name: src.name.clone(),
            shard_key_column: mapping.shard_key_column.clone(),
            shard_key_type: scalar_type(&mapping.shard_key_type)?,
        });
    }
    Ok(RunPlan {
        id: spec.id.clone(),
        source: ReplConfig {
            host: source.host.clone(),
            port: source.port as u16,
            user: config.source_user.clone(),
            password: config.source_password.clone(),
            database: source.database.clone(),
        },
        slot: spec.slot.clone(),
        publication: spec.publication.clone(),
        target_database: spec.target_database.clone(),
        expect_provenance: spec.expect_provenance.clone(),
        tables,
        range,
        hash_function: hash,
    })
}

impl WorkflowRegistry {
    /// Start (or idempotently re-acknowledge) a workflow. A running workflow
    /// with the same id and byte-identical spec is a success; a different spec
    /// under a running id is a conflict. A stopped or failed workflow is
    /// replaced — the operator's retry — and re-seeds from scratch.
    pub async fn start(
        &self,
        spec: &v1::WorkflowSpec,
        config: &WorkflowConfig,
    ) -> Result<(), WorkflowError> {
        use prost::Message;
        let run = plan(spec, config)?;
        let spec_bytes = spec.encode_to_vec();
        let mut inner = self.inner.lock().await;
        if let Some(existing) = inner.get(&run.id)
            && !existing.join.is_finished()
        {
            if existing.spec_bytes == spec_bytes {
                return Ok(());
            }
            return Err(WorkflowError::Conflict(run.id));
        }
        // One running workflow per target database: seeding truncates, so a
        // second worker under a DIFFERENT id would destroy the first's copy.
        if let Some((other, _)) = inner.iter().find(|(id, h)| {
            **id != run.id && !h.join.is_finished() && h.target_database == run.target_database
        }) {
            return Err(WorkflowError::TargetBusy(
                other.clone(),
                run.target_database,
            ));
        }
        let (status_tx, status_rx) = watch::channel(v1::WorkflowStatus {
            id: run.id.clone(),
            phase: v1::WorkflowPhase::Init as i32,
            ..Default::default()
        });
        let (stop_tx, stop_rx) = watch::channel(false);
        let cfg = config.clone();
        let id = run.id.clone();
        let target_database = run.target_database.clone();
        let join = tokio::spawn(async move {
            if let Err(e) = run_workflow(run, cfg, status_tx.clone(), stop_rx).await {
                status_tx.send_modify(|s| {
                    s.phase = v1::WorkflowPhase::Error as i32;
                    s.error = e.to_string();
                });
            }
        });
        inner.insert(
            id,
            WorkflowHandle {
                spec_bytes,
                target_database,
                status: status_rx,
                stop: stop_tx,
                join,
            },
        );
        Ok(())
    }

    /// Signal a workflow to stop. Unknown ids succeed (idempotent).
    pub async fn stop(&self, id: &str) {
        let inner = self.inner.lock().await;
        if let Some(handle) = inner.get(id) {
            let _ = handle.stop.send(true);
        }
    }

    /// Current status of every workflow (or only `ids` when nonempty).
    pub async fn statuses(&self, ids: &[String]) -> Vec<v1::WorkflowStatus> {
        let inner = self.inner.lock().await;
        inner
            .iter()
            .filter(|(id, _)| ids.is_empty() || ids.contains(id))
            .map(|(_, h)| h.status.borrow().clone())
            .collect()
    }
}

async fn connect_sql(
    config: &tokio_postgres::Config,
    database: &str,
) -> anyhow::Result<tokio_postgres::Client> {
    let mut config = config.clone();
    config.dbname(database);
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::warn!(error = %e, "workflow connection closed");
        }
    });
    Ok(client)
}

async fn connect_source_sql(run: &RunPlan) -> anyhow::Result<tokio_postgres::Client> {
    let mut config = tokio_postgres::Config::new();
    config
        .host(&run.source.host)
        .port(run.source.port)
        .user(&run.source.user)
        .password(&run.source.password)
        .dbname(&run.source.database);
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::warn!(error = %e, "workflow source connection closed");
        }
    });
    Ok(client)
}

/// Owned per-relation filter info (Relation messages borrow frame data).
struct RelFilter {
    key_index: usize,
    key_type: ScalarType,
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// PostgreSQL type OIDs a declared shard-key type may hash soundly. bpchar
/// (1042) is deliberately absent from Text: char(n) pads with spaces, so the
/// same logical value hashes differently than its text form would.
fn allowed_key_oids(key_type: ScalarType) -> &'static [u32] {
    match key_type {
        ScalarType::Int => &[20, 21, 23],
        ScalarType::Text => &[25, 1043],
        ScalarType::Uuid => &[2950],
        ScalarType::Bytea => &[17],
    }
}

/// One preflighted table: its full column list (for the copy) after every
/// destructive-work precondition held.
struct TablePreflight {
    columns: Vec<String>,
}

/// Everything that must hold BEFORE any destructive step (truncate, slot
/// drop, checkpoint clear). A typo'd or misdeclared spec must fail here, with
/// the target untouched.
async fn preflight(
    run: &RunPlan,
    source_sql: &tokio_postgres::Client,
    target_sql: &tokio_postgres::Client,
) -> anyhow::Result<Vec<TablePreflight>> {
    // The target database must be the shard this workflow was aimed at: its
    // provenance marker is stamped by CreateDatabase and never by hand.
    let marker: Option<String> = target_sql
        .query_one(
            "SELECT shobj_description(oid, 'pg_database') FROM pg_database
             WHERE datname = current_database()",
            &[],
        )
        .await?
        .get(0);
    let expected = format!("pgshard-provenance:{}", run.expect_provenance);
    anyhow::ensure!(
        marker.as_deref() == Some(expected.as_str()),
        "target database {} carries provenance {:?}, expected {:?}: refusing to truncate a database this workflow does not own",
        run.target_database,
        marker.as_deref().unwrap_or("<none>"),
        expected,
    );

    // The publication must republish EVERYTHING: all DML kinds (a disabled
    // kind is silently omitted — TRUNCATE included, so a source truncate
    // reaches the stream and fails loudly instead of silently diverging),
    // no row filter (it would drop changes the copy included), and no column
    // list (it would transform rows). And it must cover EXACTLY the mapped
    // tables: a mapped table missing from it would be seeded once and then
    // silently never receive changes; an unmapped table in it would kill the
    // stream mid-flight after seeding.
    let pub_row = source_sql
        .query_opt(
            "SELECT pubinsert, pubupdate, pubdelete, pubtruncate
             FROM pg_publication WHERE pubname = $1",
            &[&run.publication],
        )
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "publication {} does not exist on the source",
                run.publication
            )
        })?;
    let (ins, upd, del, trunc): (bool, bool, bool, bool) = (
        pub_row.get(0),
        pub_row.get(1),
        pub_row.get(2),
        pub_row.get(3),
    );
    anyhow::ensure!(
        ins && upd && del && trunc,
        "publication {} does not publish all of insert/update/delete/truncate: disabled kinds would be silently omitted from the stream",
        run.publication
    );
    let published: Vec<(String, String, Option<Vec<String>>, Option<String>)> = source_sql
        .query(
            "SELECT schemaname::text, tablename::text, attnames::text[], rowfilter
             FROM pg_publication_tables WHERE pubname = $1",
            &[&run.publication],
        )
        .await?
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect();
    for table in &run.tables {
        anyhow::ensure!(
            published
                .iter()
                .any(|(s, n, ..)| *s == table.schema && *n == table.name),
            "table {}.{} is not in publication {}: it would be seeded once and then never receive changes",
            table.schema,
            table.name,
            run.publication
        );
    }
    for (s, n, _, rowfilter) in &published {
        anyhow::ensure!(
            run.tables.iter().any(|t| t.schema == *s && t.name == *n),
            "publication {} carries unmapped table {s}.{n}: the stream would fail after seeding",
            run.publication
        );
        anyhow::ensure!(
            rowfilter.is_none(),
            "publication {} filters rows of {s}.{n}: the stream would silently drop changes the seed copy included",
            run.publication
        );
    }

    let mut plans = Vec::with_capacity(run.tables.len());
    for table in &run.tables {
        let cols: Vec<(String, u32)> = source_sql
            .query(
                "SELECT a.attname::text, a.atttypid::oid FROM pg_attribute a
                 JOIN pg_class c ON c.oid = a.attrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1 AND c.relname = $2
                   AND a.attnum > 0 AND NOT a.attisdropped
                 ORDER BY a.attnum",
                &[&table.schema, &table.name],
            )
            .await?
            .into_iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
        anyhow::ensure!(
            !cols.is_empty(),
            "source table {}.{} not found",
            table.schema,
            table.name
        );
        // A column list on the publication would stream a transformed row
        // shape; the seed copy takes every column, so the stream must too.
        if let Some((.., Some(attnames), _)) = published
            .iter()
            .find(|(s, n, ..)| *s == table.schema && *n == table.name)
        {
            for (name, _) in &cols {
                anyhow::ensure!(
                    attnames.contains(name),
                    "publication {} omits column {name} of {}.{}: streamed rows would be silently transformed",
                    run.publication,
                    table.schema,
                    table.name
                );
            }
        }
        // Missing target tables/columns must surface before the slot is
        // replaced or anything is truncated, not one table into the seed.
        let target_cols: Vec<String> = target_sql
            .query(
                "SELECT a.attname::text FROM pg_attribute a
                 JOIN pg_class c ON c.oid = a.attrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1 AND c.relname = $2
                   AND a.attnum > 0 AND NOT a.attisdropped",
                &[&table.schema, &table.name],
            )
            .await?
            .into_iter()
            .map(|r| r.get(0))
            .collect();
        anyhow::ensure!(
            !target_cols.is_empty(),
            "target table {}.{} does not exist in {}",
            table.schema,
            table.name,
            run.target_database
        );
        for (name, _) in &cols {
            anyhow::ensure!(
                target_cols.contains(name),
                "target table {}.{} is missing column {name}",
                table.schema,
                table.name
            );
        }
        let key_oid = cols
            .iter()
            .find(|(name, _)| *name == table.shard_key_column)
            .map(|(_, oid)| *oid)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "shard key column {} not found in {}.{}",
                    table.shard_key_column,
                    table.schema,
                    table.name
                )
            })?;
        anyhow::ensure!(
            allowed_key_oids(table.shard_key_type).contains(&key_oid),
            "shard key {}.{}.{} has type oid {key_oid}, which cannot be hashed as declared {:?}: rows would land on the wrong shard",
            table.schema,
            table.name,
            table.shard_key_column,
            table.shard_key_type,
        );

        // The replica identity must cover the shard key: streamed UPDATE and
        // DELETE are filtered by it, and a shard-key change is only visible
        // when the identity carries the old key.
        let replident: i8 = source_sql
            .query_one(
                "SELECT c.relreplident FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1 AND c.relname = $2",
                &[&table.schema, &table.name],
            )
            .await?
            .get(0);
        match replident as u8 {
            b'd' | b'i' => {
                let use_replident = replident as u8 == b'i';
                // Only the first indnkeyatts index columns are identity KEY
                // columns; INCLUDE payload columns never appear in the old
                // tuple, so counting them would let a shard-key change slip
                // past the boundary check unseen.
                let identity_cols: Vec<String> = source_sql
                    .query(
                        "SELECT a.attname::text FROM pg_index i
                         JOIN pg_class c ON c.oid = i.indrelid
                         JOIN pg_namespace n ON n.oid = c.relnamespace
                         JOIN pg_attribute a
                           ON a.attrelid = c.oid
                          AND a.attnum = ANY((i.indkey)[0:i.indnkeyatts - 1])
                         WHERE n.nspname = $1 AND c.relname = $2
                           AND (CASE WHEN $3 THEN i.indisreplident ELSE i.indisprimary END)",
                        &[&table.schema, &table.name, &use_replident],
                    )
                    .await?
                    .into_iter()
                    .map(|r| r.get(0))
                    .collect();
                anyhow::ensure!(
                    identity_cols.contains(&table.shard_key_column),
                    "replica identity of {}.{} does not cover shard key {}: updates and deletes could not be range-filtered",
                    table.schema,
                    table.name,
                    table.shard_key_column
                );
            }
            // 'f' (FULL) is rejected too: the applier refuses FULL-identity
            // updates/deletes, so accepting it here would destructively
            // re-seed and then fail on the first mutation.
            other => anyhow::bail!(
                "table {}.{} has replica identity {:?} (need default or a replident index covering the shard key): updates and deletes could not be applied",
                table.schema,
                table.name,
                other as char
            ),
        }
        plans.push(TablePreflight {
            columns: cols.into_iter().map(|(name, _)| name).collect(),
        });
    }
    Ok(plans)
}

async fn run_workflow(
    run: RunPlan,
    config: WorkflowConfig,
    status: watch::Sender<v1::WorkflowStatus>,
    mut stop: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let shard_fn = shard_function(&run.hash_function).map_err(|e| anyhow::anyhow!("{e}"))?;

    // INIT: every destructive-work precondition is proven BEFORE anything is
    // dropped, cleared, or truncated.
    let source_sql = connect_source_sql(&run).await?;
    let target_sql = connect_sql(&config.target, &run.target_database).await?;
    let tables = preflight(&run, &source_sql, &target_sql).await?;

    // A pre-existing slot from an earlier attempt is dropped — this run
    // re-seeds from scratch — but only a slot that is provably ours: our
    // database, the pgoutput plugin, and (via plan()) the pgshard_ name
    // prefix. "Inactive" also describes a legitimately disconnected foreign
    // consumer, whose restart position must never be destroyed. An ACTIVE
    // slot means another worker is live and this one must not race it.
    if let Some(row) = source_sql
        .query_opt(
            "SELECT active, plugin::text, database::text
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&run.slot],
        )
        .await?
    {
        // plugin/database are NULL for physical slots — those are never ours.
        let (active, plugin, database): (bool, Option<String>, Option<String>) =
            (row.get(0), row.get(1), row.get(2));
        anyhow::ensure!(
            !active,
            "slot {} is still active on the source: another worker holds it",
            run.slot
        );
        anyhow::ensure!(
            plugin.as_deref() == Some("pgoutput")
                && database.as_deref() == Some(run.source.database.as_str()),
            "slot {} belongs to plugin {plugin:?} on database {database:?}, not this workflow: refusing to drop it",
            run.slot
        );
        source_sql
            .execute("SELECT pg_drop_replication_slot($1)", &[&run.slot])
            .await?;
    }
    let mut repl = ReplicationClient::connect(&run.source).await?;
    let snapshot = repl
        .create_logical_slot_exported(&run.slot, false)
        .await
        .map_err(|e| anyhow::anyhow!("creating slot {}: {e}", run.slot))?;

    // COPY: seed each table under the slot's snapshot. Targets are truncated
    // first and the consumer's checkpoint row is cleared, so a retried seed
    // is idempotent; both are safe only because the target is non-serving.
    status.send_modify(|s| {
        s.phase = v1::WorkflowPhase::Copy as i32;
        s.copy = Some(v1::CopyProgress {
            tables_total: run.tables.len() as u32,
            ..Default::default()
        });
    });
    // A re-seed is a fresh stream from a fresh slot: the consumer's old
    // checkpoint (if any) would fence every new apply as stale, so it is
    // cleared. Only a provably-missing progress table is ignorable (the
    // applier creates it); any other failure could leave a HIGHER stale
    // checkpoint alive, silently fencing out every commit of the new stream.
    if let Err(e) = target_sql
        .execute(
            "DELETE FROM pgshard.repl_progress WHERE consumer = $1",
            &[&run.id],
        )
        .await
    {
        let missing_table = e.code().is_some_and(|c| {
            *c == tokio_postgres::error::SqlState::UNDEFINED_TABLE
                || *c == tokio_postgres::error::SqlState::INVALID_SCHEMA_NAME
        });
        anyhow::ensure!(
            missing_table,
            "clearing stale checkpoint for {}: {e}",
            run.id
        );
    }
    let mut rows_copied = 0u64;
    for ((done, table), pre) in run.tables.iter().enumerate().zip(&tables) {
        if *stop.borrow() {
            status.send_modify(|s| s.phase = v1::WorkflowPhase::Stopped as i32);
            return Ok(());
        }
        let qualified = format!(
            "{}.{}",
            quote_ident(&table.schema),
            quote_ident(&table.name)
        );
        target_sql
            .batch_execute(&format!("TRUNCATE {qualified}"))
            .await?;
        let spec = CopySpec {
            schema: &table.schema,
            table: &table.name,
            columns: &pre.columns,
            shard_key_column: &table.shard_key_column,
            shard_key_type: table.shard_key_type,
            target_range: run.range,
        };
        rows_copied += copy_filtered(&source_sql, &target_sql, &snapshot, &spec, shard_fn).await?;
        status.send_modify(|s| {
            s.copy = Some(v1::CopyProgress {
                tables_total: run.tables.len() as u32,
                tables_done: done as u32 + 1,
                rows_copied,
            });
        });
    }

    // STREAMING: apply the slot's changes with the transactional checkpoint,
    // keeping only rows whose shard key falls in the range.
    status.send_modify(|s| s.phase = v1::WorkflowPhase::Streaming as i32);
    repl.start_replication(&run.slot, &run.publication).await?;
    let mut applier = Applier::new(
        connect_sql(&config.target, &run.target_database).await?,
        &*run.id,
    )
    .await
    .map_err(|e| anyhow::anyhow!("applier: {e}"))?;
    let mut decoder = PgOutputDecoder::new(4);
    let mut relations: HashMap<u32, RelFilter> = HashMap::new();
    loop {
        let frame = tokio::select! {
            frame = repl.next() => frame?,
            _ = stop.changed() => {
                if *stop.borrow() {
                    status.send_modify(|s| s.phase = v1::WorkflowPhase::Stopped as i32);
                    return Ok(());
                }
                continue;
            }
        };
        let Some(frame) = frame else {
            anyhow::bail!("replication stream ended");
        };
        let msg = decoder.decode(&frame.data)?;
        let committed = matches!(msg, LogicalRepMsg::Commit(_));
        match &msg {
            LogicalRepMsg::Relation(rel) => {
                let table = run
                    .tables
                    .iter()
                    .find(|t| t.schema == rel.namespace && t.name == rel.name)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "stream carries unmapped table {}.{}: publication wider than the spec",
                            rel.namespace,
                            rel.name
                        )
                    })?;
                let key_index = shard_key_index(rel, &table.shard_key_column)?;
                // A mid-stream ALTER re-sends the Relation; the preflighted
                // guarantees must hold for the LIVE table too — both the
                // shard key's type and the replica identity covering it (an
                // uncovered key change ships no old tuple, so a boundary
                // crossing would slip past the update check unseen).
                let key_oid = rel.columns[key_index].type_oid;
                anyhow::ensure!(
                    allowed_key_oids(table.shard_key_type).contains(&key_oid),
                    "stream reports shard key {}.{}.{} as type oid {key_oid}, which cannot be hashed as declared {:?}",
                    rel.namespace,
                    rel.name,
                    table.shard_key_column,
                    table.shard_key_type,
                );
                anyhow::ensure!(
                    matches!(rel.replica_identity, b'd' | b'i')
                        && rel.columns[key_index].flags & 1 == 1,
                    "replica identity of {}.{} no longer covers shard key {}: updates and deletes could not be range-filtered",
                    rel.namespace,
                    rel.name,
                    table.shard_key_column,
                );
                relations.insert(
                    rel.oid,
                    RelFilter {
                        key_index,
                        key_type: table.shard_key_type,
                    },
                );
                applier.handle(&msg).await?;
            }
            LogicalRepMsg::Insert(ins) => {
                let f = rel_filter(&relations, ins.rel_oid)?;
                if tuple_in_range(
                    &ins.new_tuple,
                    f.key_index,
                    f.key_type,
                    shard_fn,
                    &run.range,
                )? {
                    applier.handle(&msg).await?;
                }
            }
            LogicalRepMsg::Update(upd) => {
                let f = rel_filter(&relations, upd.rel_oid)?;
                let new_in = tuple_in_range(
                    &upd.new_tuple,
                    f.key_index,
                    f.key_type,
                    shard_fn,
                    &run.range,
                )?;
                // The router forbids shard-key updates, but triggers and
                // direct writes do not go through the router. The replica
                // identity covers the shard key (preflighted), so a key
                // change always ships the old identity tuple; a row crossing
                // the range boundary cannot be represented as an UPDATE on
                // one side (in→out leaves a stale target row, out→in updates
                // a row that is not there) — fail loudly.
                if let Some(old) = upd.key.as_ref().or(upd.old.as_ref()) {
                    let old_in =
                        tuple_in_range(old, f.key_index, f.key_type, shard_fn, &run.range)?;
                    anyhow::ensure!(
                        old_in == new_in,
                        "update moves a shard key across the target range boundary: the source row was written outside the router"
                    );
                }
                if new_in {
                    applier.handle(&msg).await?;
                }
            }
            LogicalRepMsg::Delete(del) => {
                let f = rel_filter(&relations, del.rel_oid)?;
                let tuple = del
                    .key
                    .as_ref()
                    .or(del.old.as_ref())
                    .ok_or_else(|| anyhow::anyhow!("delete without key or old tuple"))?;
                if tuple_in_range(tuple, f.key_index, f.key_type, shard_fn, &run.range)? {
                    applier.handle(&msg).await?;
                }
            }
            LogicalRepMsg::Truncate(_) => {
                // A truncate cannot be range-filtered; replaying it would wipe
                // the target's whole keyspace slice for rows outside the
                // source's responsibility. Fail loudly rather than guess.
                anyhow::bail!("TRUNCATE on a seeded table is not supported during seeding");
            }
            _ => applier.handle(&msg).await?,
        }
        if committed {
            let ack = applier.ack_lsn();
            repl.confirm(ack);
            repl.send_standby_status().await?;
            status.send_modify(|s| {
                s.applied_lsn = Some(v1::Lsn { value: ack.0 });
            });
        }
    }
}

fn rel_filter(relations: &HashMap<u32, RelFilter>, oid: u32) -> anyhow::Result<&RelFilter> {
    relations
        .get(&oid)
        .ok_or_else(|| anyhow::anyhow!("row for unknown relation oid {oid}"))
}
