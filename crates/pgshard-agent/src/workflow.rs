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
    tables: Vec<TablePlan>,
    range: KeyRange,
    hash_function: String,
}

pub struct WorkflowHandle {
    /// Serialized spec, for idempotent StartWorkflow retries.
    spec_bytes: Vec<u8>,
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
    if !is_safe_ident(&spec.slot) {
        return invalid(format!("slot {:?} is not a safe identifier", spec.slot));
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
    let source = spec
        .source_primary
        .as_ref()
        .filter(|e| !e.host.is_empty() && e.port != 0 && !e.database.is_empty())
        .ok_or_else(|| {
            WorkflowError::Invalid("source_primary host/port/database are required".into())
        })?;
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
        let (status_tx, status_rx) = watch::channel(v1::WorkflowStatus {
            id: run.id.clone(),
            phase: v1::WorkflowPhase::Init as i32,
            ..Default::default()
        });
        let (stop_tx, stop_rx) = watch::channel(false);
        let cfg = config.clone();
        let id = run.id.clone();
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

async fn run_workflow(
    run: RunPlan,
    config: WorkflowConfig,
    status: watch::Sender<v1::WorkflowStatus>,
    mut stop: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let shard_fn = shard_function(&run.hash_function).map_err(|e| anyhow::anyhow!("{e}"))?;

    // INIT: fresh slot + exported snapshot. A pre-existing slot from an
    // earlier attempt is dropped — this run re-seeds from scratch — but an
    // ACTIVE slot means another worker is live and this one must not race it.
    let source_sql = connect_source_sql(&run).await?;
    source_sql
        .execute(
            "SELECT pg_drop_replication_slot(slot_name)
             FROM pg_replication_slots WHERE slot_name = $1 AND active = false",
            &[&run.slot],
        )
        .await?;
    let active: bool = source_sql
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&run.slot],
        )
        .await?
        .get(0);
    anyhow::ensure!(
        !active,
        "slot {} is still active on the source: another worker holds it",
        run.slot
    );
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
    let target_sql = connect_sql(&config.target, &run.target_database).await?;
    // A re-seed is a fresh stream from a fresh slot: the consumer's old
    // checkpoint (if any) would fence every new apply as stale, so it is
    // cleared. The table may not exist yet — the applier creates it.
    let _ = target_sql
        .execute(
            "DELETE FROM pgshard.repl_progress WHERE consumer = $1",
            &[&run.id],
        )
        .await;
    let mut rows_copied = 0u64;
    for (done, table) in run.tables.iter().enumerate() {
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
        let columns: Vec<String> = source_sql
            .query(
                "SELECT column_name FROM information_schema.columns
                 WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
                &[&table.schema, &table.name],
            )
            .await?
            .into_iter()
            .map(|r| r.get::<_, String>(0))
            .collect();
        anyhow::ensure!(
            !columns.is_empty(),
            "source table {}.{} not found",
            table.schema,
            table.name
        );
        let spec = CopySpec {
            schema: &table.schema,
            table: &table.name,
            columns: &columns,
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
                relations.insert(
                    rel.oid,
                    RelFilter {
                        key_index: shard_key_index(rel, &table.shard_key_column)?,
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
                // The shard key is immutable, so the new tuple decides.
                let f = rel_filter(&relations, upd.rel_oid)?;
                if tuple_in_range(
                    &upd.new_tuple,
                    f.key_index,
                    f.key_type,
                    shard_fn,
                    &run.range,
                )? {
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
