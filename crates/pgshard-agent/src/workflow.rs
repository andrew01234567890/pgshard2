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

impl WorkflowHandle {
    /// An unfinished task holds its claims — replication session, slot, and
    /// target database — until it has actually terminated; releasing on the
    /// terminal STATUS alone would let a replacement race the old worker's
    /// teardown. A winding-down handle answers Stopping (retryable) instead.
    fn winding_down(&self) -> bool {
        let phase = self.status.borrow().phase;
        phase == v1::WorkflowPhase::Error as i32
            || phase == v1::WorkflowPhase::Stopped as i32
            || *self.stop.borrow()
    }
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
    #[error("workflow {0} is stopping; retry once it has terminated")]
    Stopping(String),
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
    // Duplicate mappings would seed under the LAST one (each truncates) but
    // stream under the FIRST — two filters for one table.
    for (i, m) in spec.tables.iter().enumerate() {
        if let Some(a) = &m.source
            && spec.tables[..i].iter().any(|prev| {
                prev.source
                    .as_ref()
                    .is_some_and(|b| b.schema == a.schema && b.name == a.name)
            })
        {
            return invalid(format!("table {}.{} is mapped twice", a.schema, a.name));
        }
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
            // Terminal status or a signaled stop means the old worker is
            // winding down but may not have released its sessions yet;
            // acknowledging a byte-identical restart now would either return
            // a success with no worker behind it or race the teardown.
            if existing.winding_down() {
                return Err(WorkflowError::Stopping(run.id));
            }
            if existing.spec_bytes == spec_bytes {
                return Ok(());
            }
            return Err(WorkflowError::Conflict(run.id));
        }
        // One running workflow per target database: seeding truncates, so a
        // second worker under a DIFFERENT id would destroy the first's copy.
        if let Some((other, holder)) = inner.iter().find(|(id, h)| {
            **id != run.id && !h.join.is_finished() && h.target_database == run.target_database
        }) {
            // A winding-down holder releases the target momentarily — that is
            // the retryable Stopping case, not the persistent TargetBusy one.
            if holder.winding_down() {
                return Err(WorkflowError::Stopping(other.clone()));
            }
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
        .dbname(&run.source.database)
        // The cutover write-fence spares application_names starting pgshard_:
        // this is a READER (catalog polls), not a writer, so terminating it
        // would only make the workflow error and block the cutover.
        .application_name("pgshard_seed_reader");
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

/// One publication member, read from the UNDERLYING catalog rows: an
/// explicit column list that names every current column is invisible in
/// pg_publication_tables.attnames yet freezes the published set (a later
/// ADD COLUMN silently vanishes from the stream), so only
/// pg_publication_rel.prattrs decides `has_column_list`.
#[derive(PartialEq)]
struct PublishedTable {
    schema: String,
    name: String,
    /// The currently-published column names — part of the drift shape: with
    /// no explicit list, ADD COLUMN silently expands the publication without
    /// touching any versioned catalog row, and only this list betrays it.
    columns: Vec<String>,
    has_column_list: bool,
    has_row_filter: bool,
}

/// The validated shape of the publication. Captured at preflight and
/// re-fetched during streaming: ALTER PUBLICATION can disable a DML kind or
/// add a row filter mid-stream, after which pgoutput silently omits changes
/// with no Relation message to betray it — the only defense is to notice the
/// catalog changed and fail the workflow loudly (a retry re-seeds from
/// scratch, so omitted changes can never be served).
#[derive(PartialEq)]
struct PublicationShape {
    insert: bool,
    update: bool,
    delete: bool,
    truncate: bool,
    via_partition_root: bool,
    publishes_generated: bool,
    all_tables: bool,
    /// Row versions (catalog kind, oid, xmin) of the pg_publication row and
    /// every pg_publication_rel / pg_publication_namespace row. A sampled
    /// shape comparison alone would miss a toggle-away-and-back between
    /// polls; ANY ALTER PUBLICATION rewrites at least one of these rows, so
    /// the version set cannot match unless the publication was untouched
    /// the whole time (xid reuse is separately fenced by the wrap horizon).
    versions: Vec<(i32, u32, String)>,
    tables: Vec<PublishedTable>,
}

/// The publication's validated shape plus the exact xid horizon fencing xmin
/// reuse: 32-bit xmins recur when the counter wraps back to a captured value,
/// and freezing PRESERVES old xmins — a row frozen nearly a full wrap ago can
/// be only a few xids ahead of the counter. `horizon` is the minimum forward
/// distance from the baseline xid to ANY captured xmin; while fewer xids than
/// that have been consumed, no new assignment can equal a captured value, so
/// version equality is proof the row was never rewritten.
struct PublicationBaseline {
    shape: PublicationShape,
    xid: u64,
    horizon: u64,
}

/// Forward-recurrence horizon: min over captured xmins of the xid distance
/// until the counter reaches that value again. Special xids (< 3: bootstrap,
/// frozen) are never assigned again and do not bound the horizon.
fn xid_recurrence_horizon(base: u64, versions: &[(i32, u32, String)]) -> anyhow::Result<u64> {
    let base32 = base as u32;
    let mut horizon = u64::from(u32::MAX) + 1;
    for (_, _, xmin) in versions {
        let v: u32 = xmin
            .parse()
            .map_err(|_| anyhow::anyhow!("unparseable catalog xmin {xmin:?}"))?;
        if v < 3 {
            continue;
        }
        let d = u64::from(v.wrapping_sub(base32));
        if d > 0 {
            horizon = horizon.min(d);
        }
    }
    Ok(horizon)
}

async fn fetch_publication(
    source_sql: &tokio_postgres::Client,
    publication: &str,
) -> anyhow::Result<PublicationBaseline> {
    let row = source_sql
        .query_opt(
            "SELECT pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot,
                    pubgencols::text <> 'n', puballtables,
                    pg_current_xact_id()::text
             FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("publication {publication} does not exist on the source"))?;
    let xid: u64 = row.get::<_, String>(7).parse()?;
    let versions: Vec<(i32, u32, String)> = source_sql
        .query(
            "SELECT 0, oid, xmin::text FROM pg_publication WHERE pubname = $1
             UNION ALL
             SELECT 1, pr.oid, pr.xmin::text FROM pg_publication_rel pr
             JOIN pg_publication p ON p.oid = pr.prpubid
             WHERE p.pubname = $1
             UNION ALL
             SELECT 2, pn.oid, pn.xmin::text FROM pg_publication_namespace pn
             JOIN pg_publication p ON p.oid = pn.pnpubid
             WHERE p.pubname = $1
             ORDER BY 1, 2",
            &[&publication],
        )
        .await?
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    let tables = source_sql
        .query(
            "SELECT pt.schemaname::text, pt.tablename::text, pt.attnames::text[],
                    pr.prattrs IS NOT NULL, pr.prqual IS NOT NULL
             FROM pg_publication_tables pt
             JOIN pg_publication p ON p.pubname = pt.pubname
             JOIN pg_namespace n ON n.nspname = pt.schemaname
             JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
             LEFT JOIN pg_publication_rel pr
               ON pr.prpubid = p.oid AND pr.prrelid = c.oid
             WHERE pt.pubname = $1
             ORDER BY pt.schemaname, pt.tablename",
            &[&publication],
        )
        .await?
        .into_iter()
        .map(|r| PublishedTable {
            schema: r.get(0),
            name: r.get(1),
            columns: r.get::<_, Option<Vec<String>>>(2).unwrap_or_default(),
            has_column_list: r.get::<_, Option<bool>>(3).unwrap_or(false),
            has_row_filter: r.get::<_, Option<bool>>(4).unwrap_or(false),
        })
        .collect();
    let horizon = xid_recurrence_horizon(xid, &versions)?;
    Ok(PublicationBaseline {
        shape: PublicationShape {
            insert: row.get(0),
            update: row.get(1),
            delete: row.get(2),
            truncate: row.get(3),
            via_partition_root: row.get(4),
            publishes_generated: row.get(5),
            all_tables: row.get(6),
            versions,
            tables,
        },
        xid,
        horizon,
    })
}

/// Everything that must hold BEFORE any destructive step (truncate, slot
/// drop, checkpoint clear). A typo'd or misdeclared spec must fail here, with
/// the target untouched.
async fn preflight(
    run: &RunPlan,
    source_sql: &tokio_postgres::Client,
    target_sql: &tokio_postgres::Client,
) -> anyhow::Result<(Vec<TablePreflight>, PublicationBaseline)> {
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
    let baseline = fetch_publication(source_sql, &run.publication).await?;
    let shape = &baseline.shape;
    anyhow::ensure!(
        shape.insert && shape.update && shape.delete && shape.truncate,
        "publication {} does not publish all of insert/update/delete/truncate: disabled kinds would be silently omitted from the stream",
        run.publication
    );
    // publish_via_partition_root publishes the ROOT in the catalog while
    // pgoutput still announces leaf relations (unmapped here), and a direct
    // leaf TRUNCATE is not published at all — the loud-failure guarantees do
    // not hold. Partitioned sources are out of M1 scope.
    anyhow::ensure!(
        !shape.via_partition_root,
        "publication {} sets publish_via_partition_root, which this runner cannot stream soundly",
        run.publication
    );
    anyhow::ensure!(
        !shape.publishes_generated,
        "publication {} publishes generated columns, which this runner cannot stream soundly",
        run.publication
    );
    // FOR ALL TABLES and TABLES IN SCHEMA memberships expand DYNAMICALLY —
    // a mapped table can be dropped and recreated between polls with the
    // expanded list returning to its original value and (for all-tables) no
    // catalog row to version. Only explicit FOR TABLE publications give
    // every member a versioned pg_publication_rel row.
    anyhow::ensure!(
        !shape.all_tables,
        "publication {} is FOR ALL TABLES; its membership cannot be pinned — use an explicit FOR TABLE publication",
        run.publication
    );
    anyhow::ensure!(
        shape.versions.iter().all(|(kind, ..)| *kind != 2),
        "publication {} publishes TABLES IN SCHEMA; its membership cannot be pinned — use an explicit FOR TABLE publication",
        run.publication
    );
    let published = &shape.tables;
    for table in &run.tables {
        anyhow::ensure!(
            published
                .iter()
                .any(|p| p.schema == table.schema && p.name == table.name),
            "table {}.{} is not in publication {}: it would be seeded once and then never receive changes",
            table.schema,
            table.name,
            run.publication
        );
    }
    for p in published {
        anyhow::ensure!(
            run.tables
                .iter()
                .any(|t| t.schema == p.schema && t.name == p.name),
            "publication {} carries unmapped table {}.{}: the stream would fail after seeding",
            run.publication,
            p.schema,
            p.name
        );
        anyhow::ensure!(
            !p.has_row_filter,
            "publication {} filters rows of {}.{}: the stream would silently drop changes the seed copy included",
            run.publication,
            p.schema,
            p.name
        );
        anyhow::ensure!(
            !p.has_column_list,
            "publication {} sets a column list for {}.{}: even a currently-complete list freezes the published set, so a later ADD COLUMN would silently vanish from the stream",
            run.publication,
            p.schema,
            p.name
        );
    }

    let mut plans = Vec::with_capacity(run.tables.len());
    for table in &run.tables {
        let src = table_columns(source_sql, &table.schema, &table.name).await?;
        anyhow::ensure!(
            !src.is_empty(),
            "source table {}.{} not found",
            table.schema,
            table.name
        );
        // Only plain tables: pgoutput announces partition LEAVES while the
        // catalog publishes the root, and a direct leaf TRUNCATE is not
        // published — the mapping and loud-failure guarantees break.
        anyhow::ensure!(
            src[0].relkind as u8 == b'r',
            "source table {}.{} is not a plain table (relkind {:?}); partitioned sources are not supported",
            table.schema,
            table.name,
            src[0].relkind as u8 as char
        );
        // Generated columns are rejected outright for M1: pgoutput does not
        // publish them and COPY cannot insert them, and PostgreSQL offers no
        // sound cross-database comparison of the generation expressions —
        // differing expressions would silently diverge.
        for c in &src {
            anyhow::ensure!(
                !c.generated,
                "column {} of {}.{} is generated; generated columns cannot be streamed soundly",
                c.name,
                table.schema,
                table.name
            );
        }
        // The target table must be COPY- and apply-compatible before the slot
        // is replaced or anything is truncated, not one table into the seed.
        // The contract is EXACT schema equivalence — reshard targets are
        // clones — because anything looser reintroduces silent divergence:
        // an extra target column's defaults or constraints, a same-named
        // column of another type, or generation expressions that differ.
        let tgt = table_columns(target_sql, &table.schema, &table.name).await?;
        anyhow::ensure!(
            !tgt.is_empty(),
            "target table {}.{} does not exist in {}",
            table.schema,
            table.name,
            run.target_database
        );
        anyhow::ensure!(
            tgt[0].relkind as u8 == b'r',
            "target table {}.{} is not a plain table",
            table.schema,
            table.name
        );
        for t in &tgt {
            anyhow::ensure!(
                src.iter().any(|c| c.name == t.name),
                "target table {}.{} has extra column {}: copied and streamed rows do not carry it",
                table.schema,
                table.name,
                t.name
            );
        }
        for c in &src {
            let t = tgt.iter().find(|t| t.name == c.name).ok_or_else(|| {
                anyhow::anyhow!(
                    "target table {}.{} is missing column {}",
                    table.schema,
                    table.name,
                    c.name
                )
            })?;
            // Type OIDs are database-local, so equality is only meaningful
            // for builtin pg_catalog types (stable OIDs); custom types are
            // out of M1 scope.
            anyhow::ensure!(
                c.type_oid < 16384 && t.type_oid < 16384,
                "column {} of {}.{} has a non-builtin type; custom types cannot be compared across databases and are not supported",
                c.name,
                table.schema,
                table.name
            );
            anyhow::ensure!(
                t.type_oid == c.type_oid && t.typmod == c.typmod,
                "column {} of {}.{} has a different type on the target: copied and streamed values could be transformed or rejected",
                c.name,
                table.schema,
                table.name
            );
            anyhow::ensure!(
                !t.generated,
                "column {} of {}.{} is generated on the target; copied and streamed values would be rejected",
                c.name,
                table.schema,
                table.name
            );
            // GENERATED ALWAYS identity refuses explicit values: COPY would
            // pass (COPY overrides), but every streamed INSERT would fail.
            anyhow::ensure!(
                t.identity as u8 != b'a',
                "column {} of {}.{} is GENERATED ALWAYS AS IDENTITY on the target: streamed inserts carrying explicit values would be rejected",
                c.name,
                table.schema,
                table.name
            );
        }
        let key = src
            .iter()
            .find(|c| c.name == table.shard_key_column)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "shard key column {} not found in {}.{}",
                    table.shard_key_column,
                    table.schema,
                    table.name
                )
            })?;
        let key_oid = key.type_oid;
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
            columns: src.iter().map(|c| c.name.clone()).collect(),
        });
    }
    Ok((plans, baseline))
}

/// One column as both sides' compatibility check needs it.
struct SqlColumn {
    name: String,
    type_oid: u32,
    typmod: i32,
    generated: bool,
    /// attidentity: 0 none, 'a' GENERATED ALWAYS, 'd' BY DEFAULT.
    identity: i8,
    relkind: i8,
}

async fn table_columns(
    sql: &tokio_postgres::Client,
    schema: &str,
    name: &str,
) -> anyhow::Result<Vec<SqlColumn>> {
    Ok(sql
        .query(
            "SELECT a.attname::text, a.atttypid::oid, a.atttypmod,
                    a.attgenerated <> '', a.attidentity, c.relkind
             FROM pg_attribute a
             JOIN pg_class c ON c.oid = a.attrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2
               AND a.attnum > 0 AND NOT a.attisdropped
             ORDER BY a.attnum",
            &[&schema, &name],
        )
        .await?
        .into_iter()
        .map(|r| SqlColumn {
            name: r.get(0),
            type_oid: r.get(1),
            typmod: r.get(2),
            generated: r.get(3),
            identity: r.get(4),
            relkind: r.get(5),
        })
        .collect())
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
    let (tables, publication) = preflight(&run, &source_sql, &target_sql).await?;

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
    let (snapshot, consistent_point) = repl
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
    // keeping only rows whose shard key falls in the range. The applied
    // watermark STARTS at the slot's consistent point: everything at or
    // before it arrived via the snapshot copy, so an idle source (no commits
    // to stream) still reports an honest, convergeable position.
    repl.start_replication(&run.slot, &run.publication).await?;
    let mut applier = Applier::new(
        connect_sql(&config.target, &run.target_database).await?,
        &*run.id,
    )
    .await
    .map_err(|e| anyhow::anyhow!("applier: {e}"))?;
    // Publish STREAMING (and the initial watermark) only once the stream AND
    // the applier actually exist: publishing earlier would let the operator
    // read a caught-up-looking status with no live consumer behind it.
    status.send_modify(|s| {
        s.phase = v1::WorkflowPhase::Streaming as i32;
        s.applied_lsn = Some(v1::Lsn {
            value: consistent_point.0,
        });
    });
    let mut decoder = PgOutputDecoder::new(4);
    let mut relations: HashMap<u32, RelFilter> = HashMap::new();
    // The pgshard journal message decoded inside the CURRENT transaction, if
    // any; acknowledged (published as journal_lsn) only once that
    // transaction COMMITS — the barrier is real only when durably decoded.
    let mut pending_journal: Option<u64> = None;
    // ALTER PUBLICATION mid-stream makes pgoutput silently omit changes with
    // no Relation message to betray it; the only defense is to poll the
    // catalog and fail loudly on drift (bounded by this interval — a failed
    // workflow re-seeds from scratch, so omitted changes are never served).
    let mut pub_recheck = tokio::time::interval(std::time::Duration::from_secs(5));
    pub_recheck.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let frame = tokio::select! {
            frame = repl.next() => frame?,
            _ = pub_recheck.tick() => {
                let now = fetch_publication(&source_sql, &run.publication).await?;
                anyhow::ensure!(
                    now.shape == publication.shape,
                    "publication {} changed while streaming: the stream may have silently omitted changes",
                    run.publication
                );
                anyhow::ensure!(
                    now.xid.saturating_sub(publication.xid) < publication.horizon,
                    "the source has consumed enough xids since this workflow began that a captured catalog row version could recur; the versions can no longer be trusted"
                );
                continue;
            }
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
        if let LogicalRepMsg::Message(m) = &msg
            && m.prefix == "pgshard"
            && m.transactional
        {
            // The cutover freeze barrier: its OWN WAL position is the value
            // the operator compares against the emitted journal's LSN — an
            // explicit database-local acknowledgement, immune to WAL from
            // other databases (which never produces frames on this slot).
            pending_journal = Some(m.lsn.0);
        }
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
            let journal = pending_journal.take();
            status.send_modify(|s| {
                s.applied_lsn = Some(v1::Lsn { value: ack.0 });
                if let Some(j) = journal {
                    s.journal_lsn = Some(v1::Lsn { value: j });
                }
            });
        }
    }
}

fn rel_filter(relations: &HashMap<u32, RelFilter>, oid: u32) -> anyhow::Result<&RelFilter> {
    relations
        .get(&oid)
        .ok_or_else(|| anyhow::anyhow!("row for unknown relation oid {oid}"))
}

#[cfg(test)]
mod tests {
    use super::xid_recurrence_horizon;

    fn v(xmins: &[&str]) -> Vec<(i32, u32, String)> {
        xmins.iter().map(|x| (0, 1, x.to_string())).collect()
    }

    const FULL_CYCLE: u64 = 1 << 32;

    #[test]
    fn the_nearest_forward_xmin_bounds_the_horizon() {
        let h = xid_recurrence_horizon(1000, &v(&["1010", "500000"])).unwrap();
        assert_eq!(h, 10);
    }

    #[test]
    fn an_xmin_behind_the_counter_recurs_only_after_wrapping_forward() {
        let h = xid_recurrence_horizon(1000, &v(&["990"])).unwrap();
        assert_eq!(h, FULL_CYCLE - 10);
    }

    #[test]
    fn an_xmin_equal_to_the_baseline_means_a_full_cycle() {
        let h = xid_recurrence_horizon(1000, &v(&["1000"])).unwrap();
        assert_eq!(h, FULL_CYCLE);
    }

    #[test]
    fn special_xids_never_recur_and_do_not_bound_the_horizon() {
        let h = xid_recurrence_horizon(1000, &v(&["0", "1", "2"])).unwrap();
        assert_eq!(h, FULL_CYCLE);
    }

    #[test]
    fn a_64_bit_baseline_bounds_by_its_low_32_bits() {
        let base = (7 << 32) | 1000;
        let h = xid_recurrence_horizon(base, &v(&["1010"])).unwrap();
        assert_eq!(h, 10);
    }

    #[test]
    fn an_unparseable_xmin_is_an_error() {
        assert!(xid_recurrence_horizon(1000, &v(&["not-an-xid"])).is_err());
    }
}
