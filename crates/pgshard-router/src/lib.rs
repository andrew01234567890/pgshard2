//! The router's routing layer: turn a compiled [`Topology`] into an executable
//! [`Router`], then route SQL to concrete [`Target`]s (a node endpoint plus the
//! shard's database, since each shard is its own database on a shared node).
//!
//! This is the bridge between the pieces that already exist — the topology watch
//! ([`pgshard_topo`]), the SQL parser ([`pgshard_sql`]), and the planner
//! ([`pgshard_plan`]) — and the wire session loop that will call it. [`Router`]
//! is an immutable snapshot built from one epoch's topology; the session layer
//! swaps a new one in when a higher epoch is applied.
//!
//! # Scope
//!
//! The routable shard set is the topology's `Serving` shards, which must
//! partition the keyspace (they do at steady state; a transient reshard state
//! that does not is a build error, so the caller keeps the previous snapshot).
//! Writes and single-shard reads go to a shard's primary; a scatter read fans to
//! the primaries of the covered shards (M1 does not route reads to replicas
//! through the router). A shard with no current primary (mid-failover) resolves
//! to [`Route::Unavailable`] rather than a wrong endpoint.

pub mod wire;

use std::collections::BTreeMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use pgshard_core::{KeyRange, SequenceBinding, TableDef, TableName, VSchema, VSchemaError};
use pgshard_plan::{Parameterized, Plan, ShardCatalog, ShardId};
use pgshard_sql::SqlError;
use pgshard_topo::{ShardState, TableType, Topology};
use tokio::sync::watch;

pub use pgshard_topo::Instance as Endpoint;

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("sharded table {0} has no shard key column")]
    MissingShardKey(TableName),
    #[error("duplicate serving shard name {0:?}")]
    DuplicateShard(String),
    #[error(transparent)]
    VSchema(#[from] VSchemaError),
    #[error("serving shards do not partition the keyspace: {0}")]
    Partition(#[from] pgshard_core::PartitionError),
}

/// A concrete place to run a statement: the node to connect to and the Postgres
/// database on it. Each shard is its own database on a (possibly shared) node,
/// so the node endpoint alone is not enough to reach the shard's data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub endpoint: Endpoint,
    /// The database to connect to — the shard's name (a node hosts one database
    /// per placed shard).
    pub database: String,
}

/// Where the router should send one statement, resolved to concrete targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// Send to this one shard database.
    Shard(Target),
    /// Fan a read out to these shard databases and merge.
    Scatter(Vec<Target>),
    /// Run on every shard database (DDL).
    Broadcast(Vec<Target>),
    /// The unsharded system database. Carries the node endpoint; the system
    /// database name is not yet in the topology (a follow-up), so the wire layer
    /// supplies it.
    System(Endpoint),
    /// The session layer handles it (SET/SHOW/txn/tableless).
    Local,
    /// Routing needs bind parameters; the executor finishes at Bind.
    NeedsBind(Parameterized),
    /// A targeted shard currently has no primary (failing over); the router must
    /// not guess an endpoint.
    Unavailable(String),
    /// The statement cannot be routed; return this SQLSTATE.
    Reject { code: &'static str, reason: String },
}

/// An immutable routing snapshot for one topology epoch.
pub struct Router {
    epoch: u64,
    vschema: VSchema,
    catalog: ShardCatalog,
    /// Every serving shard's current primary, if it has one.
    primaries: BTreeMap<ShardId, Option<Endpoint>>,
    /// The system (unsharded) database endpoint, if known.
    system: Option<Endpoint>,
}

impl Router {
    /// Build a router from a compiled topology. Fails if a sharded table lacks a
    /// shard key, the hash function is unknown, or the serving shards do not
    /// partition the keyspace.
    pub fn build(topo: &Topology) -> Result<Self, BuildError> {
        let vschema = build_vschema(topo)?;

        let mut serving: Vec<(KeyRange, ShardId)> = Vec::new();
        let mut primaries = BTreeMap::new();
        for shard in &topo.shards {
            if shard.state != ShardState::Serving {
                continue;
            }
            let id = ShardId::new(shard.name.clone());
            // Duplicate names would collapse in `primaries` (last write wins)
            // while both survive in the catalog, silently mis-routing one range.
            if primaries
                .insert(id.clone(), shard.primary.clone())
                .is_some()
            {
                return Err(BuildError::DuplicateShard(shard.name.clone()));
            }
            serving.push((shard.key_range, id));
        }
        // `ShardCatalog::new` checks contiguity in input order; sort first so a
        // valid-but-unsorted shard list (which the topology layer accepts) is not
        // spuriously rejected.
        serving.sort_by_key(|(kr, _)| kr.start());
        let catalog = ShardCatalog::new(serving)?;

        Ok(Router {
            epoch: topo.epoch,
            vschema,
            catalog,
            primaries,
            system: topo.sequence_endpoint.clone(),
        })
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Any serving shard with a primary, as a connection [`Target`]. Used to run
    /// tableless/session statements (`SELECT 1`, `SHOW`) that route nowhere in
    /// particular — they run correctly on any shard database.
    pub fn any_shard_target(&self) -> Option<Target> {
        self.primaries.iter().find_map(|(id, primary)| {
            primary.as_ref().map(|ep| Target {
                endpoint: ep.clone(),
                database: id.0.clone(),
            })
        })
    }

    /// Route a (possibly multi-statement) query, one [`Route`] per statement.
    pub fn route(&self, sql: &str) -> Result<Vec<Route>, SqlError> {
        let parsed = pgshard_sql::parse(sql)?;
        let plans = pgshard_plan::plan_all(&parsed, &self.vschema, &self.catalog);
        Ok(plans.into_iter().map(|p| self.resolve(p)).collect())
    }

    fn resolve(&self, plan: Plan) -> Route {
        match plan {
            Plan::SingleShard(id) => match self.target(&id) {
                Ok(t) => Route::Shard(t),
                Err(unavail) => unavail,
            },
            Plan::Scatter(ids) => self.resolve_many(ids, Route::Scatter),
            Plan::Broadcast(ids) => self.resolve_many(ids, Route::Broadcast),
            Plan::Unsharded => match &self.system {
                Some(ep) => Route::System(ep.clone()),
                None => Route::Unavailable("system database has no endpoint".to_owned()),
            },
            Plan::RouterLocal => Route::Local,
            Plan::Parameterized(p) => Route::NeedsBind(p),
            Plan::Reject { code, reason } => Route::Reject { code, reason },
        }
    }

    /// The connection target for `id` (its primary node + its database), or a
    /// [`Route::Unavailable`] describing why it cannot be reached.
    fn target(&self, id: &ShardId) -> Result<Target, Route> {
        match self.primaries.get(id) {
            Some(Some(ep)) => Ok(Target {
                endpoint: ep.clone(),
                database: id.0.clone(),
            }),
            Some(None) => Err(Route::Unavailable(format!("shard {id} has no primary"))),
            // The planner only names shards from this catalog, so a miss is a bug
            // rather than a routing outcome — surface it as unavailable.
            None => Err(Route::Unavailable(format!(
                "shard {id} is not in the topology"
            ))),
        }
    }

    fn resolve_many(&self, ids: Vec<ShardId>, wrap: fn(Vec<Target>) -> Route) -> Route {
        let mut targets = Vec::with_capacity(ids.len());
        for id in ids {
            match self.target(&id) {
                Ok(t) => targets.push(t),
                Err(unavail) => return unavail,
            }
        }
        wrap(targets)
    }
}

/// A hot-swappable router the wire layer reads once per query.
pub type SharedRouter = Arc<ArcSwap<Router>>;

/// Wrap a router so it can be swapped as topology epochs advance.
pub fn shared(router: Router) -> SharedRouter {
    Arc::new(ArcSwap::from_pointee(router))
}

/// Rebuild and swap `target` whenever a new topology arrives on `updates` (the
/// [`pgshard_topo`] watcher only publishes strictly-increasing epochs). A
/// topology that fails to build — e.g. a transient reshard state whose serving
/// shards do not yet partition the keyspace — is logged and skipped, keeping the
/// last good router. Returns when the watcher is dropped.
pub async fn watch_topology(target: SharedRouter, mut updates: watch::Receiver<Arc<Topology>>) {
    while updates.changed().await.is_ok() {
        let topology = updates.borrow_and_update().clone();
        match Router::build(&topology) {
            Ok(router) => {
                let epoch = router.epoch();
                target.store(Arc::new(router));
                tracing::info!(epoch, "applied topology update");
            }
            Err(err) => {
                tracing::warn!(error = %err, "topology update failed to build; keeping current router");
            }
        }
    }
}

fn build_vschema(topo: &Topology) -> Result<VSchema, BuildError> {
    let mut vschema = VSchema::default();
    for t in &topo.tables {
        let name = TableName::new(t.schema.clone(), t.name.clone());
        let def = match t.table_type {
            TableType::Global => TableDef::Global,
            TableType::Sharded => {
                let shard_key_column = t
                    .shard_key_column
                    .clone()
                    .ok_or_else(|| BuildError::MissingShardKey(name.clone()))?;
                TableDef::Sharded {
                    shard_key_column,
                    shard_function: topo.hash_function.clone(),
                    sequences: t
                        .sequences
                        .iter()
                        .map(|s| SequenceBinding {
                            column: s.column.clone(),
                            sequence: s.sequence.clone(),
                        })
                        .collect(),
                }
            }
        };
        vschema.insert(name, def)?;
    }
    Ok(vschema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgshard_topo::{Instance, Sequence, ShardEntry, TableEntry};

    fn instance(pod: &str) -> Instance {
        Instance {
            pod: pod.to_owned(),
            host: format!("{pod}-host"),
            port: 5432,
            can_read: false,
        }
    }

    /// A target for shard `db` whose primary is pod `pod`.
    fn target(db: &str, pod: &str) -> Target {
        Target {
            endpoint: instance(pod),
            database: db.to_owned(),
        }
    }

    fn shard(name: &str, range: &str, primary: Option<&str>) -> ShardEntry {
        ShardEntry {
            name: name.to_owned(),
            key_range: range.parse().unwrap(),
            state: ShardState::Serving,
            primary: primary.map(instance),
            replicas: Vec::new(),
        }
    }

    fn orders() -> TableEntry {
        TableEntry {
            schema: "public".into(),
            name: "orders".into(),
            table_type: TableType::Sharded,
            shard_key_column: Some("customer_id".into()),
            sequences: vec![Sequence {
                column: "id".into(),
                sequence: "orders_id".into(),
            }],
        }
    }

    fn settings() -> TableEntry {
        TableEntry {
            schema: "public".into(),
            name: "settings".into(),
            table_type: TableType::Global,
            shard_key_column: None,
            sequences: Vec::new(),
        }
    }

    fn topology(shards: Vec<ShardEntry>, tables: Vec<TableEntry>) -> Topology {
        Topology {
            epoch: 7,
            topology_generation: 1,
            write_lease_seconds: 10,
            hash_function: "xxhash64_v1".into(),
            shards,
            tables,
            gates: Vec::new(),
            sequence_endpoint: Some(instance("system")),
        }
    }

    fn router() -> Router {
        Router::build(&topology(
            vec![
                shard("s0", "-80", Some("s0p")),
                shard("s1", "80-", Some("s1p")),
            ],
            vec![orders(), settings()],
        ))
        .unwrap()
    }

    fn one(sql: &str) -> Route {
        router().route(sql).unwrap().into_iter().next().unwrap()
    }

    #[test]
    fn single_shard_resolves_to_that_shards_primary() {
        // customer_id=1 hashes into [80-) -> s1; =0 into [-80) -> s0.
        assert_eq!(
            one("SELECT * FROM orders WHERE customer_id = 1"),
            Route::Shard(target("s1", "s1p"))
        );
        assert_eq!(
            one("INSERT INTO orders (customer_id) VALUES (0)"),
            Route::Shard(target("s0", "s0p"))
        );
    }

    #[test]
    fn scatter_and_broadcast_resolve_to_primaries() {
        assert_eq!(
            one("SELECT * FROM orders"),
            Route::Scatter(vec![target("s0", "s0p"), target("s1", "s1p")])
        );
        assert_eq!(
            one("CREATE TABLE t (id int)"),
            Route::Broadcast(vec![target("s0", "s0p"), target("s1", "s1p")])
        );
    }

    #[test]
    fn global_reads_go_to_the_system_endpoint() {
        assert_eq!(
            one("SELECT * FROM settings"),
            Route::System(instance("system"))
        );
        assert_eq!(one("SELECT 1"), Route::Local);
    }

    #[test]
    fn parameterized_and_reject_pass_through() {
        assert!(matches!(
            one("SELECT * FROM orders WHERE customer_id = $1"),
            Route::NeedsBind(_)
        ));
        assert!(matches!(
            one("UPDATE orders SET total = 1"),
            Route::Reject { code: "0A000", .. }
        ));
    }

    #[test]
    fn a_shard_without_a_primary_is_unavailable() {
        let r = Router::build(&topology(
            vec![
                shard("s0", "-80", Some("s0p")),
                shard("s1", "80-", None), // failing over
            ],
            vec![orders()],
        ))
        .unwrap();
        // customer_id=1 -> s1, which has no primary.
        assert!(matches!(
            r.route("SELECT * FROM orders WHERE customer_id = 1")
                .unwrap()
                .into_iter()
                .next()
                .unwrap(),
            Route::Unavailable(_)
        ));
        // A scatter also becomes unavailable if any covered shard lacks a primary.
        assert!(matches!(
            r.route("SELECT * FROM orders")
                .unwrap()
                .into_iter()
                .next()
                .unwrap(),
            Route::Unavailable(_)
        ));
    }

    #[test]
    fn non_serving_shards_are_excluded_from_routing() {
        // A buffered reshard target does not partition the space on its own; the
        // serving shards must still partition. Here the serving pair does.
        let mut extra = shard("target", "40-80", Some("tp"));
        extra.state = ShardState::Buffered;
        let r = Router::build(&topology(
            vec![
                shard("s0", "-80", Some("s0p")),
                shard("s1", "80-", Some("s1p")),
                extra,
            ],
            vec![orders()],
        ))
        .unwrap();
        assert_eq!(
            r.route("SELECT * FROM orders WHERE customer_id = 1")
                .unwrap()
                .into_iter()
                .next()
                .unwrap(),
            Route::Shard(target("s1", "s1p"))
        );
    }

    #[test]
    fn build_rejects_bad_topologies() {
        // Serving shards that leave a gap are not a partition.
        let gap = topology(
            vec![shard("s0", "-40", Some("a")), shard("s1", "80-", Some("b"))],
            vec![orders()],
        );
        assert!(matches!(Router::build(&gap), Err(BuildError::Partition(_))));

        // A sharded table with no shard key.
        let mut bad_table = orders();
        bad_table.shard_key_column = None;
        let missing = topology(vec![shard("s0", "-", Some("a"))], vec![bad_table]);
        assert!(matches!(
            Router::build(&missing),
            Err(BuildError::MissingShardKey(_))
        ));

        // An unknown hash function.
        let mut unknown = topology(vec![shard("s0", "-", Some("a"))], vec![orders()]);
        unknown.hash_function = "md5".into();
        assert!(matches!(
            Router::build(&unknown),
            Err(BuildError::VSchema(_))
        ));

        // Two serving shards with the same name.
        let dup = topology(
            vec![
                shard("dup", "-80", Some("a")),
                shard("dup", "80-", Some("b")),
            ],
            vec![orders()],
        );
        assert!(matches!(
            Router::build(&dup),
            Err(BuildError::DuplicateShard(_))
        ));
    }

    #[test]
    fn a_valid_but_unsorted_shard_list_still_builds_and_routes() {
        // The topology layer accepts an unsorted-but-partitioning shard list, so
        // the router must too (sort before validating contiguity).
        let r = Router::build(&topology(
            vec![
                shard("s1", "80-", Some("s1p")),
                shard("s0", "-80", Some("s0p")),
            ],
            vec![orders()],
        ))
        .unwrap();
        assert_eq!(
            r.route("SELECT * FROM orders WHERE customer_id = 0")
                .unwrap()
                .into_iter()
                .next()
                .unwrap(),
            Route::Shard(target("s0", "s0p"))
        );
    }

    async fn wait_for_epoch(router: &SharedRouter, epoch: u64) {
        for _ in 0..200 {
            if router.load().epoch() == epoch {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!(
            "router never reached epoch {epoch}; still {}",
            router.load().epoch()
        );
    }

    #[tokio::test]
    async fn watch_topology_applies_higher_epochs_and_keeps_the_last_good_router() {
        let pair = || {
            vec![
                shard("s0", "-80", Some("s0p")),
                shard("s1", "80-", Some("s1p")),
            ]
        };
        let base = topology(pair(), vec![orders()]); // epoch 7
        let router = shared(Router::build(&base).unwrap());
        assert_eq!(router.load().epoch(), 7);

        let (tx, rx) = watch::channel(Arc::new(base));
        let handle = tokio::spawn(watch_topology(router.clone(), rx));

        // A higher-epoch valid topology is applied.
        let mut newer = topology(pair(), vec![orders()]);
        newer.epoch = 8;
        tx.send(Arc::new(newer)).unwrap();
        wait_for_epoch(&router, 8).await;

        // A topology that fails to build (unknown hash function) is skipped; the
        // last good router stays serving.
        let mut broken = topology(pair(), vec![orders()]);
        broken.epoch = 9;
        broken.hash_function = "md5".into();
        tx.send(Arc::new(broken)).unwrap();
        for _ in 0..20 {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        assert_eq!(
            router.load().epoch(),
            8,
            "a build error must keep the last good router"
        );

        // And the watcher survives the error: a later valid epoch still applies.
        let mut recovered = topology(pair(), vec![orders()]);
        recovered.epoch = 10;
        tx.send(Arc::new(recovered)).unwrap();
        wait_for_epoch(&router, 10).await;

        drop(tx);
        handle.await.unwrap();
    }
}
