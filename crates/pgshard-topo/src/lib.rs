//! Topology watching for the router and agents.
//!
//! The compiled routing view (PgShardRouting in Kubernetes, a JSON file in
//! tests and non-k8s development) is applied through a single rule:
//! an update is accepted iff its epoch is strictly greater than the last
//! applied epoch. Consumers hold a `tokio::sync::watch` receiver and always
//! see the newest accepted snapshot.

pub mod file;
pub mod model;

use std::sync::Arc;

use tokio::sync::watch;

pub use file::FileWatcher;
pub use model::{
    GateMatch, GateMode, GateSpec, Instance, Sequence, ShardEntry, ShardKeyType, ShardState,
    TableEntry, TableType, Topology,
};

#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("invalid topology: {0}")]
    Invalid(String),
}

/// A source of topology snapshots. Implementations push validated,
/// epoch-ordered snapshots into the watch channel.
pub trait TopologyWatcher {
    fn subscribe(&self) -> watch::Receiver<Arc<Topology>>;
}

/// The instant a watcher last successfully read AND validated its source —
/// bumped on every successful poll, including one whose epoch was unchanged
/// (an unchanged-but-readable source still confirms the view is current), and
/// never on an error. Distinct from receiving a new snapshot: this is what
/// lets a consumer bound how stale its view can possibly be — the router's
/// write lease rejects writes once the age exceeds the topology's
/// `write_lease_seconds`, so a router cut off from its source cannot keep
/// writing against a routing world that may have moved on.
#[derive(Clone)]
pub struct Freshness(Arc<std::sync::RwLock<std::time::Instant>>);

impl Freshness {
    pub fn new() -> Self {
        Self(Arc::new(std::sync::RwLock::new(std::time::Instant::now())))
    }

    /// Record a successful source validation now.
    pub fn bump(&self) {
        *self.0.write().unwrap_or_else(|e| e.into_inner()) = std::time::Instant::now();
    }

    /// How long ago the source was last successfully validated.
    pub fn age(&self) -> std::time::Duration {
        self.0.read().unwrap_or_else(|e| e.into_inner()).elapsed()
    }

    /// Test hook: pretend the last successful validation happened `by` ago.
    #[doc(hidden)]
    pub fn backdate(&self, by: std::time::Duration) {
        let mut guard = self.0.write().unwrap_or_else(|e| e.into_inner());
        if let Some(at) = std::time::Instant::now().checked_sub(by) {
            *guard = at;
        }
    }
}

impl Default for Freshness {
    fn default() -> Self {
        Self::new()
    }
}

/// Validates a candidate snapshot: the epoch and topology generation are the
/// CRD's 1-based counters, the shard function must be known, and serving shards
/// must partition the full keyspace. Serving shards are sorted before the
/// partition check so a snapshot whose shard list is not already start-ordered
/// is not spuriously rejected.
pub fn validate(topology: &Topology) -> Result<(), TopologyError> {
    if topology.epoch == 0 {
        return Err(TopologyError::Invalid("epoch must be >= 1".into()));
    }
    if topology.topology_generation == 0 {
        return Err(TopologyError::Invalid(
            "topologyGeneration must be >= 1".into(),
        ));
    }
    pgshard_core::shard_function(&topology.hash_function)
        .map_err(|e| TopologyError::Invalid(e.to_string()))?;
    let mut serving: Vec<_> = topology
        .shards
        .iter()
        .filter(|s| s.state == ShardState::Serving)
        .map(|s| s.key_range)
        .collect();
    serving.sort_by_key(|kr| kr.start());
    pgshard_core::validate_partition(&serving)
        .map_err(|e| TopologyError::Invalid(format!("serving shards: {e}")))?;
    Ok(())
}
