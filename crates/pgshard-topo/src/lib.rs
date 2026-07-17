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
    GateMatch, GateMode, GateSpec, Instance, Sequence, ShardEntry, ShardState, TableEntry,
    TableType, Topology,
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
