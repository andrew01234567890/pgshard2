//! Router-facing topology model. Mirrors the PgShardRouting CRD (the
//! Kubernetes watcher deserializes the CRD into these types; the file
//! watcher reads them as JSON).

use pgshard_core::KeyRange;
use serde::{Deserialize, Serialize};

fn default_port() -> u16 {
    5432
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Instance {
    pub pod: String,
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default)]
    pub can_read: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ShardState {
    Serving,
    Buffered,
    ReadOnly,
    Draining,
    Hidden,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardEntry {
    pub name: String,
    #[serde(with = "keyrange_serde")]
    pub key_range: KeyRange,
    pub state: ShardState,
    #[serde(default)]
    pub primary: Option<Instance>,
    #[serde(default)]
    pub replicas: Vec<Instance>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableEntry {
    #[serde(default = "default_schema")]
    pub schema: String,
    pub name: String,
    #[serde(default)]
    pub shard_key_column: Option<String>,
    #[serde(default)]
    pub global: bool,
}

fn default_schema() -> String {
    "public".to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum GateMode {
    BufferWrites,
    BufferAll,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateSpec {
    pub id: String,
    pub mode: GateMode,
    /// RFC 3339 wall-clock deadline; the gate engine converts to a
    /// monotonic instant when it applies the snapshot.
    pub deadline: String,
    #[serde(default)]
    pub min_topology_generation: u64,
    #[serde(default)]
    pub tables: Vec<String>,
    #[serde(default, with = "keyrange_vec_serde")]
    pub key_ranges: Vec<KeyRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topology {
    pub epoch: u64,
    pub topology_generation: u64,
    #[serde(default = "default_hash_function")]
    pub hash_function: String,
    #[serde(default)]
    pub write_lease_seconds: u32,
    #[serde(default)]
    pub shards: Vec<ShardEntry>,
    #[serde(default)]
    pub tables: Vec<TableEntry>,
    #[serde(default)]
    pub gates: Vec<GateSpec>,
    #[serde(default)]
    pub sequence_endpoint: Option<Instance>,
}

fn default_hash_function() -> String {
    "xxhash64_v1".to_string()
}

impl Default for Topology {
    /// An empty, epoch-zero placeholder used before the first snapshot;
    /// deliberately fails validation (no serving shards) so nothing routes
    /// against it.
    fn default() -> Self {
        Topology {
            epoch: 0,
            topology_generation: 0,
            hash_function: default_hash_function(),
            write_lease_seconds: 0,
            shards: Vec::new(),
            tables: Vec::new(),
            gates: Vec::new(),
            sequence_endpoint: None,
        }
    }
}

/// Canonical trimmed-hex bound syntax ("40-80", "-" = full range), shared
/// with the CRD and the Go operator.
mod keyrange_serde {
    use pgshard_core::KeyRange;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(kr: &KeyRange, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&kr.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<KeyRange, D::Error> {
        let raw = String::deserialize(d)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

mod keyrange_vec_serde {
    use pgshard_core::KeyRange;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[KeyRange], s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(v.iter().map(|kr| kr.to_string()))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<KeyRange>, D::Error> {
        let raw = Vec::<String>::deserialize(d)?;
        raw.into_iter()
            .map(|s| s.parse().map_err(serde::de::Error::custom))
            .collect()
    }
}
