//! Router-facing topology model — a faithful mirror of the PgShardRouting CRD
//! (`PgShardRoutingSpec`). The Kubernetes watcher deserializes the CRD spec
//! into these types; the file watcher reads the identical JSON shape (camelCase
//! fields, `{start,end}` key ranges, `match`-nested gates), so a topology.json
//! is exactly what the operator compiles into the CRD.

use pgshard_core::KeyRange;
use serde::{Deserialize, Serialize};

fn default_port() -> u16 {
    5432
}

/// A directly addressable PostgreSQL instance (CRD `RoutingEndpoint`). Routers
/// dial the pod host, not a Service, so routing changes never wait on kube-proxy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
pub struct ShardEntry {
    pub name: String,
    #[serde(with = "keyrange_serde")]
    pub key_range: KeyRange,
    pub state: ShardState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary: Option<Instance>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replicas: Vec<Instance>,
}

/// Reuses the vschema table kind (CRD `TableType`); the operator projects
/// TableEntry.Type into RoutingTable.type, so the two must stay one enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TableType {
    Sharded,
    Global,
}

/// Binds a column to a global sequence (CRD `RoutingSequence`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sequence {
    pub column: String,
    pub sequence: String,
}

/// The declared type of a shard-key column (CRD `shardKeyType`). It lets the
/// router coerce a literal to the column's type before hashing so that different
/// spellings of one value (e.g. `1` and `'1'`) route to the same shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ShardKeyType {
    Int,
    Text,
    Uuid,
    Bytea,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableEntry {
    // schema is required by the CRD (the operator always materializes it, e.g.
    // "public"), so it is required here too rather than silently defaulted.
    pub schema: String,
    pub name: String,
    #[serde(rename = "type")]
    pub table_type: TableType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key_column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key_type: Option<ShardKeyType>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sequences: Vec<Sequence>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum GateMode {
    BufferWrites,
    BufferAll,
}

fn default_gate_mode() -> GateMode {
    GateMode::BufferWrites
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// Selects the traffic a gate buffers (CRD `GateMatch`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GateMatch {
    #[serde(default, skip_serializing_if = "is_false")]
    pub all: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<String>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        with = "keyrange_vec_serde"
    )]
    pub key_ranges: Vec<KeyRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GateSpec {
    pub id: String,
    // match is required by the CRD; requiring it here rejects a malformed gate
    // that would otherwise advance the epoch while selecting no traffic.
    #[serde(rename = "match")]
    pub match_: GateMatch,
    #[serde(default = "default_gate_mode")]
    pub mode: GateMode,
    /// RFC 3339 wall-clock deadline; the gate engine converts to a monotonic
    /// instant when it applies the snapshot.
    pub deadline: String,
    #[serde(default)]
    pub min_topology_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Topology {
    pub epoch: u64,
    pub topology_generation: u64,
    #[serde(default = "default_write_lease_seconds")]
    pub write_lease_seconds: u32,
    #[serde(default = "default_hash_function")]
    pub hash_function: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shards: Vec<ShardEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<TableEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gates: Vec<GateSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence_endpoint: Option<Instance>,
}

fn default_hash_function() -> String {
    "xxhash64_v1".to_string()
}

fn default_write_lease_seconds() -> u32 {
    10
}

impl Default for Topology {
    /// An empty, epoch-zero placeholder used before the first snapshot;
    /// deliberately fails validation (no serving shards) so nothing routes
    /// against it.
    fn default() -> Self {
        Topology {
            epoch: 0,
            topology_generation: 0,
            write_lease_seconds: default_write_lease_seconds(),
            hash_function: default_hash_function(),
            shards: Vec::new(),
            tables: Vec::new(),
            gates: Vec::new(),
            sequence_endpoint: None,
        }
    }
}

/// A key range in the CRD `{start,end}` shape: canonical trimmed big-endian hex
/// bounds, where an empty string is 0 (start) or the top of the keyspace (end).
/// Shared byte-for-byte with the Go operator's KeyRange.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Bounds {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    start: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    end: String,
}

impl Bounds {
    fn from_range(kr: &KeyRange) -> Self {
        Bounds {
            start: pgshard_core::keyspace::format_bound(kr.start()),
            end: kr
                .end()
                .map(pgshard_core::keyspace::format_bound)
                .unwrap_or_default(),
        }
    }

    fn into_range<E: serde::de::Error>(self) -> Result<KeyRange, E> {
        let start = canonical_bound(&self.start).map_err(E::custom)?;
        let end = if self.end.is_empty() {
            None
        } else {
            Some(canonical_bound(&self.end).map_err(E::custom)?)
        };
        KeyRange::new(start, end).map_err(E::custom)
    }
}

/// Parses a trimmed-hex bound, rejecting a noncanonical spelling the CRD's
/// pattern forbids (e.g. "4000", which aliases "40"): the input must be exactly
/// what `format_bound` would round-trip to.
fn canonical_bound(s: &str) -> Result<u64, String> {
    let value = pgshard_core::keyspace::parse_bound(s).map_err(|e| e.to_string())?;
    let canonical = pgshard_core::keyspace::format_bound(value);
    if canonical != s {
        return Err(format!(
            "noncanonical key-range bound {s:?}: expected {canonical:?}"
        ));
    }
    Ok(value)
}

mod keyrange_serde {
    use super::Bounds;
    use pgshard_core::KeyRange;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(kr: &KeyRange, s: S) -> Result<S::Ok, S::Error> {
        Bounds::from_range(kr).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<KeyRange, D::Error> {
        Bounds::deserialize(d)?.into_range()
    }
}

mod keyrange_vec_serde {
    use super::Bounds;
    use pgshard_core::KeyRange;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[KeyRange], s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(v.iter().map(Bounds::from_range))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<KeyRange>, D::Error> {
        Vec::<Bounds>::deserialize(d)?
            .into_iter()
            .map(|b| b.into_range())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_key_type_round_trips_as_a_lowercase_string() {
        let entry = TableEntry {
            schema: "public".into(),
            name: "orders".into(),
            table_type: TableType::Sharded,
            shard_key_column: Some("customer_id".into()),
            shard_key_type: Some(ShardKeyType::Int),
            sequences: Vec::new(),
        };
        let json = serde_json::to_value(&entry).unwrap();
        assert_eq!(json["shardKeyType"], "int");
        assert_eq!(serde_json::from_value::<TableEntry>(json).unwrap(), entry);
    }

    #[test]
    fn a_topology_without_a_shard_key_type_defaults_to_none() {
        // Backward compatibility: topology JSON emitted before the field existed
        // must still deserialize, with the type absent.
        let entry: TableEntry = serde_json::from_str(
            r#"{"schema":"public","name":"orders","type":"sharded","shardKeyColumn":"customer_id"}"#,
        )
        .unwrap();
        assert_eq!(entry.shard_key_type, None);
    }
}
