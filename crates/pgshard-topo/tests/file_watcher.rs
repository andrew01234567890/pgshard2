use std::sync::Arc;
use std::time::Duration;

use pgshard_topo::{FileWatcher, ShardState, TableType, Topology, TopologyWatcher};

/// A minimal serving-partition topology in the CRD JSON shape (camelCase,
/// `{start,end}` key ranges) that the operator compiles into PgShardRouting.
fn two_shard_topology(epoch: u64) -> serde_json::Value {
    serde_json::json!({
        "epoch": epoch,
        "topologyGeneration": 1,
        "shards": [
            {"name": "c-min-80", "keyRange": {"end": "80"}, "state": "serving",
             "primary": {"pod": "c-min-80-0", "host": "10.0.0.1"}},
            {"name": "c-80-max", "keyRange": {"start": "80"}, "state": "serving",
             "primary": {"pod": "c-80-max-0", "host": "10.0.0.2"}},
        ],
        "tables": [
            {"schema": "public", "name": "orders", "type": "sharded",
             "shardKeyColumn": "customer_id"}
        ]
    })
}

async fn write_topology(path: &std::path::Path, value: &serde_json::Value) {
    tokio::fs::write(path, serde_json::to_vec_pretty(value).unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn loads_and_applies_epoch_ordered_updates() {
    let dir = std::env::temp_dir().join(format!("pgshard-topo-{}", std::process::id()));
    tokio::fs::create_dir_all(&dir).await.unwrap();
    let path = dir.join("topology.json");

    write_topology(&path, &two_shard_topology(1)).await;
    let watcher = FileWatcher::start(&path, Duration::from_secs(3600))
        .await
        .unwrap();
    let mut rx = watcher.subscribe();
    assert_eq!(rx.borrow().epoch, 1);
    assert_eq!(rx.borrow().shards.len(), 2);
    assert_eq!(rx.borrow().shards[0].state, ShardState::Serving);
    assert_eq!(rx.borrow().shards[0].key_range.to_string(), "-80");

    // Higher epoch applies and wakes subscribers.
    write_topology(&path, &two_shard_topology(5)).await;
    assert!(watcher.reload().await.unwrap());
    rx.changed().await.unwrap();
    assert_eq!(rx.borrow().epoch, 5);

    // Lower or equal epochs are ignored.
    write_topology(&path, &two_shard_topology(4)).await;
    assert!(!watcher.reload().await.unwrap());
    write_topology(&path, &two_shard_topology(5)).await;
    assert!(!watcher.reload().await.unwrap());
    assert_eq!(rx.borrow().epoch, 5);
}

#[tokio::test]
async fn invalid_snapshots_are_rejected_and_current_kept() {
    let dir = std::env::temp_dir().join(format!("pgshard-topo-bad-{}", std::process::id()));
    tokio::fs::create_dir_all(&dir).await.unwrap();
    let path = dir.join("topology.json");

    write_topology(&path, &two_shard_topology(1)).await;
    let watcher = FileWatcher::start(&path, Duration::from_secs(3600))
        .await
        .unwrap();
    let rx = watcher.subscribe();

    // Gap in the serving partition (only one shard serving).
    let mut broken = two_shard_topology(9);
    broken["shards"][1]["state"] = serde_json::json!("hidden");
    write_topology(&path, &broken).await;
    assert!(watcher.reload().await.is_err());
    assert_eq!(rx.borrow().epoch, 1);

    // Unknown hash function.
    let mut broken = two_shard_topology(9);
    broken["hashFunction"] = serde_json::json!("md5");
    write_topology(&path, &broken).await;
    assert!(watcher.reload().await.is_err());
    assert_eq!(rx.borrow().epoch, 1);

    // Epoch below the CRD 1-based minimum.
    let mut broken = two_shard_topology(0);
    broken["epoch"] = serde_json::json!(0);
    write_topology(&path, &broken).await;
    assert!(watcher.reload().await.is_err());
    assert_eq!(rx.borrow().epoch, 1);

    // Malformed JSON.
    tokio::fs::write(&path, b"{nope").await.unwrap();
    assert!(watcher.reload().await.is_err());
    assert_eq!(rx.borrow().epoch, 1);
}

#[tokio::test]
async fn default_topology_fails_validation() {
    let empty = Arc::new(Topology::default());
    assert!(pgshard_topo::validate(&empty).is_err());
}

#[tokio::test]
async fn initial_load_must_be_valid() {
    let dir = std::env::temp_dir().join(format!("pgshard-topo-init-{}", std::process::id()));
    tokio::fs::create_dir_all(&dir).await.unwrap();
    let path = dir.join("topology.json");
    let mut broken = two_shard_topology(1);
    broken["shards"][0]["state"] = serde_json::json!("hidden");
    write_topology(&path, &broken).await;
    assert!(
        FileWatcher::start(&path, Duration::from_secs(3600))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn zero_poll_interval_is_rejected() {
    let dir = std::env::temp_dir().join(format!("pgshard-topo-zero-{}", std::process::id()));
    tokio::fs::create_dir_all(&dir).await.unwrap();
    let path = dir.join("topology.json");
    write_topology(&path, &two_shard_topology(1)).await;
    assert!(FileWatcher::start(&path, Duration::ZERO).await.is_err());
}

/// The file model deserializes a full PgShardRouting spec (as the operator
/// compiles it) and self-round-trips, locking the crate to the CRD wire shape.
#[test]
fn crd_shape_round_trips() {
    let crd = serde_json::json!({
        "epoch": 7,
        "topologyGeneration": 3,
        "writeLeaseSeconds": 15,
        "hashFunction": "xxhash64_v1",
        "shards": [
            {"name": "c-min-80", "keyRange": {"end": "80"}, "state": "serving",
             "primary": {"pod": "c-min-80-0", "host": "10.0.0.1", "port": 5432},
             "replicas": [{"pod": "c-min-80-1", "host": "10.0.0.3", "canRead": true}]},
            {"name": "c-80-max", "keyRange": {"start": "80"}, "state": "serving",
             "primary": {"pod": "c-80-max-0", "host": "10.0.0.2"}}
        ],
        "tables": [
            {"schema": "public", "name": "orders", "type": "sharded",
             "shardKeyColumn": "customer_id"},
            {"schema": "public", "name": "countries", "type": "global",
             "sequences": [{"column": "id", "sequence": "countries_id_seq"}]}
        ],
        "gates": [
            {"id": "reshard-1", "match": {"keyRanges": [{"start": "80"}]},
             "mode": "bufferWrites", "deadline": "2026-07-17T00:00:00Z",
             "minTopologyGeneration": 3}
        ],
        "sequenceEndpoint": {"pod": "sys-0", "host": "10.0.1.1"}
    });

    let topo: Topology = serde_json::from_value(crd).unwrap();
    assert_eq!(topo.epoch, 7);
    assert_eq!(topo.topology_generation, 3);
    assert_eq!(topo.write_lease_seconds, 15);
    assert_eq!(topo.tables[0].table_type, TableType::Sharded);
    assert_eq!(topo.tables[1].table_type, TableType::Global);
    assert_eq!(topo.tables[1].sequences[0].sequence, "countries_id_seq");
    assert_eq!(topo.gates[0].match_.key_ranges.len(), 1);
    assert_eq!(
        topo.gates[0].match_.key_ranges[0].start(),
        0x8000_0000_0000_0000
    );
    assert!(topo.shards[0].replicas[0].can_read);
    assert!(topo.sequence_endpoint.is_some());
    assert!(pgshard_topo::validate(&topo).is_ok());

    // Serialize then deserialize is the identity (faithful mirror).
    let reserialized = serde_json::to_value(&topo).unwrap();
    let again: Topology = serde_json::from_value(reserialized).unwrap();
    assert_eq!(topo, again);
}

/// A noncanonical key-range bound (one the CRD's pattern rejects because it
/// aliases a shorter bound, e.g. "4000" == "40") is rejected, not silently
/// canonicalized.
#[test]
fn noncanonical_key_range_bound_is_rejected() {
    let bad = serde_json::json!({
        "epoch": 1,
        "topologyGeneration": 1,
        "shards": [
            {"name": "s", "keyRange": {"end": "4000"}, "state": "serving",
             "primary": {"pod": "s-0", "host": "10.0.0.1"}}
        ]
    });
    assert!(serde_json::from_value::<Topology>(bad).is_err());
}

/// Omitted optional fields resolve to the CRD's documented defaults, not to
/// their bare Rust zero values (write lease 10, not 0; port 5432; hash fn set).
#[test]
fn omitted_optionals_use_crd_defaults() {
    let minimal = serde_json::json!({
        "epoch": 1,
        "topologyGeneration": 1,
        "shards": [
            {"name": "s", "keyRange": {}, "state": "serving",
             "primary": {"pod": "s-0", "host": "10.0.0.1"}}
        ]
    });
    let topo: Topology = serde_json::from_value(minimal).unwrap();
    assert_eq!(topo.write_lease_seconds, 10);
    assert_eq!(topo.hash_function, "xxhash64_v1");
    assert_eq!(topo.shards[0].primary.as_ref().unwrap().port, 5432);
    // An empty `{}` range is the full keyspace.
    assert_eq!(topo.shards[0].key_range.to_string(), "-");
    assert!(pgshard_topo::validate(&topo).is_ok());
}
