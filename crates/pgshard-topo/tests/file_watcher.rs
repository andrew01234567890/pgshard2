use std::sync::Arc;
use std::time::Duration;

use pgshard_topo::{FileWatcher, ShardState, Topology, TopologyWatcher};

fn two_shard_topology(epoch: u64) -> serde_json::Value {
    serde_json::json!({
        "epoch": epoch,
        "topology_generation": 1,
        "shards": [
            {"name": "c-min-80", "key_range": "-80", "state": "serving",
             "primary": {"pod": "c-min-80-0", "host": "10.0.0.1"}},
            {"name": "c-80-max", "key_range": "80-", "state": "serving",
             "primary": {"pod": "c-80-max-0", "host": "10.0.0.2"}},
        ],
        "tables": [
            {"name": "orders", "shard_key_column": "customer_id"}
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
    broken["hash_function"] = serde_json::json!("md5");
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
