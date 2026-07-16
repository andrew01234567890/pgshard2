use std::time::{Duration, Instant};

use pgshard_core::{KeyRange, KeyspaceId};
use pgshard_gate::{
    GateEngine, GateLimits, GateMatch, GateMode, GateSpec, Release, StatementScope,
};

fn write_scope(table: &str, id: u64) -> StatementScope {
    StatementScope {
        tables: vec![table.to_string()],
        keyspace_ids: vec![KeyspaceId(id)],
        is_write: true,
    }
}

fn read_scope(table: &str, id: u64) -> StatementScope {
    StatementScope {
        is_write: false,
        ..write_scope(table, id)
    }
}

fn keyrange_gate(id: &str, range: &str, deadline_in: Duration) -> GateSpec {
    GateSpec {
        id: id.to_string(),
        mode: GateMode::WritesOnly,
        matcher: GateMatch {
            key_ranges: vec![range.parse::<KeyRange>().unwrap()],
            ..GateMatch::default()
        },
        deadline: Instant::now() + deadline_in,
        min_topology_generation: 2,
    }
}

#[tokio::test]
async fn parks_matching_writes_and_replays_fifo_on_open() {
    let engine = GateEngine::new(GateLimits::default());
    engine.topology_applied(1);
    engine.close(keyrange_gate("g1", "40-80", Duration::from_secs(60)));

    // Write inside the gated range parks; outside proceeds; read passes.
    let parked = engine.check(&write_scope("orders", 0x50 << 56)).unwrap();
    assert!(engine.check(&write_scope("orders", 0x10 << 56)).is_none());
    assert!(engine.check(&read_scope("orders", 0x50 << 56)).is_none());

    engine.topology_applied(2);
    engine.open("g1");
    match parked.await.unwrap() {
        Release::Replan {
            topology_generation,
        } => assert_eq!(topology_generation, 2),
        other => panic!("expected replan, got {other:?}"),
    }
}

#[tokio::test]
async fn buffer_all_mode_parks_reads_too() {
    let engine = GateEngine::new(GateLimits::default());
    let mut gate = keyrange_gate("g-all", "-", Duration::from_secs(60));
    gate.mode = GateMode::All;
    gate.matcher = GateMatch {
        all: true,
        ..GateMatch::default()
    };
    engine.close(gate);
    assert!(engine.check(&read_scope("anything", 7)).is_some());
}

#[tokio::test]
async fn table_gates_match_by_name() {
    let engine = GateEngine::new(GateLimits::default());
    engine.close(GateSpec {
        id: "ddl-orders".into(),
        mode: GateMode::WritesOnly,
        matcher: GateMatch {
            tables: vec!["public.orders".into()],
            ..GateMatch::default()
        },
        deadline: Instant::now() + Duration::from_secs(60),
        min_topology_generation: 0,
    });
    assert!(engine.check(&write_scope("public.orders", 1)).is_some());
    assert!(engine.check(&write_scope("public.customers", 1)).is_none());
}

#[tokio::test]
async fn deadline_expiry_releases_as_expired() {
    let engine = GateEngine::new(GateLimits::default());
    engine.close(keyrange_gate("g-exp", "-", Duration::from_millis(0)));
    let parked = engine.check(&write_scope("t", 1)).unwrap();
    assert_eq!(engine.expire_due(Instant::now()), 1);
    assert_eq!(parked.await.unwrap(), Release::Expired);
    // Gate is gone: traffic flows.
    assert!(engine.check(&write_scope("t", 1)).is_none());
}

#[tokio::test]
async fn overfull_gate_rejects_new_sessions() {
    let engine = GateEngine::new(GateLimits {
        max_sessions: 1,
        max_wait: Duration::from_secs(20),
    });
    engine.close(keyrange_gate("g-full", "-", Duration::from_secs(60)));
    let _first = engine.check(&write_scope("t", 1)).unwrap();
    let second = engine.check(&write_scope("t", 2)).unwrap();
    assert_eq!(second.await.unwrap(), Release::Rejected);
}

#[tokio::test]
async fn overdue_sessions_are_rejected_but_gate_stays() {
    let engine = GateEngine::new(GateLimits {
        max_sessions: 100,
        max_wait: Duration::from_millis(0),
    });
    engine.close(keyrange_gate("g-wait", "-", Duration::from_secs(60)));
    let parked = engine.check(&write_scope("t", 1)).unwrap();
    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(engine.reject_overdue_sessions(Instant::now()), 1);
    assert_eq!(parked.await.unwrap(), Release::Rejected);
    assert_eq!(engine.status(), vec![("g-wait".to_string(), 0)]);
}

#[tokio::test]
async fn reclosing_same_id_updates_spec_without_dropping_parked() {
    let engine = GateEngine::new(GateLimits::default());
    engine.close(keyrange_gate("g-re", "-", Duration::from_secs(60)));
    let parked = engine.check(&write_scope("t", 1)).unwrap();
    // Level-triggered re-application of the same gate id.
    engine.close(keyrange_gate("g-re", "-", Duration::from_secs(120)));
    assert_eq!(engine.status(), vec![("g-re".to_string(), 1)]);
    engine.topology_applied(2);
    engine.open("g-re");
    assert!(matches!(parked.await.unwrap(), Release::Replan { .. }));
}
