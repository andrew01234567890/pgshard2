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
        max_total_sessions: 100,
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
        max_total_sessions: 1000,
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
async fn open_before_generation_holds_until_topology_applied() {
    let engine = GateEngine::new(GateLimits::default());
    engine.topology_applied(1);
    // keyrange_gate requires topology generation 2.
    engine.close(keyrange_gate("cut", "40-80", Duration::from_secs(60)));
    let mut parked = engine.check(&write_scope("orders", 0x50 << 56)).unwrap();

    // Open arrives before generation 2 is applied: the write must NOT be
    // released against the stale generation-1 topology — it stays parked.
    engine.open("cut");
    assert!(parked.try_recv().is_err());
    assert_eq!(engine.status(), vec![("cut".to_string(), 1)]);

    // Applying generation 2 satisfies the held-open gate and replays it.
    engine.topology_applied(2);
    match parked.await.unwrap() {
        Release::Replan {
            topology_generation,
        } => assert_eq!(topology_generation, 2),
        other => panic!("expected replan at gen 2, got {other:?}"),
    }
}

#[tokio::test]
async fn open_after_deadline_expires_instead_of_replaying() {
    let engine = GateEngine::new(GateLimits::default());
    engine.topology_applied(2);
    engine.close(keyrange_gate("late", "-", Duration::from_millis(0)));
    let parked = engine.check(&write_scope("t", 1)).unwrap();
    std::thread::sleep(Duration::from_millis(2));
    // A late open (past the deadline) is an aborted cutover, not a switch.
    engine.open("late");
    assert_eq!(parked.await.unwrap(), Release::Expired);
}

#[tokio::test]
async fn overlapping_gates_require_recheck_after_each_release() {
    let engine = GateEngine::new(GateLimits::default());
    engine.topology_applied(5);
    let mut failover = keyrange_gate("failover", "40-80", Duration::from_secs(60));
    failover.min_topology_generation = 5;
    engine.close(failover);
    engine.close(GateSpec {
        id: "ddl".into(),
        mode: GateMode::WritesOnly,
        matcher: GateMatch {
            tables: vec!["orders".into()],
            ..GateMatch::default()
        },
        deadline: Instant::now() + Duration::from_secs(60),
        min_topology_generation: 5,
    });

    // This write matches BOTH gates (range 40-80 and table orders).
    let scope = StatementScope {
        tables: vec!["orders".into()],
        keyspace_ids: vec![KeyspaceId(0x50 << 56)],
        is_write: true,
    };
    // check parks behind the first gate; opening it releases the session, which
    // re-checks and parks behind the still-closed second gate.
    let first = engine.check(&scope).unwrap();
    engine.open("failover");
    assert!(matches!(first.await.unwrap(), Release::Replan { .. }));
    let second = engine.check(&scope).unwrap();
    engine.open("ddl");
    assert!(matches!(second.await.unwrap(), Release::Replan { .. }));
    // No gate blocks the write now.
    assert!(engine.check(&scope).is_none());
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
