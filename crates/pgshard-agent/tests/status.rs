//! `PgInstance::snapshot` against a real PostgreSQL. Requires Docker
//! (testcontainers); runs in CI's rust lane.

use pgshard_agent::instance::Instance;
use pgshard_agent::pg::PgInstance;
use pgshard_testutil::Pg;

#[tokio::test]
async fn snapshot_reports_a_real_primary() {
    let pg = Pg::start().await.expect("start postgres");
    let snap = PgInstance::new(pg.connection_string())
        .snapshot()
        .await
        .expect("snapshot");

    assert!(!snap.in_recovery, "a fresh cluster is a primary");
    assert!(
        snap.accepting,
        "a successful snapshot means it accepts connections"
    );
    // The live timeline, read from the current WAL file name — 1 on a fresh
    // cluster, and (unlike pg_control_checkpoint) never lagging a promotion.
    assert_eq!(snap.timeline, 1);
    assert!(snap.write_lsn > 0, "a primary reports a current write LSN");
    assert_eq!(snap.receive_lsn, 0, "a primary has no receive LSN");
    assert!(!snap.receiver_active, "a primary has no walreceiver");
    assert!(snap.system_id != 0, "the control-file system id is read");
    assert!(!snap.postgres_version.is_empty());
}
