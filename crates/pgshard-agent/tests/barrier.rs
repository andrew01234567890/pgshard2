//! `PgInstance` barrier primitives against a real PostgreSQL: the consistency
//! point and WAL switch a cross-shard backup barrier is built from. Requires
//! Docker (testcontainers); runs in CI's rust lane.

use pgshard_agent::instance::Instance;
use pgshard_agent::pg::PgInstance;
use pgshard_testutil::Pg;

#[tokio::test]
async fn create_restore_point_and_switch_wal_on_a_real_primary() {
    let pg = Pg::start().await.expect("start postgres");
    let instance = PgInstance::new(pg.connection_string());

    // A restore point on the primary returns a real LSN and the current timeline.
    let rp = instance
        .create_restore_point("pgshard_barrier_1")
        .await
        .expect("create restore point");
    assert!(rp.lsn > 0, "restore point carries a real LSN");
    assert_eq!(rp.timeline, 1, "a fresh cluster is on timeline 1");

    // A second point does not move backwards (LSNs are monotonic on a primary).
    let rp2 = instance
        .create_restore_point("pgshard_barrier_2")
        .await
        .expect("second restore point");
    assert!(rp2.lsn >= rp.lsn, "restore-point LSNs advance");

    // Forcing a WAL switch returns the switch LSN.
    let switch_lsn = instance.switch_wal(false).await.expect("switch wal");
    assert!(
        switch_lsn >= rp2.lsn,
        "the switch LSN is at least the last point"
    );

    // Waiting for archival is not implemented yet: it errors loudly rather than
    // return an unconfirmed LSN.
    assert!(
        instance.switch_wal(true).await.is_err(),
        "wait_archived is rejected until implemented"
    );
}
