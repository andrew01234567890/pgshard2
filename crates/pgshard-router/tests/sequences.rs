//! `PgBlockReserver` against a real PostgreSQL: reservations advance the row
//! monotonically and, under concurrent connections, never overlap — the atomic
//! `UPDATE ... RETURNING` is what keeps ids globally unique. Requires Docker
//! (testcontainers); runs in CI's rust lane.

use pgshard_router::sequence::PgBlockReserver;
use pgshard_seq::{BlockReserver, SeqError};
use pgshard_testutil::Pg;

const SCHEMA: &str = "\
    CREATE SCHEMA pgshard; \
    CREATE TABLE pgshard.sequences ( \
        name text PRIMARY KEY, \
        next_id bigint NOT NULL, \
        block_size bigint NOT NULL); \
    INSERT INTO pgshard.sequences VALUES ('orders_id', 1, 100);";

#[tokio::test]
async fn reserves_disjoint_monotonic_blocks() {
    let pg = Pg::start().await.expect("start postgres");
    pg.connect()
        .await
        .unwrap()
        .batch_execute(SCHEMA)
        .await
        .unwrap();

    let reserver = PgBlockReserver::new(pg.connection_string().parse().unwrap());

    // reserve() drives a blocking client, so it runs on a blocking thread.
    tokio::task::spawn_blocking(move || {
        // Each reservation claims the next block_size ids; the row advances so
        // the ranges are back-to-back and disjoint.
        assert_eq!(reserver.reserve("orders_id").unwrap(), (1, 100));
        assert_eq!(reserver.reserve("orders_id").unwrap(), (101, 100));
        assert_eq!(reserver.reserve("orders_id").unwrap(), (201, 100));

        // An unregistered sequence is a distinct error, not a backend failure —
        // a caller can tell "no such sequence" from "the database is down".
        assert!(matches!(
            reserver.reserve("missing"),
            Err(SeqError::UnknownSequence(s)) if s == "missing"
        ));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn misconfigured_block_size_is_rejected_without_moving_next_id() {
    let pg = Pg::start().await.expect("start postgres");
    let admin = pg.connect().await.unwrap();
    admin
        .batch_execute(
            "CREATE SCHEMA pgshard; \
             CREATE TABLE pgshard.sequences ( \
                 name text PRIMARY KEY, next_id bigint NOT NULL, block_size bigint NOT NULL); \
             INSERT INTO pgshard.sequences VALUES ('bad', 500, -100);",
        )
        .await
        .unwrap();

    let reserver = PgBlockReserver::new(pg.connection_string().parse().unwrap());
    tokio::task::spawn_blocking(move || {
        // A non-positive block_size is rejected loudly, not used.
        assert!(matches!(reserver.reserve("bad"), Err(SeqError::Backend(_))));
    })
    .await
    .unwrap();

    // Crucially, the rejected reservation must NOT have advanced (here, moved
    // backward) next_id — otherwise a later repair to a valid block_size would
    // hand out ids overlapping ones already reserved.
    let row = admin
        .query_one(
            "SELECT next_id FROM pgshard.sequences WHERE name = 'bad'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        row.get::<_, i64>(0),
        500,
        "a rejected reservation must leave next_id unchanged"
    );
}

#[tokio::test]
async fn concurrent_reservations_never_overlap() {
    let pg = Pg::start().await.expect("start postgres");
    pg.connect()
        .await
        .unwrap()
        .batch_execute(SCHEMA)
        .await
        .unwrap();

    // Eight independent connections each reserve ten blocks at once. If the
    // UPDATE were not atomic, two would claim the same range.
    const RESERVERS: usize = 8;
    const PER: usize = 10;
    let conn = pg.connection_string();
    let tasks: Vec<_> = (0..RESERVERS)
        .map(|_| {
            let conn = conn.clone();
            tokio::task::spawn_blocking(move || {
                let reserver = PgBlockReserver::new(conn.parse().unwrap());
                (0..PER)
                    .map(|_| reserver.reserve("orders_id").unwrap())
                    .collect::<Vec<_>>()
            })
        })
        .collect();

    let mut ranges: Vec<(i64, i64)> = Vec::new();
    for task in tasks {
        ranges.extend(task.await.unwrap());
    }
    ranges.sort();

    // Every id from 1 up is covered exactly once: the sorted ranges are
    // contiguous and non-overlapping, so no id was handed out twice.
    assert_eq!(ranges.len(), RESERVERS * PER);
    let mut expected = 1;
    for (start, size) in ranges {
        assert_eq!(start, expected, "gap or overlap in reserved ranges");
        assert_eq!(size, 100);
        expected += size;
    }
}
