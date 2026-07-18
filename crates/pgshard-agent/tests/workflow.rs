//! Live end-to-end test of the seeding-workflow runner: filtered snapshot
//! seed, then filtered streaming apply, against a real PostgreSQL 18.

use std::time::Duration;

use pgshard_agent::workflow::{WorkflowConfig, WorkflowError, WorkflowRegistry};
use pgshard_core::{ScalarValue, shard_function};
use pgshard_proto::v1;
use pgshard_testutil::Pg;

const UPPER_HALF: u64 = 1 << 63;

fn in_upper_half(id: i32) -> bool {
    let f = shard_function("xxhash64_v1").unwrap();
    f.keyspace_id(&ScalarValue::Int64(id as i64)).0 >= UPPER_HALF
}

fn spec(pg: &Pg, id: &str, slot: &str) -> v1::WorkflowSpec {
    v1::WorkflowSpec {
        id: id.into(),
        kind: v1::WorkflowKind::Reshard as i32,
        source_shard: "src".into(),
        source_primary: Some(v1::PgEndpoint {
            host: pg.host().to_owned(),
            port: pg.port() as u32,
            database: "postgres".into(),
        }),
        source_policy: v1::SourcePolicy::Primary as i32,
        slot: slot.into(),
        publication: "seed_pub".into(),
        tables: vec![v1::TableMapping {
            source: Some(v1::TableRef {
                schema: "public".into(),
                name: "orders".into(),
            }),
            target: None,
            column_map: Default::default(),
            shard_key_column: "id".into(),
            shard_key_type: "int".into(),
        }],
        filter: Some(v1::RowFilter {
            filter: Some(v1::row_filter::Filter::KeyRange(v1::KeyRangeFilter {
                range: Some(v1::KeyRange {
                    start: UPPER_HALF,
                    end: None,
                }),
                hash_function: "xxhash64_v1".into(),
            })),
        }),
        target_database: "shard_target".into(),
        ..Default::default()
    }
}

async fn wait_for<F: Fn(&v1::WorkflowStatus) -> bool>(
    registry: &WorkflowRegistry,
    id: &str,
    what: &str,
    pred: F,
) -> v1::WorkflowStatus {
    for _ in 0..300 {
        if let Some(status) = registry.statuses(&[id.to_owned()]).await.into_iter().next() {
            assert!(
                status.phase != v1::WorkflowPhase::Error as i32,
                "workflow failed while waiting for {what}: {}",
                status.error
            );
            if pred(&status) {
                return status;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn seeds_then_streams_only_the_target_keyrange() -> anyhow::Result<()> {
    let pg = Pg::start().await?;
    let source = pg.connect().await?;
    source
        .batch_execute(
            "CREATE TABLE orders (id int PRIMARY KEY, note text);
             CREATE PUBLICATION seed_pub FOR TABLE orders;",
        )
        .await?;
    // Separately: CREATE DATABASE cannot run inside the implicit transaction a
    // multi-statement batch gets.
    source.batch_execute("CREATE DATABASE shard_target").await?;
    let target_conn = format!(
        "host={} port={} user=postgres password=postgres dbname=shard_target",
        pg.host(),
        pg.port()
    );
    let (target, conn) = tokio_postgres::connect(&target_conn, tokio_postgres::NoTls).await?;
    tokio::spawn(conn);
    target
        .batch_execute("CREATE TABLE orders (id int PRIMARY KEY, note text)")
        .await?;

    // Seed rows on both sides of the keyspace split.
    for id in 1..=20i32 {
        source
            .execute("INSERT INTO orders VALUES ($1, 'seed')", &[&id])
            .await?;
    }
    let expected_seeded: Vec<i32> = (1..=20).filter(|id| in_upper_half(*id)).collect();
    assert!(
        !expected_seeded.is_empty() && expected_seeded.len() < 20,
        "fixture must split across the range"
    );

    let config = WorkflowConfig {
        target: format!(
            "host={} port={} user=postgres password=postgres",
            pg.host(),
            pg.port()
        )
        .parse()?,
        source_user: "postgres".into(),
        source_password: "postgres".into(),
    };
    let registry = WorkflowRegistry::default();
    registry
        .start(&spec(&pg, "wf1", "wf1_slot"), &config)
        .await?;

    // Idempotent re-start with the identical spec; a different spec under the
    // same id is a conflict.
    registry
        .start(&spec(&pg, "wf1", "wf1_slot"), &config)
        .await?;
    match registry
        .start(&spec(&pg, "wf1", "other_slot"), &config)
        .await
    {
        Err(WorkflowError::Conflict(_)) => {}
        other => panic!("conflicting spec must be rejected, got {other:?}"),
    }

    wait_for(&registry, "wf1", "the streaming phase", |s| {
        s.phase == v1::WorkflowPhase::Streaming as i32
    })
    .await;

    let seeded: Vec<i32> = target
        .query("SELECT id FROM orders ORDER BY id", &[])
        .await?
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(
        seeded, expected_seeded,
        "seed must copy exactly the in-range rows"
    );

    // Live changes stream through the same filter.
    for id in 21..=40i32 {
        source
            .execute("INSERT INTO orders VALUES ($1, 'live')", &[&id])
            .await?;
    }
    let expected_all: Vec<i32> = (1..=40).filter(|id| in_upper_half(*id)).collect();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let rows: Vec<i32> = target
            .query("SELECT id FROM orders ORDER BY id", &[])
            .await?
            .into_iter()
            .map(|r| r.get(0))
            .collect();
        if rows == expected_all {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "streamed rows never converged: got {rows:?}, want {expected_all:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // A deliberate stop parks the workflow.
    registry.stop("wf1").await;
    wait_for(&registry, "wf1", "the stopped phase", |s| {
        s.phase == v1::WorkflowPhase::Stopped as i32
    })
    .await;
    Ok(())
}

#[tokio::test]
async fn rejects_specs_the_runner_cannot_execute_honestly() -> anyhow::Result<()> {
    let pg = Pg::start().await?;
    let config = WorkflowConfig {
        target: "host=localhost user=postgres".parse()?,
        source_user: "postgres".into(),
        source_password: "postgres".into(),
    };
    let registry = WorkflowRegistry::default();

    let mut bad = spec(&pg, "wfx", "wfx_slot");
    bad.tables[0].shard_key_type = "float".into();
    assert!(matches!(
        registry.start(&bad, &config).await,
        Err(WorkflowError::Invalid(_))
    ));

    let mut renamed = spec(&pg, "wfy", "wfy_slot");
    renamed.tables[0].target = Some(v1::TableRef {
        schema: "public".into(),
        name: "orders_v2".into(),
    });
    assert!(matches!(
        registry.start(&renamed, &config).await,
        Err(WorkflowError::Unimplemented(..))
    ));

    let mut standby = spec(&pg, "wfz", "wfz_slot");
    standby.source_policy = v1::SourcePolicy::PreferStandby as i32;
    assert!(matches!(
        registry.start(&standby, &config).await,
        Err(WorkflowError::Unimplemented(..))
    ));

    let mut inject = spec(&pg, "wfi", "bad-slot;DROP");
    inject.id = "wfi".into();
    assert!(matches!(
        registry.start(&inject, &config).await,
        Err(WorkflowError::Invalid(_))
    ));
    Ok(())
}
