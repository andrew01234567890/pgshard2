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

const TARGET_PROVENANCE: &str = "shard-uid-1";

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
        expect_provenance: TARGET_PROVENANCE.into(),
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
    source
        .batch_execute(&format!(
            "COMMENT ON DATABASE shard_target IS 'pgshard-provenance:{TARGET_PROVENANCE}'"
        ))
        .await?;
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
        .start(&spec(&pg, "wf1", "pgshard_wf1"), &config)
        .await?;

    // Idempotent re-start with the identical spec; a different spec under the
    // same id is a conflict.
    registry
        .start(&spec(&pg, "wf1", "pgshard_wf1"), &config)
        .await?;
    match registry
        .start(&spec(&pg, "wf1", "pgshard_other"), &config)
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

    // A deliberate stop parks the workflow — and a restart racing the stop
    // must NEVER be acknowledged against the dying worker: it is either
    // refused as Stopping (retry) or actually replaces it. Looping on the
    // retry must end with a REAL new worker streaming again; the old bug
    // (silent ack, no worker) would leave the status parked at Stopped.
    registry.stop("wf1").await;
    loop {
        match registry
            .start(&spec(&pg, "wf1", "pgshard_wf1"), &config)
            .await
        {
            Ok(()) => break,
            Err(WorkflowError::Stopping(_)) => tokio::task::yield_now().await,
            other => panic!("restart during stop must be Stopping or success, got {other:?}"),
        }
    }
    wait_for(&registry, "wf1", "streaming again after the restart", |s| {
        s.phase == v1::WorkflowPhase::Streaming as i32
    })
    .await;
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

    let mut bad = spec(&pg, "wfx", "pgshard_wfx");
    bad.tables[0].shard_key_type = "float".into();
    assert!(matches!(
        registry.start(&bad, &config).await,
        Err(WorkflowError::Invalid(_))
    ));

    let mut renamed = spec(&pg, "wfy", "pgshard_wfy");
    renamed.tables[0].target = Some(v1::TableRef {
        schema: "public".into(),
        name: "orders_v2".into(),
    });
    assert!(matches!(
        registry.start(&renamed, &config).await,
        Err(WorkflowError::Unimplemented(..))
    ));

    let mut standby = spec(&pg, "wfz", "pgshard_wfz");
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

    let mut unprefixed = spec(&pg, "wfp", "wfp_slot");
    unprefixed.id = "wfp".into();
    assert!(matches!(
        registry.start(&unprefixed, &config).await,
        Err(WorkflowError::Invalid(_))
    ));

    let mut wrong_kind = spec(&pg, "wfk", "pgshard_wfk");
    wrong_kind.kind = v1::WorkflowKind::DdlShadow as i32;
    assert!(matches!(
        registry.start(&wrong_kind, &config).await,
        Err(WorkflowError::Invalid(_))
    ));

    let mut no_provenance = spec(&pg, "wfv", "pgshard_wfv");
    no_provenance.expect_provenance = String::new();
    assert!(matches!(
        registry.start(&no_provenance, &config).await,
        Err(WorkflowError::Invalid(_))
    ));

    let mut duplicated = spec(&pg, "wfd", "pgshard_wfd");
    duplicated.tables.push(duplicated.tables[0].clone());
    assert!(matches!(
        registry.start(&duplicated, &config).await,
        Err(WorkflowError::Invalid(_))
    ));
    Ok(())
}

async fn wait_for_error(registry: &WorkflowRegistry, id: &str, needle: &str) -> String {
    for _ in 0..300 {
        if let Some(status) = registry.statuses(&[id.to_owned()]).await.into_iter().next()
            && status.phase == v1::WorkflowPhase::Error as i32
        {
            assert!(
                status.error.contains(needle),
                "workflow {id} failed for the wrong reason: {}",
                status.error
            );
            return status.error;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("workflow {id} never reached the error phase");
}

#[tokio::test]
async fn preflight_refuses_destructive_work_before_touching_the_target() -> anyhow::Result<()> {
    let pg = Pg::start().await?;
    let source = pg.connect().await?;
    source
        .batch_execute(
            "CREATE TABLE orders (id int PRIMARY KEY, note text);
             CREATE PUBLICATION seed_pub FOR TABLE orders;",
        )
        .await?;
    source.batch_execute("CREATE DATABASE shard_target").await?;
    source
        .batch_execute(&format!(
            "COMMENT ON DATABASE shard_target IS 'pgshard-provenance:{TARGET_PROVENANCE}'"
        ))
        .await?;
    let target_conn = format!(
        "host={} port={} user=postgres password=postgres dbname=shard_target",
        pg.host(),
        pg.port()
    );
    let (target, conn) = tokio_postgres::connect(&target_conn, tokio_postgres::NoTls).await?;
    tokio::spawn(conn);
    target
        .batch_execute(
            "CREATE TABLE orders (id int PRIMARY KEY, note text);
             INSERT INTO orders VALUES (1, 'survivor')",
        )
        .await?;

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

    // Wrong provenance: the database is not the shard this workflow was
    // aimed at; nothing may be truncated.
    let mut foreign = spec(&pg, "wf_foreign", "pgshard_foreign");
    foreign.expect_provenance = "some-other-shard".into();
    registry.start(&foreign, &config).await?;
    wait_for_error(&registry, "wf_foreign", "provenance").await;

    // Declared shard-key type contradicting the real column type: hashing
    // would place rows on the wrong shard.
    let mut mistyped = spec(&pg, "wf_mistyped", "pgshard_mistyped");
    mistyped.tables[0].shard_key_type = "text".into();
    registry.start(&mistyped, &config).await?;
    wait_for_error(&registry, "wf_mistyped", "cannot be hashed").await;

    // A mapped table missing from the publication would be seeded once and
    // then silently go stale.
    let mut unpublished = spec(&pg, "wf_unpub", "pgshard_unpub");
    unpublished.publication = "absent_pub".into();
    registry.start(&unpublished, &config).await?;
    wait_for_error(&registry, "wf_unpub", "publication").await;

    // A publication with a disabled DML kind, a row filter, or a column list
    // would silently omit or transform streamed changes.
    source
        .batch_execute(
            "CREATE PUBLICATION pub_ins FOR TABLE orders WITH (publish = 'insert');
             CREATE PUBLICATION pub_filt FOR TABLE orders WHERE (id > 0);
             CREATE PUBLICATION pub_cols FOR TABLE orders (id);",
        )
        .await?;
    let mut insert_only = spec(&pg, "wf_ins", "pgshard_ins");
    insert_only.publication = "pub_ins".into();
    registry.start(&insert_only, &config).await?;
    wait_for_error(&registry, "wf_ins", "does not publish all").await;

    let mut filtered = spec(&pg, "wf_filt", "pgshard_filt");
    filtered.publication = "pub_filt".into();
    registry.start(&filtered, &config).await?;
    wait_for_error(&registry, "wf_filt", "filters rows").await;

    let mut col_listed = spec(&pg, "wf_cols", "pgshard_cols");
    col_listed.publication = "pub_cols".into();
    registry.start(&col_listed, &config).await?;
    wait_for_error(&registry, "wf_cols", "different column set").await;

    // Dynamic membership cannot be pinned by catalog row versions.
    source
        .batch_execute("CREATE PUBLICATION pub_all FOR ALL TABLES")
        .await?;
    let mut all_tables = spec(&pg, "wf_all", "pgshard_all");
    all_tables.publication = "pub_all".into();
    registry.start(&all_tables, &config).await?;
    wait_for_error(&registry, "wf_all", "FOR ALL TABLES").await;

    source
        .batch_execute("CREATE PUBLICATION pub_schema FOR TABLES IN SCHEMA public")
        .await?;
    let mut in_schema = spec(&pg, "wf_schema", "pgshard_schema");
    in_schema.publication = "pub_schema".into();
    registry.start(&in_schema, &config).await?;
    wait_for_error(&registry, "wf_schema", "TABLES IN SCHEMA").await;

    // A partition-root publication announces leaf relations the mapping
    // does not know, and direct leaf truncates go unpublished.
    source
        .batch_execute(
            "CREATE TABLE measurements (id int NOT NULL, reading int)
                 PARTITION BY RANGE (id);
             CREATE TABLE measurements_low PARTITION OF measurements
                 FOR VALUES FROM (MINVALUE) TO (1000);
             CREATE PUBLICATION pub_root FOR TABLE measurements
                 WITH (publish_via_partition_root = true);",
        )
        .await?;
    let mut partitioned = spec(&pg, "wf_part", "pgshard_part");
    partitioned.publication = "pub_root".into();
    partitioned.tables[0].source = Some(v1::TableRef {
        schema: "public".into(),
        name: "measurements".into(),
    });
    registry.start(&partitioned, &config).await?;
    wait_for_error(&registry, "wf_part", "publish_via_partition_root").await;

    // A same-named target column of a different type would truncate first
    // and fail mid-copy without the deep compatibility check.
    source
        .batch_execute("CREATE TABLE mistyped_target (id int PRIMARY KEY, note text)")
        .await?;
    target
        .batch_execute("CREATE TABLE mistyped_target (id int PRIMARY KEY, note int)")
        .await?;
    source
        .batch_execute("CREATE PUBLICATION pub_mt FOR TABLE mistyped_target")
        .await?;
    let mut mistyped_tgt = spec(&pg, "wf_mt", "pgshard_mt");
    mistyped_tgt.publication = "pub_mt".into();
    mistyped_tgt.tables[0].source = Some(v1::TableRef {
        schema: "public".into(),
        name: "mistyped_target".into(),
    });
    registry.start(&mistyped_tgt, &config).await?;
    wait_for_error(&registry, "wf_mt", "different type on the target").await;

    // Exact schema equivalence: an extra target column's defaults or
    // constraints could reject or transform copied rows after truncation.
    source
        .batch_execute("CREATE TABLE extra_col (id int PRIMARY KEY, note text)")
        .await?;
    target
        .batch_execute("CREATE TABLE extra_col (id int PRIMARY KEY, note text, added timestamptz)")
        .await?;
    source
        .batch_execute("CREATE PUBLICATION pub_extra FOR TABLE extra_col")
        .await?;
    let mut extra = spec(&pg, "wf_extra", "pgshard_extra");
    extra.publication = "pub_extra".into();
    extra.tables[0].source = Some(v1::TableRef {
        schema: "public".into(),
        name: "extra_col".into(),
    });
    registry.start(&extra, &config).await?;
    wait_for_error(&registry, "wf_extra", "extra column").await;

    // Generated columns cannot be compared across databases and are refused.
    source
        .batch_execute(
            "CREATE TABLE gen_col (id int PRIMARY KEY, doubled int GENERATED ALWAYS AS (id * 2) STORED)",
        )
        .await?;
    source
        .batch_execute("CREATE PUBLICATION pub_gen FOR TABLE gen_col")
        .await?;
    let mut generated = spec(&pg, "wf_gen", "pgshard_gen");
    generated.publication = "pub_gen".into();
    generated.tables[0].source = Some(v1::TableRef {
        schema: "public".into(),
        name: "gen_col".into(),
    });
    registry.start(&generated, &config).await?;
    wait_for_error(&registry, "wf_gen", "generated").await;

    // REPLICA IDENTITY FULL is refused up front: the applier cannot apply
    // FULL-identity updates/deletes, so accepting it would re-seed
    // destructively and then fail on the first mutation.
    source
        .batch_execute("ALTER TABLE orders REPLICA IDENTITY FULL")
        .await?;
    registry
        .start(&spec(&pg, "wf_full", "pgshard_full"), &config)
        .await?;
    wait_for_error(&registry, "wf_full", "replica identity").await;
    source
        .batch_execute("ALTER TABLE orders REPLICA IDENTITY DEFAULT")
        .await?;

    let survivors: i64 = target
        .query_one("SELECT count(*) FROM orders", &[])
        .await?
        .get(0);
    assert_eq!(
        survivors, 1,
        "every refused start must leave the target untouched"
    );

    // A second RUNNING workflow under a different id may not seize the same
    // target database.
    registry
        .start(&spec(&pg, "wf_a", "pgshard_wfa"), &config)
        .await?;
    match registry
        .start(&spec2(&pg, "wf_b", "pgshard_wfb"), &config)
        .await
    {
        Err(WorkflowError::TargetBusy(id, db)) => {
            assert_eq!(id, "wf_a");
            assert_eq!(db, "shard_target");
        }
        other => panic!("a busy target must be refused, got {other:?}"),
    }
    Ok(())
}

/// Same as spec() but a genuinely different byte encoding under a new id, so
/// the same-id idempotency path cannot mask the target-busy check.
fn spec2(pg: &Pg, id: &str, slot: &str) -> v1::WorkflowSpec {
    let mut s = spec(pg, id, slot);
    s.source_shard = "src2".into();
    s
}

#[tokio::test]
async fn update_moving_the_shard_key_across_the_boundary_fails_loudly() -> anyhow::Result<()> {
    let pg = Pg::start().await?;
    let source = pg.connect().await?;
    source
        .batch_execute(
            "CREATE TABLE orders (id int PRIMARY KEY, note text);
             CREATE PUBLICATION seed_pub FOR TABLE orders;",
        )
        .await?;
    source.batch_execute("CREATE DATABASE shard_target").await?;
    source
        .batch_execute(&format!(
            "COMMENT ON DATABASE shard_target IS 'pgshard-provenance:{TARGET_PROVENANCE}'"
        ))
        .await?;
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

    let out_of_range = (1..100).find(|id| !in_upper_half(*id)).unwrap();
    let in_range = (1..100).find(|id| in_upper_half(*id)).unwrap();
    source
        .execute(
            "INSERT INTO orders VALUES ($1, 'movable')",
            &[&out_of_range],
        )
        .await?;

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
        .start(&spec(&pg, "wf_move", "pgshard_move"), &config)
        .await?;
    wait_for(&registry, "wf_move", "the streaming phase", |s| {
        s.phase == v1::WorkflowPhase::Streaming as i32
    })
    .await;

    // The router forbids shard-key updates; a direct write bypasses it. The
    // stream must refuse the boundary crossing instead of silently keeping a
    // stale (or missing) target row.
    source
        .execute(
            "UPDATE orders SET id = $1 WHERE id = $2",
            &[&in_range, &out_of_range],
        )
        .await?;
    wait_for_error(&registry, "wf_move", "across the target range boundary").await;
    Ok(())
}

#[tokio::test]
async fn dropping_replica_identity_mid_stream_fails_loudly() -> anyhow::Result<()> {
    let pg = Pg::start().await?;
    let source = pg.connect().await?;
    source
        .batch_execute(
            "CREATE TABLE orders (id int PRIMARY KEY, note text);
             CREATE PUBLICATION seed_pub FOR TABLE orders;",
        )
        .await?;
    source.batch_execute("CREATE DATABASE shard_target").await?;
    source
        .batch_execute(&format!(
            "COMMENT ON DATABASE shard_target IS 'pgshard-provenance:{TARGET_PROVENANCE}'"
        ))
        .await?;
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

    let in_range = (1..100).find(|id| in_upper_half(*id)).unwrap();
    source
        .execute("INSERT INTO orders VALUES ($1, 'kept')", &[&in_range])
        .await?;

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
        .start(&spec(&pg, "wf_ident", "pgshard_ident"), &config)
        .await?;
    wait_for(&registry, "wf_ident", "the streaming phase", |s| {
        s.phase == v1::WorkflowPhase::Streaming as i32
    })
    .await;

    // Preflight passed with the PK identity; the ALTER re-sends the Relation
    // with FULL, under which the stream can no longer see shard-key changes
    // the way the boundary check needs — it must fail, not guess.
    source
        .batch_execute("ALTER TABLE orders REPLICA IDENTITY FULL")
        .await?;
    source
        .execute(
            "UPDATE orders SET note = 'poked' WHERE id = $1",
            &[&in_range],
        )
        .await?;
    wait_for_error(&registry, "wf_ident", "no longer covers shard key").await;
    Ok(())
}

#[tokio::test]
async fn altering_the_publication_mid_stream_fails_loudly() -> anyhow::Result<()> {
    let pg = Pg::start().await?;
    let source = pg.connect().await?;
    source
        .batch_execute(
            "CREATE TABLE orders (id int PRIMARY KEY, note text);
             CREATE PUBLICATION seed_pub FOR TABLE orders;",
        )
        .await?;
    source.batch_execute("CREATE DATABASE shard_target").await?;
    source
        .batch_execute(&format!(
            "COMMENT ON DATABASE shard_target IS 'pgshard-provenance:{TARGET_PROVENANCE}'"
        ))
        .await?;
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
        .start(&spec(&pg, "wf_pubdrift", "pgshard_pubdrift"), &config)
        .await?;
    wait_for(&registry, "wf_pubdrift", "the streaming phase", |s| {
        s.phase == v1::WorkflowPhase::Streaming as i32
    })
    .await;

    // A transient toggle — disable a DML kind and RESTORE it before the next
    // poll — leaves the sampled shape identical, but every ALTER PUBLICATION
    // rewrites the catalog row and xids never repeat, so the captured row
    // version cannot match. The workflow must still fail.
    source
        .batch_execute(
            "ALTER PUBLICATION seed_pub SET (publish = 'insert');
             ALTER PUBLICATION seed_pub SET (publish = 'insert, update, delete, truncate');",
        )
        .await?;
    wait_for_error(&registry, "wf_pubdrift", "changed while streaming").await;
    Ok(())
}
