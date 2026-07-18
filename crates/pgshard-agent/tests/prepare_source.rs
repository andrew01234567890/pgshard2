//! Live tests of PrepareSource: the source-side publication provisioner must
//! produce EXACTLY the shape the seeding runner's preflight accepts, be a
//! strict no-op when the shape already holds (a reconcile retry must never
//! rewrite catalog rows a live consumer's drift poll has pinned), and
//! converge a mismatched same-name publication.

use pgshard_agent::instance::Instance;
use pgshard_agent::pg::PgInstance;
use pgshard_agent::workflow::{WorkflowConfig, WorkflowRegistry};
use pgshard_proto::v1;
use pgshard_testutil::Pg;

fn conninfo(pg: &Pg, db: &str) -> String {
    format!(
        "host={} port={} user=postgres password=postgres dbname={db}",
        pg.host(),
        pg.port()
    )
}

async fn pub_xmin(client: &tokio_postgres::Client, name: &str) -> String {
    client
        .query_one(
            "SELECT xmin::text FROM pg_publication WHERE pubname = $1",
            &[&name],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
async fn provisions_verifies_and_converges_the_publication() -> anyhow::Result<()> {
    let pg = Pg::start().await?;
    let admin = pg.connect().await?;
    admin.batch_execute("CREATE DATABASE shard_src").await?;
    let (src, conn) =
        tokio_postgres::connect(&conninfo(&pg, "shard_src"), tokio_postgres::NoTls).await?;
    tokio::spawn(conn);
    src.batch_execute(
        "CREATE TABLE orders (id int PRIMARY KEY, note text);
         CREATE TABLE items (id int PRIMARY KEY, sku text)",
    )
    .await?;

    let instance = PgInstance::new(conninfo(&pg, "postgres"));
    let tables = vec![
        ("public".to_string(), "orders".to_string()),
        ("public".to_string(), "items".to_string()),
    ];
    let headroom = instance
        .prepare_source("shard_src", "pgshard_seed", &tables)
        .await?;
    // testutil does not set max_slot_wal_keep_size, so retention is unlimited.
    assert_eq!(headroom, None);

    let shape: (bool, bool, bool, bool, bool, bool) = {
        let r = src
            .query_one(
                "SELECT pubinsert, pubupdate, pubdelete, pubtruncate, puballtables, pubviaroot
                 FROM pg_publication WHERE pubname = 'pgshard_seed'",
                &[],
            )
            .await?;
        (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5))
    };
    assert_eq!(shape, (true, true, true, true, false, false));
    let members: Vec<String> = src
        .query(
            "SELECT tablename::text FROM pg_publication_tables
             WHERE pubname = 'pgshard_seed' ORDER BY tablename",
            &[],
        )
        .await?
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(members, vec!["items".to_string(), "orders".to_string()]);

    // Idempotent re-prepare: the catalog row must NOT be rewritten — its xmin
    // is exactly what a live consumer's drift poll pins.
    let before = pub_xmin(&src, "pgshard_seed").await;
    instance
        .prepare_source("shard_src", "pgshard_seed", &tables)
        .await?;
    assert_eq!(
        pub_xmin(&src, "pgshard_seed").await,
        before,
        "a no-op re-prepare must leave the publication row untouched"
    );

    // A FULL column list (naming every current column) must be treated as
    // mismatched — the underlying prattrs freezes the published set even
    // though attnames looks complete.
    src.batch_execute(
        "DROP PUBLICATION pgshard_seed;
         CREATE PUBLICATION pgshard_seed FOR TABLE orders (id, note), items (id, sku)",
    )
    .await?;
    instance
        .prepare_source("shard_src", "pgshard_seed", &tables)
        .await?;
    let frozen: i64 = src
        .query_one(
            "SELECT count(*) FROM pg_publication_rel pr
             JOIN pg_publication p ON p.oid = pr.prpubid
             WHERE p.pubname = 'pgshard_seed' AND pr.prattrs IS NOT NULL",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(
        frozen, 0,
        "a full-but-frozen column list must be converged away"
    );

    // A mismatched same-name publication converges to the full shape.
    src.batch_execute(
        "DROP PUBLICATION pgshard_seed;
         CREATE PUBLICATION pgshard_seed FOR TABLE orders WITH (publish = 'insert')",
    )
    .await?;
    instance
        .prepare_source("shard_src", "pgshard_seed", &tables)
        .await?;
    let (trunc, count): (bool, i64) = {
        let r = src
            .query_one(
                "SELECT p.pubtruncate,
                        (SELECT count(*) FROM pg_publication_tables pt
                         WHERE pt.pubname = p.pubname)
                 FROM pg_publication p WHERE p.pubname = 'pgshard_seed'",
                &[],
            )
            .await?;
        (r.get(0), r.get(1))
    };
    assert!(
        trunc && count == 2,
        "the mismatched publication must converge"
    );

    // Generated-column tables are refused up front: the runner cannot stream
    // them, so provisioning would only defer the failure.
    src.batch_execute(
        "CREATE TABLE gen_src (id int PRIMARY KEY, doubled int GENERATED ALWAYS AS (id * 2) STORED)",
    )
    .await?;
    let err = instance
        .prepare_source(
            "shard_src",
            "pgshard_gen",
            &[("public".to_string(), "gen_src".to_string())],
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("generated"));
    Ok(())
}

#[tokio::test]
async fn a_prepared_publication_satisfies_the_seeding_runner() -> anyhow::Result<()> {
    let pg = Pg::start().await?;
    let admin = pg.connect().await?;
    admin.batch_execute("CREATE DATABASE shard_src").await?;
    admin.batch_execute("CREATE DATABASE shard_tgt").await?;
    admin
        .batch_execute("COMMENT ON DATABASE shard_tgt IS 'pgshard-provenance:tgt-uid'")
        .await?;
    let (src, conn) =
        tokio_postgres::connect(&conninfo(&pg, "shard_src"), tokio_postgres::NoTls).await?;
    tokio::spawn(conn);
    src.batch_execute(
        "CREATE TABLE orders (id int PRIMARY KEY, note text);
         INSERT INTO orders VALUES (1, 'row')",
    )
    .await?;
    let (tgt, conn) =
        tokio_postgres::connect(&conninfo(&pg, "shard_tgt"), tokio_postgres::NoTls).await?;
    tokio::spawn(conn);
    tgt.batch_execute("CREATE TABLE orders (id int PRIMARY KEY, note text)")
        .await?;

    let instance = PgInstance::new(conninfo(&pg, "postgres"));
    instance
        .prepare_source(
            "shard_src",
            "pgshard_seed",
            &[("public".to_string(), "orders".to_string())],
        )
        .await?;

    // The two contracts must meet: a runner pointed at the prepared
    // publication passes preflight and reaches STREAMING.
    let registry = WorkflowRegistry::default();
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
    let spec = v1::WorkflowSpec {
        id: "wf_prep".into(),
        kind: v1::WorkflowKind::Reshard as i32,
        source_shard: "src".into(),
        source_primary: Some(v1::PgEndpoint {
            host: pg.host().to_owned(),
            port: pg.port() as u32,
            database: "shard_src".into(),
        }),
        source_policy: v1::SourcePolicy::Primary as i32,
        slot: "pgshard_prep".into(),
        publication: "pgshard_seed".into(),
        tables: vec![v1::TableMapping {
            source: Some(v1::TableRef {
                schema: "public".into(),
                name: "orders".into(),
            }),
            shard_key_column: "id".into(),
            shard_key_type: "int".into(),
            ..Default::default()
        }],
        filter: Some(v1::RowFilter {
            filter: Some(v1::row_filter::Filter::All(true)),
        }),
        target_database: "shard_tgt".into(),
        expect_provenance: "tgt-uid".into(),
        ..Default::default()
    };
    registry.start(&spec, &config).await?;
    for _ in 0..300 {
        if let Some(status) = registry
            .statuses(&["wf_prep".to_owned()])
            .await
            .into_iter()
            .next()
        {
            assert!(
                status.phase != v1::WorkflowPhase::Error as i32,
                "workflow failed against the prepared publication: {}",
                status.error
            );
            if status.phase == v1::WorkflowPhase::Streaming as i32 {
                return Ok(());
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("the runner never reached streaming against the prepared publication");
}
