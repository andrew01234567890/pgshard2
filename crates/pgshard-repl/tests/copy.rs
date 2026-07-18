//! Live end-to-end test: a filtered snapshot copy moves only the rows whose
//! shard key hashes into the target range, and the keep decision matches the
//! streaming filter (both go through pgshard_core's shard function).

use pgshard_core::{KeyRange, ScalarType, ScalarValue, shard_function};
use pgshard_repl::client::{Config, ReplicationClient};
use pgshard_repl::copy::{CopySpec, copy_filtered};
use pgshard_testutil::Pg;

async fn connect_db(pg: &Pg, dbname: &str) -> anyhow::Result<tokio_postgres::Client> {
    let conn_str = format!(
        "host={} port={} user=postgres password=postgres dbname={dbname}",
        pg.host(),
        pg.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

const DDL: &str =
    "CREATE TABLE orders (id int PRIMARY KEY, customer_id int NOT NULL, note text, d date)";

#[tokio::test]
async fn copies_only_rows_in_the_target_range() -> anyhow::Result<()> {
    let pg = Pg::start().await?;
    let source = pg.connect().await?;

    // Seed the source and commit before the snapshot is exported so the copy sees
    // the rows. The note deliberately contains a tab and a backslash to exercise
    // COPY escaping on a passed-through (non-key) column.
    source.batch_execute(DDL).await?;
    const N: i32 = 40;
    for id in 1..=N {
        let note = format!("note-{id}\twith\\escapes");
        source
            .execute(
                "INSERT INTO orders (id, customer_id, note, d) VALUES ($1, $1, $2, date '2026-02-01')",
                &[&id, &note],
            )
            .await?;
    }
    // A hostile session DateStyle on the source: without the copier's ISO pin,
    // COPY TO would render 2026-02-01 as `01/02/2026`, which the target's
    // default MDY parsing stores as January 2nd.
    source.batch_execute("SET DateStyle = 'SQL, DMY'").await?;

    // A second database with the same table is the copy target, carrying an
    // ordinary trigger: replica session semantics must keep it from firing on
    // seeded rows (the source already materialized any trigger effects).
    source.batch_execute("CREATE DATABASE seed_target").await?;
    let target = connect_db(&pg, "seed_target").await?;
    target.batch_execute(DDL).await?;
    target
        .batch_execute(
            "CREATE TABLE audit (id int);
             CREATE FUNCTION audit_ins() RETURNS trigger LANGUAGE plpgsql AS
               $$ BEGIN INSERT INTO audit VALUES (NEW.id); RETURN NEW; END $$;
             CREATE TRIGGER orders_audit AFTER INSERT ON orders
               FOR EACH ROW EXECUTE FUNCTION audit_ins();",
        )
        .await?;

    // Export a snapshot via a slot the stream would later resume from.
    let config = Config {
        host: pg.host().to_owned(),
        port: pg.port(),
        user: "postgres".to_owned(),
        password: "postgres".to_owned(),
        database: "postgres".to_owned(),
    };
    let mut repl = ReplicationClient::connect(&config).await?;
    let snapshot = repl
        .create_logical_slot_exported("pgshard_copy_slot", true)
        .await?;

    // Copy the second half of the keyspace into the target.
    let shard_fn = shard_function("xxhash64_v1").unwrap();
    let target_range = KeyRange::new(0, None)?
        .split_evenly(2)?
        .into_iter()
        .nth(1)
        .unwrap();
    let columns = vec![
        "id".to_owned(),
        "customer_id".to_owned(),
        "note".to_owned(),
        "d".to_owned(),
    ];
    let spec = CopySpec {
        schema: "public",
        table: "orders",
        columns: &columns,
        shard_key_column: "customer_id",
        shard_key_type: ScalarType::Int,
        target_range,
    };
    let copied = copy_filtered(&source, &target, &snapshot, &spec, shard_fn).await?;

    // Keep the replication connection (which holds the exported snapshot) alive
    // until the copy has finished.
    drop(repl);

    // The expected set is computed the same way the stream filters: coerce the
    // key to its type, hash it, test the range.
    let expected: Vec<i32> = (1..=N)
        .filter(|&id| {
            let canonical = ScalarType::Int
                .coerce(&ScalarValue::Int64(i64::from(id)))
                .unwrap();
            target_range.contains(shard_fn.keyspace_id(&canonical))
        })
        .collect();
    assert!(
        !expected.is_empty() && expected.len() < N as usize,
        "the target range should be a proper, non-empty subset (got {})",
        expected.len()
    );
    assert_eq!(copied as usize, expected.len());

    let rows = target
        .query("SELECT id FROM orders ORDER BY id", &[])
        .await?;
    let got: Vec<i32> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(got, expected);

    // The passed-through column with tab/backslash survived byte-for-byte.
    let sample: String = target
        .query_one("SELECT note FROM orders ORDER BY id LIMIT 1", &[])
        .await?
        .get(0);
    assert!(sample.contains('\t') && sample.contains('\\'));

    // The ISO pin defeated the hostile DMY source DateStyle: every date arrived
    // as February 1st, not January 2nd.
    let wrong_dates: i64 = target
        .query_one(
            "SELECT count(*) FROM orders WHERE d IS DISTINCT FROM date '2026-02-01'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(
        wrong_dates, 0,
        "DateStyle pin failed: dates were re-parsed under a different style"
    );

    // Replica session semantics suppressed the target's ordinary trigger.
    let audit_rows: i64 = target
        .query_one("SELECT count(*) FROM audit", &[])
        .await?
        .get(0);
    assert_eq!(audit_rows, 0, "target trigger fired on seeded rows");
    Ok(())
}

/// RLS must fail the seed loudly, never silently omit rows: a non-superuser
/// seeding role reading an RLS-enabled table with `row_security = off` gets an
/// error instead of a policy-filtered (possibly empty) row set.
#[tokio::test]
async fn rls_fails_the_copy_loudly_instead_of_filtering() -> anyhow::Result<()> {
    let pg = Pg::start().await?;
    let admin = pg.connect().await?;
    admin.batch_execute(DDL).await?;
    admin
        .batch_execute(
            "INSERT INTO orders (id, customer_id, note) VALUES (1, 1, 'hidden');
             ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
             CREATE ROLE seeder LOGIN PASSWORD 'seeder';
             GRANT SELECT ON orders TO seeder;",
        )
        .await?;
    // Separate call: CREATE DATABASE cannot run inside the implicit transaction
    // a multi-statement batch gets.
    admin.batch_execute("CREATE DATABASE seed_rls").await?;
    let target = connect_db(&pg, "seed_rls").await?;
    target.batch_execute(DDL).await?;

    // The snapshot must come from a connection whose role can use it; export as
    // the superuser (snapshots are importable cross-role within the database).
    let config = Config {
        host: pg.host().to_owned(),
        port: pg.port(),
        user: "postgres".to_owned(),
        password: "postgres".to_owned(),
        database: "postgres".to_owned(),
    };
    let mut repl = ReplicationClient::connect(&config).await?;
    let snapshot = repl
        .create_logical_slot_exported("pgshard_rls_slot", true)
        .await?;

    // Source client as the RLS-subject role.
    let conn = format!(
        "host={} port={} user=seeder password=seeder dbname=postgres",
        pg.host(),
        pg.port()
    );
    let (seeder, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let shard_fn = shard_function("xxhash64_v1").unwrap();
    let columns = vec![
        "id".to_owned(),
        "customer_id".to_owned(),
        "note".to_owned(),
        "d".to_owned(),
    ];
    let spec = CopySpec {
        schema: "public",
        table: "orders",
        columns: &columns,
        shard_key_column: "customer_id",
        shard_key_type: ScalarType::Int,
        target_range: KeyRange::new(0, None)?,
    };
    let result = copy_filtered(&seeder, &target, &snapshot, &spec, shard_fn).await;
    drop(repl);

    assert!(
        result.is_err(),
        "an RLS-filtered seed must error, not silently omit rows"
    );
    let count: i64 = target
        .query_one("SELECT count(*) FROM orders", &[])
        .await?
        .get(0);
    assert_eq!(count, 0);
    Ok(())
}

/// A snapshot that passes the charset guard but does not exist: the import
/// fails inside the source transaction. The copy must report the error, leave
/// the source session usable (rolled back, not stranded in an aborted
/// transaction), and leave zero rows on the target.
#[tokio::test]
async fn failed_snapshot_import_leaves_sessions_clean() -> anyhow::Result<()> {
    let pg = Pg::start().await?;
    let source = pg.connect().await?;
    source.batch_execute(DDL).await?;
    source
        .execute(
            "INSERT INTO orders (id, customer_id, note) VALUES (1, 1, 'x')",
            &[],
        )
        .await?;
    source.batch_execute("CREATE DATABASE seed_fail").await?;
    let target = connect_db(&pg, "seed_fail").await?;
    target.batch_execute(DDL).await?;

    let shard_fn = shard_function("xxhash64_v1").unwrap();
    let columns = vec!["id".to_owned(), "customer_id".to_owned(), "note".to_owned()];
    let spec = CopySpec {
        schema: "public",
        table: "orders",
        columns: &columns,
        shard_key_column: "customer_id",
        shard_key_type: ScalarType::Int,
        target_range: KeyRange::new(0, None)?,
    };
    let err = copy_filtered(&source, &target, "deadbeef-1", &spec, shard_fn).await;
    assert!(err.is_err(), "nonexistent snapshot must fail the copy");

    // The source session must be immediately usable — a stranded aborted
    // transaction would fail this query with 25P02.
    let one: i32 = source.query_one("SELECT 1", &[]).await?.get(0);
    assert_eq!(one, 1);

    let count: i64 = target
        .query_one("SELECT count(*) FROM orders", &[])
        .await?
        .get(0);
    assert_eq!(count, 0);
    Ok(())
}
