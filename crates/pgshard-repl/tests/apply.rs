//! Live test of the transactional applier, including the exactly-once guarantee
//! under a crash-and-replay: a transaction committed to the target but not yet
//! acknowledged to the slot must not be applied twice when the slot replays it.

use std::time::Duration;

use pgshard_repl::apply::Applier;
use pgshard_repl::client::{Config, ReplicationClient};
use pgshard_repl::pgoutput::{LogicalRepMsg, PgOutputDecoder};
use pgshard_testutil::Pg;

async fn connect_db(pg: &Pg, db: &str) -> anyhow::Result<tokio_postgres::Client> {
    let conn = format!(
        "host={} port={} user=postgres password=postgres dbname={db}",
        pg.host(),
        pg.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls).await?;
    tokio::spawn(connection);
    Ok(client)
}

/// Drive the stream until one transaction commits. Deliberately does **not**
/// confirm progress back to the slot, modelling a consumer that crashed after
/// committing to the target but before the slot advanced.
async fn apply_one_txn(
    client: &mut ReplicationClient,
    decoder: &mut PgOutputDecoder,
    applier: &mut Applier,
) -> anyhow::Result<()> {
    let run = async {
        loop {
            let frame = client
                .next()
                .await?
                .ok_or_else(|| anyhow::anyhow!("stream ended early"))?;
            let msg = decoder.decode(&frame.data)?;
            let committed = matches!(msg, LogicalRepMsg::Commit(_));
            applier.handle(&msg).await?;
            if committed {
                return anyhow::Ok(());
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(30), run)
        .await
        .map_err(|_| anyhow::anyhow!("timed out applying a transaction"))?
}

#[tokio::test]
async fn applies_a_transaction_exactly_once_across_a_slot_replay() -> anyhow::Result<()> {
    let pg = Pg::start().await?;

    // Source database: a published table.
    let source = pg.connect().await?;
    source
        .batch_execute(
            "CREATE TABLE orders (id int PRIMARY KEY, note text);
             CREATE PUBLICATION orders_pub FOR TABLE orders;",
        )
        .await?;

    // A separate target database with the same table shape.
    source.batch_execute("CREATE DATABASE target").await?;
    let target_checker = connect_db(&pg, "target").await?;
    target_checker
        .batch_execute("CREATE TABLE orders (id int PRIMARY KEY, note text)")
        .await?;

    let config = Config {
        host: pg.host().to_owned(),
        port: pg.port(),
        user: "postgres".to_owned(),
        password: "postgres".to_owned(),
        database: "postgres".to_owned(),
    };

    // A PERSISTENT slot so a reconnection replays the same changes.
    let mut client = ReplicationClient::connect(&config).await?;
    client.create_logical_slot("apply_slot", false).await?;
    client.start_replication("apply_slot", "orders_pub").await?;

    // One source transaction with two rows.
    source
        .batch_execute("INSERT INTO orders VALUES (1, 'a'), (2, 'b')")
        .await?;

    // Apply it (without confirming the slot).
    let mut applier = Applier::new(connect_db(&pg, "target").await?, "consumer-1").await?;
    let mut decoder = PgOutputDecoder::new(4);
    apply_one_txn(&mut client, &mut decoder, &mut applier).await?;

    let rows = target_checker
        .query("SELECT id, note FROM orders ORDER BY id", &[])
        .await?;
    let applied: Vec<(i32, String)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(applied, vec![(1, "a".to_owned()), (2, "b".to_owned())]);
    let checkpoint = applier.checkpoint();
    assert!(checkpoint.0 > 0, "a commit LSN should have been recorded");

    // Crash-and-replay: drop the connection (the slot never advanced), reconnect
    // the same slot so it re-streams the committed transaction, and apply with a
    // fresh applier that loads the persisted checkpoint.
    drop(client);
    let mut client = ReplicationClient::connect(&config).await?;
    client.start_replication("apply_slot", "orders_pub").await?;
    let mut applier = Applier::new(connect_db(&pg, "target").await?, "consumer-1").await?;
    assert_eq!(
        applier.checkpoint(),
        checkpoint,
        "the fresh applier must resume from the persisted checkpoint"
    );
    let mut decoder = PgOutputDecoder::new(4);
    apply_one_txn(&mut client, &mut decoder, &mut applier).await?;

    // The replayed transaction was at/below the checkpoint, so it was skipped —
    // the target still holds exactly the two rows, not four.
    let count: i64 = target_checker
        .query_one("SELECT count(*) FROM orders", &[])
        .await?
        .get(0);
    assert_eq!(
        count, 2,
        "the replayed transaction must not be applied twice"
    );
    Ok(())
}

#[tokio::test]
async fn applies_updates_and_deletes() -> anyhow::Result<()> {
    let pg = Pg::start().await?;
    let source = pg.connect().await?;
    source
        .batch_execute(
            "CREATE TABLE orders (id int PRIMARY KEY, note text);
             CREATE PUBLICATION orders_pub FOR TABLE orders;",
        )
        .await?;
    source.batch_execute("CREATE DATABASE target").await?;
    let target = connect_db(&pg, "target").await?;
    target
        .batch_execute("CREATE TABLE orders (id int PRIMARY KEY, note text)")
        .await?;

    let config = Config {
        host: pg.host().to_owned(),
        port: pg.port(),
        user: "postgres".to_owned(),
        password: "postgres".to_owned(),
        database: "postgres".to_owned(),
    };
    let mut client = ReplicationClient::connect(&config).await?;
    client.create_logical_slot("dml_slot", true).await?;
    client.start_replication("dml_slot", "orders_pub").await?;

    // Three source transactions: seed two rows, update one (by its PK, the default
    // replica identity), delete the other (streamed with its key).
    source
        .batch_execute("INSERT INTO orders VALUES (1, 'a'), (2, 'b')")
        .await?;
    source
        .batch_execute("UPDATE orders SET note = 'z' WHERE id = 1")
        .await?;
    source
        .batch_execute("DELETE FROM orders WHERE id = 2")
        .await?;

    let mut applier = Applier::new(connect_db(&pg, "target").await?, "dml").await?;
    let mut decoder = PgOutputDecoder::new(4);
    for _ in 0..3 {
        apply_one_txn(&mut client, &mut decoder, &mut applier).await?;
    }

    // id=1's note is updated, id=2 is gone.
    let rows = target
        .query("SELECT id, note FROM orders ORDER BY id", &[])
        .await?;
    let got: Vec<(i32, String)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(got, vec![(1, "z".to_owned())]);
    Ok(())
}

/// Replicated rows must not re-fire the target's ordinary triggers (the source
/// already materialized their effects), and the applier must expose a durable
/// ack position at least as far as its commit checkpoint.
#[tokio::test]
async fn replica_role_suppresses_target_triggers() -> anyhow::Result<()> {
    let pg = Pg::start().await?;
    let source = pg.connect().await?;
    source
        .batch_execute(
            "CREATE TABLE orders (id int PRIMARY KEY, note text);
             CREATE PUBLICATION trig_pub FOR TABLE orders;",
        )
        .await?;

    source.batch_execute("CREATE DATABASE trig_target").await?;
    let checker = connect_db(&pg, "trig_target").await?;
    checker
        .batch_execute(
            "CREATE TABLE orders (id int PRIMARY KEY, note text);
             CREATE TABLE audit (id int);
             CREATE FUNCTION audit_ins() RETURNS trigger LANGUAGE plpgsql AS
               $$ BEGIN INSERT INTO audit VALUES (NEW.id); RETURN NEW; END $$;
             CREATE TRIGGER orders_audit AFTER INSERT ON orders
               FOR EACH ROW EXECUTE FUNCTION audit_ins();",
        )
        .await?;

    let config = Config {
        host: pg.host().to_owned(),
        port: pg.port(),
        user: "postgres".to_owned(),
        password: "postgres".to_owned(),
        database: "postgres".to_owned(),
    };
    let mut client = ReplicationClient::connect(&config).await?;
    client.create_logical_slot("trig_slot", true).await?;
    client.start_replication("trig_slot", "trig_pub").await?;

    source
        .batch_execute("INSERT INTO orders VALUES (7, 'x')")
        .await?;

    let mut applier = Applier::new(connect_db(&pg, "trig_target").await?, "trig-consumer").await?;
    let mut decoder = PgOutputDecoder::new(4);
    apply_one_txn(&mut client, &mut decoder, &mut applier).await?;

    let applied: i64 = checker
        .query_one("SELECT count(*) FROM orders", &[])
        .await?
        .get(0);
    assert_eq!(applied, 1);
    let audit: i64 = checker
        .query_one("SELECT count(*) FROM audit", &[])
        .await?
        .get(0);
    assert_eq!(audit, 0, "target trigger fired on a replicated row");

    // The ack position covers at least the applied commit, so the slot can
    // release the transaction's WAL instead of replaying it forever.
    assert!(applier.checkpoint().0 > 0);
    assert!(applier.ack_lsn() >= applier.checkpoint());
    Ok(())
}
