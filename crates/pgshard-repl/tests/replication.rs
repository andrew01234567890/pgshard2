//! Live end-to-end test: a real INSERT streamed through the hand-rolled
//! replication client, the CopyData wrapper, and the pgoutput decoder.

use std::time::Duration;

use pgshard_repl::client::{Config, ReplicationClient};
use pgshard_repl::pgoutput::{LogicalRepMsg, PgOutputDecoder, TupleColumn};
use pgshard_testutil::Pg;

/// Drive the client until an Insert is decoded, returning its new-tuple column
/// text values. Bounded by a timeout so a protocol bug fails fast.
async fn read_insert(
    client: &mut ReplicationClient,
    decoder: &mut PgOutputDecoder,
) -> anyhow::Result<Vec<Vec<u8>>> {
    let read = async {
        loop {
            let Some(frame) = client.next().await? else {
                anyhow::bail!("stream ended before the insert arrived");
            };
            if let LogicalRepMsg::Insert(insert) = decoder.decode(&frame.data)? {
                let values = insert
                    .new_tuple
                    .columns
                    .iter()
                    .map(|c| match c {
                        TupleColumn::Text(bytes) => Ok(bytes.to_vec()),
                        other => anyhow::bail!("unexpected non-text column {other:?}"),
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                return Ok(values);
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(30), read)
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for the insert"))?
}

#[tokio::test]
async fn streams_a_real_insert_end_to_end() -> anyhow::Result<()> {
    let pg = Pg::start().await?;
    let setup = pg.connect().await?;
    setup
        .batch_execute(
            "CREATE TABLE orders (id int PRIMARY KEY, note text);
             CREATE PUBLICATION orders_pub FOR TABLE orders;",
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
    client.create_logical_slot("pgshard_test_slot", true).await?;
    client.start_replication("pgshard_test_slot", "orders_pub").await?;

    // Insert after streaming has started so the change is captured.
    setup
        .execute("INSERT INTO orders (id, note) VALUES (42, 'hi')", &[])
        .await?;

    let mut decoder = PgOutputDecoder::new(4);
    let values = read_insert(&mut client, &mut decoder).await?;
    assert_eq!(values, vec![b"42".to_vec(), b"hi".to_vec()]);
    Ok(())
}
