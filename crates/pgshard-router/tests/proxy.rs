//! End-to-end proxy test: a tokio-postgres client talks to the router, which
//! routes each query to a backing PostgreSQL container. Requires Docker
//! (testcontainers); run in CI's rust lane.

use std::sync::Arc;

use pgshard_router::Router;
use pgshard_router::wire::{Backend, Handlers, Proxy};
use pgshard_testutil::Pg;
use pgshard_topo::{Instance, ShardEntry, ShardState, TableEntry, TableType, Topology};
use tokio::net::TcpListener;

/// A topology with one serving shard (database `postgres`) over the whole
/// keyspace, whose primary is the given backend, and a sharded `orders` table.
fn single_shard_topology(host: &str, port: u16) -> Topology {
    Topology {
        epoch: 1,
        topology_generation: 1,
        write_lease_seconds: 10,
        hash_function: "xxhash64_v1".into(),
        shards: vec![ShardEntry {
            name: "postgres".into(),
            key_range: "-".parse().unwrap(),
            state: ShardState::Serving,
            primary: Some(Instance {
                pod: "pg-0".into(),
                host: host.into(),
                port,
                can_read: false,
            }),
            replicas: Vec::new(),
        }],
        tables: vec![TableEntry {
            schema: "public".into(),
            name: "orders".into(),
            table_type: TableType::Sharded,
            shard_key_column: Some("customer_id".into()),
            sequences: Vec::new(),
        }],
        gates: Vec::new(),
        sequence_endpoint: None,
    }
}

/// Start the router in the background; returns the address to connect a client.
async fn spawn_router(proxy: Arc<Proxy>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let handlers = Handlers::new(proxy.clone());
            tokio::spawn(async move {
                let _ = pgwire::tokio::process_socket(socket, None, handlers).await;
            });
        }
    });
    format!("host=127.0.0.1 port={} user=app dbname=app", addr.port())
}

#[tokio::test]
async fn routes_single_shard_reads_and_writes_through_to_the_backend() {
    let pg = Pg::start().await.expect("start postgres");

    // Seed the backend's `postgres` database directly.
    let backend = pg.connect().await.unwrap();
    backend
        .batch_execute("CREATE TABLE orders (customer_id int, note text)")
        .await
        .unwrap();
    backend
        .execute(
            "INSERT INTO orders (customer_id, note) VALUES (1, 'one')",
            &[],
        )
        .await
        .unwrap();

    let router = Arc::new(Router::build(&single_shard_topology(pg.host(), pg.port())).unwrap());
    let proxy = Arc::new(Proxy::new(
        router,
        Backend {
            user: "postgres".into(),
            password: "postgres".into(),
            system_database: "postgres".into(),
        },
    ));
    let conn = spawn_router(proxy).await;

    // A client speaks to the router, not the backend.
    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
        .await
        .expect("connect to router");
    tokio::spawn(connection);

    // A keyed read is routed to the shard and returns the seeded row.
    let rows = client
        .simple_query("SELECT note FROM orders WHERE customer_id = 1")
        .await
        .unwrap();
    let note = rows.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get("note").unwrap().to_owned()),
        _ => None,
    });
    assert_eq!(note.as_deref(), Some("one"));

    // A keyed write is routed too; read it back.
    client
        .simple_query("INSERT INTO orders (customer_id, note) VALUES (2, 'two')")
        .await
        .unwrap();
    let rows = client
        .simple_query("SELECT note FROM orders WHERE customer_id = 2")
        .await
        .unwrap();
    let note = rows.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get("note").unwrap().to_owned()),
        _ => None,
    });
    assert_eq!(note.as_deref(), Some("two"));

    // A cross-shard write is rejected with SQLSTATE 0A000, not mis-routed.
    let err = client
        .simple_query("UPDATE orders SET note = 'x'")
        .await
        .unwrap_err();
    assert_eq!(err.code().map(|c| c.code()), Some("0A000"));

    // A tableless read runs on a shard and returns a real row (liveness probe).
    let rows = client.simple_query("SELECT 1 AS one").await.unwrap();
    let one = rows.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get("one").unwrap().to_owned()),
        _ => None,
    });
    assert_eq!(one.as_deref(), Some("1"));

    // Explicit transaction control is rejected, not silently autocommitted.
    let err = client.simple_query("BEGIN").await.unwrap_err();
    assert_eq!(err.code().map(|c| c.code()), Some("0A000"));
}
