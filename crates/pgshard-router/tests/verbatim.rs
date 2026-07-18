//! The verbatim (type-aware) backend preserves the backend's real column type
//! OIDs and its command tag, unlike the default text-mode path (which advertises
//! every column as text and rebuilds the tag from the leading keyword). Requires
//! Docker (testcontainers); runs in CI's rust lane.
//!
//! The test client is pgwire's own `PgWireClient`: a `tokio_postgres`
//! simple-query drops the `RowDescription` type OIDs (the very limitation the
//! verbatim backend removes), so it cannot observe the fix.

use std::sync::Arc;

use pgshard_router::Router;
use pgshard_router::wire::{Backend, Handlers, Proxy};
use pgshard_testutil::Pg;
use pgshard_topo::{
    Instance, ShardEntry, ShardKeyType, ShardState, TableEntry, TableType, Topology,
};

use pgwire::api::Type;
use pgwire::api::client::Config;
use pgwire::api::client::auth::DefaultStartupHandler;
use pgwire::api::client::query::{DefaultSimpleQueryHandler, Response};
use pgwire::error::PgWireClientError;
use pgwire::messages::response::CommandComplete;
use pgwire::tokio::client::PgWireClient;
use tokio::net::TcpListener;

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
            shard_key_type: Some(ShardKeyType::Int),
            sequences: Vec::new(),
        }],
        gates: Vec::new(),
        sequence_endpoint: None,
    }
}

/// Start a router over `proxy` in the background; returns its listening port.
async fn spawn_router(proxy: Arc<Proxy>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
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
    port
}

async fn client(port: u16) -> PgWireClient {
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user("app")
        .dbname("app");
    PgWireClient::connect(Arc::new(config), DefaultStartupHandler::new(), None)
        .await
        .expect("connect to router")
}

fn backend_creds() -> Backend {
    Backend {
        user: "postgres".into(),
        password: "postgres".into(),
        system_database: "postgres".into(),
    }
}

#[tokio::test]
async fn the_verbatim_backend_reports_real_column_types_and_the_backend_command_tag() {
    let pg = Pg::start().await.expect("start postgres");
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
    let topology = single_shard_topology(pg.host(), pg.port());

    // --- Default (verbatim) backend: real OIDs and the backend's command tag. --
    let verbatim = Arc::new(Proxy::new(
        pgshard_router::shared(Router::build(&topology).unwrap()),
        backend_creds(),
    ));
    let mut vclient = client(spawn_router(verbatim).await).await;

    let responses = vclient
        .simple_query(
            DefaultSimpleQueryHandler::new(),
            "SELECT customer_id, note FROM orders WHERE customer_id = 1",
        )
        .await
        .expect("verbatim select");
    let Some(Response::Query((_, fields, rows))) = responses.into_iter().next() else {
        panic!("expected a row-returning response");
    };
    assert_eq!(fields[0].datatype(), &Type::INT4, "customer_id is int4");
    assert_eq!(fields[1].datatype(), &Type::TEXT, "note is text");
    assert_eq!(rows.len(), 1);

    // A `SET` runs on a shard; the backend's verbatim tag is `SET`, not the
    // keyword-plus-count `SET 0` the text path reconstructs.
    let responses = vclient
        .simple_query(
            DefaultSimpleQueryHandler::new(),
            "SET application_name = 'x'",
        )
        .await
        .expect("verbatim set");
    let Some(Response::Execution(tag)) = responses.into_iter().next() else {
        panic!("expected a command response");
    };
    assert_eq!(CommandComplete::from(tag).tag, "SET");

    // --- Explicit text backend (the .text() escape hatch): the same select is
    // all-text, proving the fallback and that the two backends coexist. --------
    let text = Arc::new(
        Proxy::new(
            pgshard_router::shared(Router::build(&topology).unwrap()),
            backend_creds(),
        )
        .text(),
    );
    let mut dclient = client(spawn_router(text).await).await;

    let responses = dclient
        .simple_query(
            DefaultSimpleQueryHandler::new(),
            "SELECT customer_id, note FROM orders WHERE customer_id = 1",
        )
        .await
        .expect("default select");
    let Some(Response::Query((_, fields, _))) = responses.into_iter().next() else {
        panic!("expected a row-returning response");
    };
    assert_eq!(
        fields[0].datatype(),
        &Type::VARCHAR,
        "the text path advertises every column as text"
    );
    assert_eq!(fields[1].datatype(), &Type::VARCHAR);
}

/// Two serving shards (`sh0`, `sh1`) on the same node, splitting the keyspace at
/// 80, with a sharded `orders` table.
fn two_shard_topology(host: &str, port: u16) -> Topology {
    let shard = |name: &str, range: &str| ShardEntry {
        name: name.to_owned(),
        key_range: range.parse().unwrap(),
        state: ShardState::Serving,
        primary: Some(Instance {
            pod: format!("{name}-0"),
            host: host.into(),
            port,
            can_read: false,
        }),
        replicas: Vec::new(),
    };
    Topology {
        epoch: 1,
        topology_generation: 1,
        write_lease_seconds: 10,
        hash_function: "xxhash64_v1".into(),
        shards: vec![shard("sh0", "-80"), shard("sh1", "80-")],
        tables: vec![TableEntry {
            schema: "public".into(),
            name: "orders".into(),
            table_type: TableType::Sharded,
            shard_key_column: Some("customer_id".into()),
            shard_key_type: Some(ShardKeyType::Int),
            sequences: Vec::new(),
        }],
        gates: Vec::new(),
        sequence_endpoint: None,
    }
}

#[tokio::test]
async fn the_verbatim_backend_rejects_a_scatter_where_shards_disagree_on_column_type() {
    let pg = Pg::start().await.expect("start postgres");
    let admin = pg.connect().await.unwrap();
    admin.batch_execute("CREATE DATABASE sh0").await.unwrap();
    admin.batch_execute("CREATE DATABASE sh1").await.unwrap();
    // The same column name, a different type on each shard — a drift the text
    // backend cannot see (both would be VARCHAR) but the verbatim backend can.
    for (db, ty) in [("sh0", "text"), ("sh1", "int")] {
        let conn = format!(
            "host={} port={} user=postgres password=postgres dbname={db}",
            pg.host(),
            pg.port()
        );
        let (c, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        c.batch_execute(&format!("CREATE TABLE orders (customer_id int, note {ty})"))
            .await
            .unwrap();
    }

    let proxy = Arc::new(Proxy::new(
        pgshard_router::shared(Router::build(&two_shard_topology(pg.host(), pg.port())).unwrap()),
        backend_creds(),
    ));
    let mut client = client(spawn_router(proxy).await).await;

    // A keyless read scatters to both shards; their `note` columns have different
    // real types, so the scatter is rejected (0A000) rather than emitting one
    // shard's rows under the other's type.
    let err = client
        .simple_query(DefaultSimpleQueryHandler::new(), "SELECT note FROM orders")
        .await
        .expect_err("a type-drifted scatter must be rejected");
    match err {
        PgWireClientError::RemoteError(info) => assert_eq!(info.code, "0A000"),
        other => panic!("expected a 0A000 remote error, got {other:?}"),
    }
}

#[tokio::test]
async fn a_returning_write_reports_its_own_command_tag_not_select() {
    let pg = Pg::start().await.expect("start postgres");
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

    let proxy = Arc::new(Proxy::new(
        pgshard_router::shared(
            Router::build(&single_shard_topology(pg.host(), pg.port())).unwrap(),
        ),
        backend_creds(),
    ));
    let mut client = client(spawn_router(proxy).await).await;

    // A DELETE ... RETURNING is a row-returning write: it must report the DELETE
    // verb (the frontend appends the affected count), not SELECT.
    let responses = client
        .simple_query(
            DefaultSimpleQueryHandler::new(),
            "DELETE FROM orders WHERE customer_id = 1 RETURNING note",
        )
        .await
        .expect("delete returning");
    let Some(Response::Query((tag, _, rows))) = responses.into_iter().next() else {
        panic!("expected a row-returning response");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(CommandComplete::from(tag).tag, "DELETE 1");

    // SHOW is row-returning but its real tag `SHOW` carries no count; since the
    // frontend always appends one, the router keeps the default `SELECT n`
    // rather than emit a spurious `SHOW 1`.
    let responses = client
        .simple_query(DefaultSimpleQueryHandler::new(), "SHOW server_version")
        .await
        .expect("show");
    let Some(Response::Query((tag, _, _))) = responses.into_iter().next() else {
        panic!("expected a row-returning response");
    };
    assert_eq!(CommandComplete::from(tag).tag, "SELECT 1");
}

#[tokio::test]
async fn an_ordered_scatter_merges_shards_numerically() {
    let pg = Pg::start().await.expect("start postgres");
    let admin = pg.connect().await.unwrap();
    admin.batch_execute("CREATE DATABASE sh0").await.unwrap();
    admin.batch_execute("CREATE DATABASE sh1").await.unwrap();
    // Seed each shard directly so the values are placed deliberately across shards.
    for (db, ids) in [("sh0", [2, 100]), ("sh1", [9, 10])] {
        let conn = format!(
            "host={} port={} user=postgres password=postgres dbname={db}",
            pg.host(),
            pg.port()
        );
        let (c, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        c.batch_execute("CREATE TABLE orders (customer_id int, note text)")
            .await
            .unwrap();
        for id in ids {
            let id: i32 = id;
            c.execute(
                "INSERT INTO orders (customer_id, note) VALUES ($1, 'x')",
                &[&id],
            )
            .await
            .unwrap();
        }
    }

    let proxy = Arc::new(Proxy::new(
        pgshard_router::shared(Router::build(&two_shard_topology(pg.host(), pg.port())).unwrap()),
        backend_creds(),
    ));
    let conn = format!(
        "host=127.0.0.1 port={} user=app dbname=app",
        spawn_router(proxy).await
    );
    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);

    // A numeric ORDER BY scatter merges the shards in NUMERIC order — defeating
    // the byte-order trap, which would place 10 and 100 before 2 and 9.
    assert_eq!(
        ordered_ids(
            &client,
            "SELECT customer_id FROM orders ORDER BY customer_id"
        )
        .await,
        vec![2, 9, 10, 100]
    );
    assert_eq!(
        ordered_ids(
            &client,
            "SELECT customer_id FROM orders ORDER BY customer_id DESC"
        )
        .await,
        vec![100, 10, 9, 2]
    );

    // A scatter that also needs limiting or aggregation, or an ORDER BY on a type
    // the merge cannot handle soundly (text), is still rejected with 0A000.
    for q in [
        "SELECT customer_id FROM orders ORDER BY customer_id LIMIT 2",
        "SELECT count(*) FROM orders",
        "SELECT note FROM orders ORDER BY note",
    ] {
        let err = client.simple_query(q).await.unwrap_err();
        assert_eq!(err.code().map(|c| c.code()), Some("0A000"), "{q}");
    }
}

/// Run `query` through the router and collect its `customer_id` column, in the
/// order the rows arrive (the router's merge order).
async fn ordered_ids(client: &tokio_postgres::Client, query: &str) -> Vec<i32> {
    client
        .simple_query(query)
        .await
        .unwrap()
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                Some(r.get("customer_id").unwrap().parse::<i32>().unwrap())
            }
            _ => None,
        })
        .collect()
}
