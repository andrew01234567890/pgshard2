use pgshard_testutil::Pg;
use pgshard_wire::ProxyConfig;
use tokio::net::TcpListener;

async fn start_proxy(pg: &Pg) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = ProxyConfig {
        listen: addr,
        backend_host: pg.host().to_string(),
        backend_port: pg.port(),
        backend_password: "postgres".to_string(),
    };
    tokio::spawn(async move {
        let _ = pgshard_wire::run_on_listener(listener, config).await;
    });
    addr
}

async fn connect(addr: std::net::SocketAddr) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=postgres dbname=postgres",
            addr.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(connection);
    client
}

#[tokio::test]
async fn passes_through_queries_transactions_and_prepared_statements() {
    let pg = Pg::start().await.unwrap();
    let addr = start_proxy(&pg).await;
    let client = connect(addr).await;

    // Simple round trip.
    let one: i32 = client.query_one("SELECT 1", &[]).await.unwrap().get(0);
    assert_eq!(one, 1);

    // DDL + transaction + rows.
    client
        .batch_execute("CREATE TABLE t (id bigint PRIMARY KEY, v text)")
        .await
        .unwrap();
    client
        .batch_execute("BEGIN; INSERT INTO t VALUES (1, 'a'), (2, 'b'); COMMIT")
        .await
        .unwrap();

    // Extended protocol: prepared statement reused with parameters.
    let stmt = client
        .prepare("SELECT v FROM t WHERE id = $1")
        .await
        .unwrap();
    let v1: String = client.query_one(&stmt, &[&1i64]).await.unwrap().get(0);
    let v2: String = client.query_one(&stmt, &[&2i64]).await.unwrap().get(0);
    assert_eq!((v1.as_str(), v2.as_str()), ("a", "b"));

    // Errors pass through and the session stays usable.
    let err = client
        .query_one("SELECT no_such_col", &[])
        .await
        .unwrap_err();
    let db_err = err.as_db_error().expect("server error should pass through");
    assert!(db_err.message().contains("no_such_col"), "{db_err:?}");
    let two: i32 = client.query_one("SELECT 2", &[]).await.unwrap().get(0);
    assert_eq!(two, 2);

    // Concurrent sessions.
    let other = connect(addr).await;
    let n: i64 = other
        .query_one("SELECT count(*) FROM t", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 2);
}
