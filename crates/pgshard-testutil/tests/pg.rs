use pgshard_testutil::Pg;

#[tokio::test]
async fn starts_pg18_with_logical_wal() {
    let pg = Pg::start().await.unwrap();
    let client = pg.connect().await.unwrap();

    let version: String = client
        .query_one("SELECT current_setting('server_version')", &[])
        .await
        .unwrap()
        .get(0);
    assert!(
        version.starts_with("18"),
        "expected PostgreSQL 18, got {version}"
    );

    let wal_level: String = client
        .query_one("SELECT current_setting('wal_level')", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(wal_level, "logical");
}
