//! End-to-end proxy test: a tokio-postgres client talks to the router, which
//! routes each query to a backing PostgreSQL container. Requires Docker
//! (testcontainers); run in CI's rust lane.

use std::sync::Arc;

use pgshard_router::Router;
use pgshard_router::sequence::PgBlockReserver;
use pgshard_router::wire::{Backend, Handlers, Proxy};
use pgshard_seq::SequenceCache;
use pgshard_testutil::Pg;
use pgshard_topo::{
    Instance, Sequence, ShardEntry, ShardKeyType, ShardState, TableEntry, TableType, Topology,
    TopologyWatcher,
};
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
            shard_key_type: Some(ShardKeyType::Int),
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

    let router = pgshard_router::shared(
        Router::build(&single_shard_topology(pg.host(), pg.port())).unwrap(),
    );
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

    // A leading comment must not smuggle transaction control past the reject:
    // the classification is from the parsed AST, not the leading token. A
    // client that believes it opened a transaction while its statements
    // autocommit would be a wrong-data outcome.
    let err = client.simple_query("/* x */ BEGIN").await.unwrap_err();
    assert_eq!(err.code().map(|c| c.code()), Some("0A000"));
    let err = client
        .simple_query("/* sneak */ SAVEPOINT s1")
        .await
        .unwrap_err();
    assert_eq!(err.code().map(|c| c.code()), Some("0A000"));
}

/// A topology with two serving shards (databases `sh0`, `sh1`) on the same node,
/// splitting the keyspace at 80, and a sharded `orders` table.
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
async fn scatter_reads_concatenate_rows_from_every_shard() {
    let pg = Pg::start().await.expect("start postgres");

    // Two shard databases on the one node, each seeded with one row.
    let admin = pg.connect().await.unwrap();
    admin.batch_execute("CREATE DATABASE sh0").await.unwrap();
    admin.batch_execute("CREATE DATABASE sh1").await.unwrap();
    for (db, note) in [("sh0", "from-sh0"), ("sh1", "from-sh1")] {
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
        c.execute(
            "INSERT INTO orders (customer_id, note) VALUES (1, $1)",
            &[&note],
        )
        .await
        .unwrap();
    }

    let router =
        pgshard_router::shared(Router::build(&two_shard_topology(pg.host(), pg.port())).unwrap());
    let proxy = Arc::new(Proxy::new(
        router,
        Backend {
            user: "postgres".into(),
            password: "postgres".into(),
            system_database: "postgres".into(),
        },
    ));
    let conn = spawn_router(proxy).await;
    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);

    // A keyless read scatters to both shards and returns both rows.
    let rows = client
        .simple_query("SELECT note FROM orders")
        .await
        .unwrap();
    let mut notes: Vec<String> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get("note").unwrap().to_owned()),
            _ => None,
        })
        .collect();
    notes.sort();
    assert_eq!(notes, vec!["from-sh0".to_string(), "from-sh1".to_string()]);

    // A scatter that needs a real merge is rejected, not mis-answered.
    let err = client
        .simple_query("SELECT note FROM orders ORDER BY note")
        .await
        .unwrap_err();
    assert_eq!(err.code().map(|c| c.code()), Some("0A000"));

    // If the shards disagree on the result shape (e.g. a broadcast DDL still
    // rolling out), the scatter fails cleanly with an error — it must not panic
    // the connection or encode rows under the wrong schema.
    let sh1_conn = format!(
        "host={} port={} user=postgres password=postgres dbname=sh1",
        pg.host(),
        pg.port()
    );
    let (sh1, connection) = tokio_postgres::connect(&sh1_conn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    sh1.batch_execute("ALTER TABLE orders ADD COLUMN extra text")
        .await
        .unwrap();
    let err = client
        .simple_query("SELECT * FROM orders")
        .await
        .unwrap_err();
    assert_eq!(err.code().map(|c| c.code()), Some("0A000"));
    // The client session is still alive after the clean error (not a dropped
    // connection): a subsequent query still works.
    let alive = client.simple_query("SELECT 1 AS ok").await;
    assert!(alive.is_ok(), "the connection survived the scatter error");
}

#[tokio::test]
async fn quoted_and_bare_integer_keys_route_to_the_same_shard() {
    let pg = Pg::start().await.expect("start postgres");

    // Two shard databases on the one node, each with an empty orders table.
    let admin = pg.connect().await.unwrap();
    admin.batch_execute("CREATE DATABASE sh0").await.unwrap();
    admin.batch_execute("CREATE DATABASE sh1").await.unwrap();
    for db in ["sh0", "sh1"] {
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
    }

    let router =
        pgshard_router::shared(Router::build(&two_shard_topology(pg.host(), pg.port())).unwrap());
    let proxy = Arc::new(Proxy::new(
        router,
        Backend {
            user: "postgres".into(),
            password: "postgres".into(),
            system_database: "postgres".into(),
        },
    ));
    let conn = spawn_router(proxy).await;
    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);

    // customer_id = 2 hashes to a different shard as an integer (sh1) than as
    // text (sh0). Without type-aware coercion the bare-integer write and the
    // quoted read would land on different shards and the read would miss the
    // row; coercion makes both route as the integer key.
    client
        .simple_query("INSERT INTO orders (customer_id, note) VALUES (2, 'keyed')")
        .await
        .unwrap();
    let rows = client
        .simple_query("SELECT note FROM orders WHERE customer_id = '2'")
        .await
        .unwrap();
    let note = rows.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get("note").unwrap().to_owned()),
        _ => None,
    });
    assert_eq!(
        note.as_deref(),
        Some("keyed"),
        "the quoted-int read must route to the same shard as the bare-int write"
    );
}

/// A one-shard topology whose `orders` table binds its `id` column to the
/// `orders_id` global sequence.
fn sequenced_topology(host: &str, port: u16) -> Topology {
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
            sequences: vec![Sequence {
                column: "id".into(),
                sequence: "orders_id".into(),
            }],
        }],
        gates: Vec::new(),
        sequence_endpoint: None,
    }
}

#[tokio::test]
async fn an_omitted_sequence_column_is_filled_from_the_system_database() {
    let pg = Pg::start().await.expect("start postgres");

    // Shard database: `id` is NOT NULL with no default, so an INSERT that omits
    // it only succeeds if the router fills it.
    let backend = pg.connect().await.unwrap();
    backend
        .batch_execute("CREATE TABLE orders (id bigint PRIMARY KEY, customer_id int, note text)")
        .await
        .unwrap();

    // System database holds the sequence catalog.
    backend
        .batch_execute("CREATE DATABASE pgshard_system")
        .await
        .unwrap();
    let sys_conn = format!(
        "host={} port={} user=postgres password=postgres dbname=pgshard_system",
        pg.host(),
        pg.port()
    );
    let (sys, connection) = tokio_postgres::connect(&sys_conn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    sys.batch_execute(
        "CREATE SCHEMA pgshard; \
         CREATE TABLE pgshard.sequences ( \
             name text PRIMARY KEY, next_id bigint NOT NULL, block_size bigint NOT NULL); \
         INSERT INTO pgshard.sequences VALUES ('orders_id', 1, 100);",
    )
    .await
    .unwrap();

    let router =
        pgshard_router::shared(Router::build(&sequenced_topology(pg.host(), pg.port())).unwrap());
    let seq = Arc::new(SequenceCache::new(PgBlockReserver::new(
        sys_conn.parse().unwrap(),
    )));
    let proxy = Arc::new(Proxy::with_sequences(
        router,
        Backend {
            user: "postgres".into(),
            password: "postgres".into(),
            system_database: "pgshard_system".into(),
        },
        seq,
    ));
    let conn = spawn_router(proxy).await;
    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);

    // A single-row INSERT that omits `id` succeeds — the id is filled in.
    client
        .simple_query("INSERT INTO orders (customer_id, note) VALUES (1, 'x')")
        .await
        .expect("insert with an injected id");

    // A multi-row INSERT gets a distinct id per row (a duplicate would violate
    // the primary key).
    client
        .simple_query("INSERT INTO orders (customer_id, note) VALUES (2, 'a'), (2, 'b')")
        .await
        .expect("multi-row insert with injected ids");

    // INSERT ... RETURNING id surfaces the injected id to the client.
    let rows = client
        .simple_query("INSERT INTO orders (customer_id, note) VALUES (3, 'r') RETURNING id")
        .await
        .unwrap();
    let returned = rows.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => {
            Some(r.get("id").unwrap().parse::<i64>().unwrap())
        }
        _ => None,
    });
    assert!(
        returned.is_some_and(|id| id >= 1),
        "RETURNING surfaces the injected id"
    );

    // Every row now carries a positive id, and all four are distinct. A plain
    // keyless read concatenates (no ORDER BY, which a scatter cannot merge yet).
    let rows = client.simple_query("SELECT id FROM orders").await.unwrap();
    let mut ids: Vec<i64> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                Some(r.get("id").unwrap().parse::<i64>().unwrap())
            }
            _ => None,
        })
        .collect();
    assert_eq!(ids.len(), 4, "all four rows were stored with an id");
    assert!(ids.iter().all(|&id| id >= 1), "ids come from the sequence");
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 4, "each row got a distinct id");
}

#[tokio::test]
async fn an_insert_needing_a_sequence_without_one_configured_errors() {
    let pg = Pg::start().await.expect("start postgres");
    let router =
        pgshard_router::shared(Router::build(&sequenced_topology(pg.host(), pg.port())).unwrap());
    // A proxy with no sequence allocator (Proxy::new, not with_sequences).
    let proxy = Arc::new(Proxy::new(
        router,
        Backend {
            user: "postgres".into(),
            password: "postgres".into(),
            system_database: "postgres".into(),
        },
    ));
    let conn = spawn_router(proxy).await;
    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);

    // The INSERT omits the sequence-bound `id`, but no allocator is configured:
    // it fails loudly rather than routing a row with a missing id.
    let err = client
        .simple_query("INSERT INTO orders (customer_id, note) VALUES (1, 'x')")
        .await
        .unwrap_err();
    assert_eq!(err.code().map(|c| c.code()), Some("55000"));
}

/// A topology whose only serving shard has no primary (failover in flight):
/// a session-local statement must fail like any unavailable route — never
/// return a fabricated CommandComplete for work that ran nowhere.
#[tokio::test]
async fn no_primary_fails_local_statements_instead_of_fabricating_success() {
    let mut topo = single_shard_topology("127.0.0.1", 1);
    topo.shards[0].primary = None;
    let router = pgshard_router::shared(Router::build(&topo).unwrap());
    let proxy = Arc::new(Proxy::new(
        router,
        Backend {
            user: "postgres".into(),
            password: "postgres".into(),
            system_database: "postgres".into(),
        },
    ));
    let conn = spawn_router(proxy).await;
    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);

    let err = client.simple_query("SELECT 1").await.unwrap_err();
    assert_eq!(err.code().map(|c| c.code()), Some("57P01"));
}

/// The write lease: a router whose topology view cannot be confirmed current
/// within the lease refuses writes (a stale writer is the split-brain window
/// the lease bounds) while reads keep answering.
#[tokio::test]
async fn expired_write_lease_blocks_writes_but_not_reads() {
    let pg = Pg::start().await.expect("start postgres");
    let backend = pg.connect().await.unwrap();
    backend
        .batch_execute("CREATE TABLE orders (customer_id int, note text)")
        .await
        .unwrap();

    // write_lease_seconds is 10 in the fixture topology.
    let router = pgshard_router::shared(
        Router::build(&single_shard_topology(pg.host(), pg.port())).unwrap(),
    );
    let freshness = pgshard_topo::Freshness::new();
    let proxy = Arc::new(
        Proxy::new(
            router,
            Backend {
                user: "postgres".into(),
                password: "postgres".into(),
                system_database: "postgres".into(),
            },
        )
        .with_freshness(freshness.clone()),
    );
    let conn = spawn_router(proxy).await;
    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);

    // Fresh: the write goes through.
    client
        .simple_query("INSERT INTO orders (customer_id, note) VALUES (1, 'ok')")
        .await
        .unwrap();

    // Stale beyond the lease: writes refuse, reads still answer.
    freshness.backdate(std::time::Duration::from_secs(11));
    let err = client
        .simple_query("INSERT INTO orders (customer_id, note) VALUES (1, 'no')")
        .await
        .unwrap_err();
    assert_eq!(err.code().map(|c| c.code()), Some("57P01"));
    let rows = client
        .simple_query("SELECT note FROM orders WHERE customer_id = 1")
        .await
        .unwrap();
    assert!(rows.iter().any(|m| matches!(m,
        tokio_postgres::SimpleQueryMessage::Row(r) if r.get("note") == Some("ok"))));

    // A stale read cannot smuggle a write through a function body: the
    // backend session is read-only, so PostgreSQL itself refuses.
    backend
        .batch_execute(
            "CREATE FUNCTION sneaky_write() RETURNS int LANGUAGE sql AS \
             $$ INSERT INTO orders (customer_id, note) VALUES (9, 'sneak') RETURNING 1 $$",
        )
        .await
        .unwrap();
    let err = client
        .simple_query("SELECT sneaky_write()")
        .await
        .unwrap_err();
    assert_eq!(err.code().map(|c| c.code()), Some("25006"));

    // A successful refresh restores writes.
    freshness.bump();
    client
        .simple_query("INSERT INTO orders (customer_id, note) VALUES (1, 'again')")
        .await
        .unwrap();

    // The text (tokio-postgres) backend enforces the same stale read-only
    // session via connection options.
    let text_proxy = Arc::new(
        Proxy::new(
            pgshard_router::shared(
                Router::build(&single_shard_topology(pg.host(), pg.port())).unwrap(),
            ),
            Backend {
                user: "postgres".into(),
                password: "postgres".into(),
                system_database: "postgres".into(),
            },
        )
        .text()
        .with_freshness(freshness.clone()),
    );
    let conn2 = spawn_router(text_proxy).await;
    let (client2, connection2) = tokio_postgres::connect(&conn2, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection2);
    freshness.backdate(std::time::Duration::from_secs(11));
    let err = client2
        .simple_query("SELECT sneaky_write()")
        .await
        .unwrap_err();
    assert_eq!(err.code().map(|c| c.code()), Some("25006"));
}

/// The production lease path end to end: the lease renews only when the ACTIVE
/// router's view is re-confirmed. A source the router cannot accept — here a
/// gated topology, which Router::build refuses — must NOT keep the lease
/// alive; writes stop until the source becomes acceptable again.
#[tokio::test]
async fn lease_renews_only_when_the_active_view_is_confirmed() {
    use std::time::Duration;

    let pg = Pg::start().await.expect("start postgres");
    let backend = pg.connect().await.unwrap();
    backend
        .batch_execute("CREATE TABLE orders (customer_id int, note text)")
        .await
        .unwrap();

    let dir = std::env::temp_dir().join(format!("pgshard-lease-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("topology.json");
    let write_topo = |t: &Topology| {
        let tmp = dir.join("topology.tmp");
        std::fs::write(&tmp, serde_json::to_vec(t).unwrap()).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
    };

    let mut epoch1 = single_shard_topology(pg.host(), pg.port());
    epoch1.epoch = 1;
    write_topo(&epoch1);

    // A very long poll interval: the test drives refreshes via reload().
    let watcher = pgshard_topo::FileWatcher::start(&path, Duration::from_secs(3600))
        .await
        .unwrap();
    let initial = watcher.subscribe().borrow().clone();
    let router = pgshard_router::shared(Router::build(&initial).unwrap());
    let freshness = pgshard_topo::Freshness::new();
    tokio::spawn(pgshard_router::watch_topology_leased(
        router.clone(),
        watcher.subscribe(),
        Some(pgshard_router::LeaseWiring {
            validated: watcher.subscribe_validated(),
            freshness: freshness.clone(),
            poll_interval: Duration::from_secs(1),
        }),
    ));
    let proxy = Arc::new(
        Proxy::new(
            router,
            Backend {
                user: "postgres".into(),
                password: "postgres".into(),
                system_database: "postgres".into(),
            },
        )
        .with_freshness(freshness.clone()),
    );
    let conn = spawn_router(proxy).await;
    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);

    async fn write_ok(client: &tokio_postgres::Client) -> bool {
        client
            .simple_query("INSERT INTO orders (customer_id, note) VALUES (1, 'x')")
            .await
            .is_ok()
    }
    // Retry helper: the watch task applies events asynchronously.
    async fn eventually<F, Fut>(mut f: F) -> bool
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        for _ in 0..100 {
            if f().await {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    // Fresh: writes flow.
    assert!(write_ok(&client).await);

    // Expired, then a SAME-EPOCH revalidation renews the lease.
    freshness.backdate(Duration::from_secs(11));
    assert!(!write_ok(&client).await, "expired lease must refuse writes");
    watcher.reload().await.unwrap();
    assert!(
        eventually(|| write_ok(&client)).await,
        "a same-epoch revalidation must renew the lease"
    );

    // A gated epoch validates at the source but the router refuses it: the
    // lease must NOT renew from it.
    let mut gated = single_shard_topology(pg.host(), pg.port());
    gated.epoch = 2;
    gated.gates.push(pgshard_topo::GateSpec {
        id: "cutover".into(),
        match_: pgshard_topo::GateMatch {
            all: true,
            tables: Vec::new(),
            key_ranges: Vec::new(),
        },
        mode: pgshard_topo::GateMode::BufferWrites,
        deadline: "2026-07-18T00:00:00Z".into(),
        min_topology_generation: 0,
    });
    write_topo(&gated);
    watcher.reload().await.unwrap();
    freshness.backdate(Duration::from_secs(11));
    // Give the watch task time to (wrongly) renew if it were going to.
    tokio::time::sleep(Duration::from_millis(200)).await;
    watcher.reload().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !write_ok(&client).await,
        "a source the router cannot accept must not keep the lease alive"
    );

    // An acceptable newer epoch builds, swaps, and restores writes.
    let mut epoch3 = single_shard_topology(pg.host(), pg.port());
    epoch3.epoch = 3;
    write_topo(&epoch3);
    watcher.reload().await.unwrap();
    assert!(
        eventually(|| write_ok(&client)).await,
        "an accepted newer epoch must renew the lease"
    );
}
