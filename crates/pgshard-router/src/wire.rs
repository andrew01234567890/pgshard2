//! The PostgreSQL wire frontend: a proxy that terminates client connections,
//! routes each simple query with [`Router`], and forwards it to the shard's
//! backend database.
//!
//! # v1 scope
//!
//! This first slice handles the simple-query protocol for statements that route
//! to a single target: a shard database, the system database, or a session-local
//! statement (which runs on any shard so `SELECT 1`/`SHOW` return real rows). It
//! connects a fresh backend per query with [`tokio_postgres`] and relays results
//! in text form.
//!
//! It never mis-handles what it cannot yet support: multi-statement queries,
//! scatter/broadcast, parameterized simple queries, and transaction control
//! (which would silently autocommit across per-query connections) are rejected
//! with a clear SQLSTATE.
//!
//! Deferred to follow-ups: the extended protocol (Parse/Bind/Execute, where
//! [`pgshard_plan::resolve_bound`] is used), scatter/merge, connection pooling,
//! real session state (`SET` does not persist across queries yet), transaction
//! pinning, SCRAM auth, and TLS. Because [`tokio_postgres`]'s text-mode simple
//! query drops column type OIDs and the backend's command tag, v1 advertises all
//! columns as text and reconstructs the tag from the leading keyword; a verbatim
//! raw-protocol backend (which also streams rather than buffering rows) is the
//! planned replacement.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{Sink, stream};
use pgshard_seq::SequenceCache;
use pgshard_sql::Parsed;
use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use crate::backend::{BackendConnection, BackendResult, PgWireBackend, TokioPostgresBackend};
use crate::sequence::PgBlockReserver;
use crate::{Endpoint, Route, Router, SharedRouter, Target};

/// The sequence-id allocator shared across a proxy's queries. Long-lived across
/// topology swaps (unlike the [`Router`] snapshot), so it lives on the proxy.
pub type SequenceAllocator = Arc<SequenceCache<PgBlockReserver>>;

/// Credentials the router uses for its own backend connections, plus the name of
/// the unsharded system database (not yet carried in the topology).
#[derive(Debug, Clone)]
pub struct Backend {
    pub user: String,
    pub password: String,
    pub system_database: String,
}

/// The wire proxy: a hot-swappable [`Router`] plus backend credentials. Each
/// query reads the current router snapshot once, so a topology swap mid-session
/// takes effect on the next query without tearing a query in flight.
pub struct Proxy {
    router: SharedRouter,
    /// Backend credentials plus the system database name. Retained for the
    /// system-database routing decision; the connection itself goes through
    /// `conn`.
    backend: Backend,
    /// How a routed query reaches a shard's PostgreSQL. Defaults to the type-aware
    /// pgwire backend (real column type OIDs + verbatim command tags);
    /// [`Proxy::text`] swaps in the text-mode tokio-postgres backend.
    conn: Arc<dyn BackendConnection>,
    /// Allocates global-sequence ids for INSERTs that omit a sequence column.
    /// `None` disables injection (such an INSERT then errors rather than routing
    /// a row with a missing id).
    seq: Option<SequenceAllocator>,
}

impl Proxy {
    pub fn new(router: SharedRouter, backend: Backend) -> Self {
        Self {
            conn: Arc::new(PgWireBackend::new(backend.clone())),
            router,
            backend,
            seq: None,
        }
    }

    /// A proxy that also fills omitted global-sequence columns, reserving blocks
    /// through `seq`.
    pub fn with_sequences(router: SharedRouter, backend: Backend, seq: SequenceAllocator) -> Self {
        Self {
            conn: Arc::new(PgWireBackend::new(backend.clone())),
            router,
            backend,
            seq: Some(seq),
        }
    }

    /// Route backend traffic through the text-mode tokio-postgres backend instead
    /// of the default type-aware one. Results then advertise every column as text
    /// and the command tag is rebuilt from the leading keyword — the escape hatch
    /// kept for parity and quick fallback.
    pub fn text(mut self) -> Self {
        self.conn = Arc::new(TokioPostgresBackend::new(self.backend.clone()));
        self
    }

    async fn run_on(
        &self,
        endpoint: &Endpoint,
        database: &str,
        query: &str,
    ) -> PgWireResult<Vec<Response>> {
        let results = self.conn.run(endpoint, database, query).await?;
        Ok(results.into_iter().map(response_from).collect())
    }

    /// Allocate ids for any sequence columns the INSERT omits and rewrite it to
    /// include them, returning the new SQL. `None` when nothing needs injecting,
    /// so the caller forwards the original query unchanged.
    async fn inject_sequences(
        &self,
        router: &Router,
        parsed: &Parsed,
    ) -> PgWireResult<Option<String>> {
        let Some(node) = parsed
            .result()
            .protobuf
            .stmts
            .first()
            .and_then(|s| s.stmt.as_ref())
            .and_then(|n| n.node.as_ref())
        else {
            return Ok(None);
        };
        let injections = pgshard_plan::sequence::insert_sequence_injections(node, router.vschema());
        if injections.is_empty() {
            return Ok(None);
        }
        let Some(seq) = self.seq.clone() else {
            return Err(user_error(
                "55000",
                "cannot allocate sequence ids: the router has no system database".to_owned(),
            ));
        };
        let rows = pgshard_plan::sequence::value_row_count(node);
        // A reservation drives a blocking client that must not run on an async
        // worker (its nested block_on would panic), so allocate on the blocking
        // pool. One id is drawn per value row.
        let columns = tokio::task::spawn_blocking(move || {
            injections
                .into_iter()
                .map(|inj| {
                    let ids = (0..rows)
                        .map(|_| seq.next_id(&inj.sequence))
                        .collect::<Result<Vec<i64>, _>>()?;
                    Ok::<_, pgshard_seq::SeqError>(pgshard_plan::sequence::InjectedColumn {
                        column: inj.column,
                        ids,
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .await
        .map_err(|e| user_error("XX000", format!("sequence allocation task failed: {e}")))?
        .map_err(|e| user_error("55000", format!("sequence allocation failed: {e}")))?;

        let sql = pgshard_plan::sequence::rewrite_insert(parsed, &columns)
            .map_err(|e| user_error("XX000", format!("could not inject sequence ids: {e}")))?;
        Ok(Some(sql))
    }

    /// Fan a plain scatter read out to every shard concurrently and concatenate
    /// the rows. Only valid when the read needs no ordering, limiting, grouping,
    /// or aggregation (checked by the caller); ordered/aggregated scatters need
    /// the merge engine and are rejected until it lands.
    async fn run_scatter(&self, targets: &[Target], query: &str) -> PgWireResult<Vec<Response>> {
        let fetches = targets
            .iter()
            .map(|t| self.conn.run(&t.endpoint, &t.database, query));
        let results = futures::future::join_all(fetches).await;

        let mut schema: Option<Arc<Vec<FieldInfo>>> = None;
        let mut rows: Vec<DataRow> = Vec::new();
        for result in results {
            // First shard error fails the whole scatter — a partial result set
            // would be silently wrong.
            for shard_result in result? {
                let BackendResult::Rows {
                    schema: shard_schema,
                    rows: shard_rows,
                } = shard_result
                else {
                    // A plain scatter read is a single SELECT; a no-row result is
                    // not expected, but ignoring it is safe for concatenation.
                    continue;
                };
                match &schema {
                    None => schema = Some(shard_schema),
                    // Shards must agree on the result shape. A column-count/name
                    // mismatch (e.g. a non-atomic broadcast DDL still rolling out)
                    // would otherwise encode rows under the wrong schema — fail the
                    // scatter instead.
                    Some(first) if !schemas_match(first, &shard_schema) => {
                        return Err(user_error(
                            "0A000",
                            "shards returned different result schemas; a schema change may be mid-rollout"
                                .to_owned(),
                        ));
                    }
                    _ => {}
                }
                rows.extend(shard_rows);
            }
        }
        match schema {
            Some(schema) => Ok(vec![rows_response(schema, rows)]),
            // A scatter only comes from a SELECT, which always describes its
            // columns; guard defensively rather than panic.
            None => Ok(vec![Response::Execution(Tag::new("SELECT").with_rows(0))]),
        }
    }
}

/// Trust auth for v1 (SCRAM is a follow-up).
#[async_trait]
impl NoopStartupHandler for Proxy {
    async fn post_startup<C>(
        &self,
        _client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for Proxy {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        // Read the current routing snapshot once, so the whole query uses one
        // consistent topology even if a swap lands mid-query. Parse once and
        // route from that AST, so sequence injection reuses the same parse.
        let router = self.router.load_full();
        let parsed = pgshard_sql::parse(query)
            .map_err(|e| user_error("42601", format!("could not parse query: {e}")))?;
        let routes = router.route_parsed(&parsed);

        match routes.as_slice() {
            // A submitted query with no statements (empty or comment-only) gets an
            // EmptyQueryResponse, as PostgreSQL sends.
            [] => Ok(vec![Response::EmptyQuery]),
            [route] => self.dispatch(&router, &parsed, route.clone(), query).await,
            _ => Err(user_error(
                "0A000",
                "multi-statement simple queries are not supported yet".to_owned(),
            )),
        }
    }
}

/// Transaction-control statements cannot be honored while the router opens a
/// fresh backend connection per query: a `BEGIN` would silently autocommit its
/// following statements. They are rejected rather than falsely acknowledged
/// until session pinning lands.
fn is_transaction_control(query: &str) -> bool {
    matches!(
        command_tag(query).as_str(),
        "BEGIN" | "START" | "COMMIT" | "END" | "ROLLBACK" | "ABORT" | "SAVEPOINT" | "RELEASE"
    )
}

impl Proxy {
    async fn dispatch(
        &self,
        router: &Router,
        parsed: &Parsed,
        route: Route,
        query: &str,
    ) -> PgWireResult<Vec<Response>> {
        match route {
            Route::Shard(t) => {
                // An INSERT omitting a sequence-bound column has its id(s) filled
                // in here; every other statement forwards unchanged.
                let injected = self.inject_sequences(router, parsed).await?;
                let sql = injected.as_deref().unwrap_or(query);
                self.run_on(&t.endpoint, &t.database, sql).await
            }
            Route::System(ep) => self.run_on(&ep, &self.backend.system_database, query).await,
            // Session-local statements: reject transaction control (it would break
            // atomicity across per-query connections), and run everything else
            // (`SELECT 1`, `SHOW`, `SET`) on any shard so reads return real rows.
            // A `SET` there does not persist across queries yet — a documented
            // limitation until session state lands.
            Route::Local if is_transaction_control(query) => Err(user_error(
                "0A000",
                "transactions are not supported by this router version".to_owned(),
            )),
            Route::Local => match router.any_shard_target() {
                Some(t) => self.run_on(&t.endpoint, &t.database, query).await,
                None => Ok(vec![Response::Execution(Tag::new(&command_tag(query)))]),
            },
            Route::Reject { code, reason } => Err(user_error(code, reason)),
            Route::Unavailable(reason) => Err(user_error("57P01", reason)),
            // A plain scatter read (no ordering/limit/grouping/aggregation) is
            // fanned out and concatenated; anything needing a real merge waits
            // for the merge engine.
            Route::Scatter(targets) if is_concatenable_scatter(query) => {
                self.run_scatter(&targets, query).await
            }
            Route::Scatter(_) => Err(user_error(
                "0A000",
                "ordered or aggregated scatter reads are not supported yet".to_owned(),
            )),
            Route::Broadcast(_) => Err(user_error(
                "0A000",
                "broadcast (multi-shard DDL) is not supported yet".to_owned(),
            )),
            Route::NeedsBind(_) => Err(user_error(
                "0A000",
                "parameterized queries require the extended protocol".to_owned(),
            )),
        }
    }
}

/// Whether two result schemas describe the same columns: count, name, type, and
/// wire format. The type is load-bearing on the verbatim backend, which reports
/// real OIDs — two shards with a same-named column of different types (e.g. a
/// non-atomic DDL still rolling out) must not have one shard's rows emitted under
/// the other's type.
fn schemas_match(a: &[FieldInfo], b: &[FieldInfo]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            x.name() == y.name() && x.datatype() == y.datatype() && x.format() == y.format()
        })
}

/// A row-returning response from a schema plus already-encoded rows.
fn rows_response(schema: Arc<Vec<FieldInfo>>, rows: Vec<DataRow>) -> Response {
    let row_stream = stream::iter(rows.into_iter().map(Ok));
    Response::Query(QueryResponse::new(schema, row_stream))
}

/// Turn one backend statement result into a wire response: rows (a
/// `QueryResponse` carrying the backend's schema), a command tag (an
/// `Execution` with the tag string), or an empty-query response.
fn response_from(result: BackendResult) -> Response {
    match result {
        BackendResult::Rows { schema, rows } => rows_response(schema, rows),
        BackendResult::Command { tag } => Response::Execution(Tag::new(&tag)),
        BackendResult::Empty => Response::EmptyQuery,
    }
}

/// Whether a scatter read can be answered by simply concatenating each shard's
/// rows: a single `SELECT` with no `ORDER BY`, `LIMIT`/`OFFSET`, `GROUP BY`,
/// `DISTINCT`, and only plain column/`*` output (no aggregates or expressions).
/// Anything else needs cross-shard sorting/limiting/aggregation — the merge
/// engine — and is rejected for now.
///
/// This re-parses the query (the planner already parsed it once); the plan cache
/// that avoids the double parse is a follow-up.
fn is_concatenable_scatter(query: &str) -> bool {
    let Ok(parsed) = pg_query::parse(query) else {
        return false;
    };
    let [stmt] = parsed.protobuf.stmts.as_slice() else {
        return false;
    };
    let Some(pg_query::NodeEnum::SelectStmt(select)) =
        stmt.stmt.as_ref().and_then(|n| n.node.as_ref())
    else {
        return false;
    };
    // These checks make the guard self-sufficient rather than trusting the
    // planner to have filtered every unsafe shape upstream:
    // - a set operation (UNION/…) keeps its operands in larg/rarg with an empty
    //   target_list and needs real merging;
    // - a WITH clause can shadow a sharded table name, so each shard would
    //   re-evaluate the CTE and the rows would be duplicated on concatenation;
    // - a locking clause (FOR UPDATE/SHARE) cannot be honored across ephemeral
    //   per-shard connections;
    // - SELECT ... INTO is a write.
    select.op == pg_query::protobuf::SetOperation::SetopNone as i32
        && select.with_clause.is_none()
        && select.into_clause.is_none()
        && select.locking_clause.is_empty()
        && select.sort_clause.is_empty()
        && select.group_clause.is_empty()
        && select.distinct_clause.is_empty()
        && !select.group_distinct
        && select.limit_count.is_none()
        && select.limit_offset.is_none()
        && !select.target_list.is_empty()
        && select.target_list.iter().all(is_plain_column_target)
}

/// A `ResTarget` whose value is a bare column reference (which also covers `*`),
/// not an aggregate or expression.
fn is_plain_column_target(node: &pg_query::protobuf::Node) -> bool {
    match node.node.as_ref() {
        Some(pg_query::NodeEnum::ResTarget(rt)) => matches!(
            rt.val.as_deref().and_then(|v| v.node.as_ref()),
            Some(pg_query::NodeEnum::ColumnRef(_))
        ),
        _ => false,
    }
}

/// The command tag for a statement with no rows. v1 derives it from the leading
/// keyword; a verbatim relay of the backend's own tag is a follow-up (it needs
/// the raw backend protocol rather than tokio-postgres, which drops the tag).
fn command_tag(query: &str) -> String {
    query
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == '(' || c == ';')
        .next()
        .filter(|w| !w.is_empty())
        .map(|w| w.to_uppercase())
        .unwrap_or_else(|| "OK".to_owned())
}

fn user_error(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message,
    )))
}

/// Wraps a [`Proxy`] as the pgwire server handler set. Extended-query and copy
/// handlers fall back to pgwire's defaults (which reject) until implemented.
pub struct Handlers {
    proxy: Arc<Proxy>,
}

impl Handlers {
    pub fn new(proxy: Arc<Proxy>) -> Self {
        Self { proxy }
    }
}

impl PgWireServerHandlers for Handlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.proxy.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.proxy.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::{command_tag, is_concatenable_scatter};

    #[test]
    fn command_tag_uses_the_leading_keyword() {
        assert_eq!(command_tag("  insert into t values (1)"), "INSERT");
        assert_eq!(command_tag("SET search_path = app"), "SET");
        assert_eq!(command_tag("begin;"), "BEGIN");
        assert_eq!(command_tag("CREATE(x"), "CREATE");
        assert_eq!(command_tag("   "), "OK");
    }

    #[test]
    fn only_plain_selects_are_concatenable_scatters() {
        // Plain projections (including *) concatenate correctly across shards.
        assert!(is_concatenable_scatter("SELECT * FROM orders"));
        assert!(is_concatenable_scatter(
            "SELECT id, note FROM orders WHERE note = 'x'"
        ));
        // Anything that needs cross-shard combining does not.
        assert!(!is_concatenable_scatter("SELECT * FROM orders ORDER BY id"));
        assert!(!is_concatenable_scatter("SELECT * FROM orders LIMIT 10"));
        assert!(!is_concatenable_scatter("SELECT * FROM orders OFFSET 5"));
        assert!(!is_concatenable_scatter("SELECT count(*) FROM orders"));
        assert!(!is_concatenable_scatter("SELECT DISTINCT note FROM orders"));
        assert!(!is_concatenable_scatter(
            "SELECT note FROM orders GROUP BY note"
        ));
        assert!(!is_concatenable_scatter("SELECT lower(note) FROM orders"));
        // A WITH clause can shadow a table name (each shard re-evaluates the CTE),
        // and FOR UPDATE/SHARE cannot be honored across per-shard connections.
        assert!(!is_concatenable_scatter(
            "WITH orders AS (VALUES (1)) SELECT * FROM orders"
        ));
        assert!(!is_concatenable_scatter("SELECT * FROM orders FOR UPDATE"));
        assert!(!is_concatenable_scatter(
            "SELECT * FROM orders UNION SELECT * FROM orders"
        ));
        // Not a single plain SELECT at all.
        assert!(!is_concatenable_scatter("UPDATE orders SET note = 'x'"));
    }
}
