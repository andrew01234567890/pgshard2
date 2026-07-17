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
use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use tokio_postgres::{NoTls, SimpleQueryMessage};

use crate::{Endpoint, Route, Router};

/// Credentials the router uses for its own backend connections, plus the name of
/// the unsharded system database (not yet carried in the topology).
#[derive(Debug, Clone)]
pub struct Backend {
    pub user: String,
    pub password: String,
    pub system_database: String,
}

/// The wire proxy: one immutable [`Router`] snapshot plus backend credentials.
pub struct Proxy {
    router: Arc<Router>,
    backend: Backend,
}

impl Proxy {
    pub fn new(router: Arc<Router>, backend: Backend) -> Self {
        Self { router, backend }
    }

    async fn run_on(
        &self,
        endpoint: &Endpoint,
        database: &str,
        query: &str,
    ) -> PgWireResult<Vec<Response>> {
        let conn = format!(
            "host={} port={} user={} password={} dbname={}",
            endpoint.host, endpoint.port, self.backend.user, self.backend.password, database
        );
        let (client, connection) = tokio_postgres::connect(&conn, NoTls)
            .await
            .map_err(backend_error)?;
        // The connection task drives the protocol; it ends when `client` drops.
        let driver = tokio::spawn(connection);
        let result = client.simple_query(query).await.map_err(backend_error);
        drop(client);
        let _ = driver.await;
        translate(query, result?)
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
        let routes = self
            .router
            .route(query)
            .map_err(|e| user_error("42601", format!("could not parse query: {e}")))?;

        match routes.as_slice() {
            // A submitted query with no statements (empty or comment-only) gets an
            // EmptyQueryResponse, as PostgreSQL sends.
            [] => Ok(vec![Response::EmptyQuery]),
            [route] => self.dispatch(route.clone(), query).await,
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
    async fn dispatch(&self, route: Route, query: &str) -> PgWireResult<Vec<Response>> {
        match route {
            Route::Shard(t) => self.run_on(&t.endpoint, &t.database, query).await,
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
            Route::Local => match self.router.any_shard_target() {
                Some(t) => self.run_on(&t.endpoint, &t.database, query).await,
                None => Ok(vec![Response::Execution(Tag::new(&command_tag(query)))]),
            },
            Route::Reject { code, reason } => Err(user_error(code, reason)),
            Route::Unavailable(reason) => Err(user_error("57P01", reason)),
            Route::Scatter(_) | Route::Broadcast(_) => Err(user_error(
                "0A000",
                "scatter and broadcast queries are not supported yet".to_owned(),
            )),
            Route::NeedsBind(_) => Err(user_error(
                "0A000",
                "parameterized queries require the extended protocol".to_owned(),
            )),
        }
    }
}

/// Turn a backend `simple_query` result into wire responses. v1 forwards a single
/// statement, so there is one result set: rows (a `QueryResponse`) or a command
/// tag (an `Execution`).
fn translate(query: &str, messages: Vec<SimpleQueryMessage>) -> PgWireResult<Vec<Response>> {
    let mut schema: Option<Arc<Vec<FieldInfo>>> = None;
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    let mut affected: u64 = 0;
    for msg in messages {
        match msg {
            SimpleQueryMessage::RowDescription(cols) => {
                // v1 forwards a single statement, so there is one result set. If a
                // second description ever arrives, start fresh so rows always match
                // the current schema (never index a stale one out of bounds).
                rows.clear();
                schema = Some(Arc::new(
                    cols.iter()
                        .map(|c| {
                            FieldInfo::new(
                                c.name().to_owned(),
                                None,
                                None,
                                Type::VARCHAR,
                                FieldFormat::Text,
                            )
                        })
                        .collect(),
                ));
            }
            SimpleQueryMessage::Row(r) => {
                rows.push((0..r.len()).map(|i| r.get(i).map(str::to_owned)).collect());
            }
            SimpleQueryMessage::CommandComplete(n) => affected = n,
            _ => {}
        }
    }

    if let Some(schema) = schema {
        let mut encoder = DataRowEncoder::new(schema.clone());
        let mut encoded = Vec::with_capacity(rows.len());
        for row in &rows {
            for value in row {
                encoder.encode_field(&value.as_deref())?;
            }
            encoded.push(encoder.take_row());
        }
        let row_stream = stream::iter(encoded.into_iter().map(Ok));
        Ok(vec![Response::Query(QueryResponse::new(
            schema, row_stream,
        ))])
    } else {
        let command = command_tag(query);
        // Only INSERT carries the `oid rows` tag shape (`INSERT 0 n`); UPDATE and
        // DELETE are `verb n`. Getting INSERT's zero oid right keeps libpq's
        // PQcmdTuples (psycopg2 rowcount) working.
        let mut tag = Tag::new(&command).with_rows(affected as usize);
        if command == "INSERT" {
            tag = tag.with_oid(0);
        }
        Ok(vec![Response::Execution(tag)])
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

fn backend_error(e: tokio_postgres::Error) -> PgWireError {
    // Surface the backend's SQLSTATE when it has one, else a generic failure.
    match e.as_db_error() {
        Some(db) => PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            db.code().code().to_owned(),
            db.message().to_owned(),
        ))),
        None => user_error("08006", format!("backend connection failed: {e}")),
    }
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
    use super::command_tag;

    #[test]
    fn command_tag_uses_the_leading_keyword() {
        assert_eq!(command_tag("  insert into t values (1)"), "INSERT");
        assert_eq!(command_tag("SET search_path = app"), "SET");
        assert_eq!(command_tag("begin;"), "BEGIN");
        assert_eq!(command_tag("CREATE(x"), "CREATE");
        assert_eq!(command_tag("   "), "OK");
    }
}
