//! The backend connection: how the proxy runs a routed query on a shard's
//! PostgreSQL and gets results back.
//!
//! Two implementations coexist behind [`BackendConnection`] so the verbatim
//! path can be introduced without a big-bang rewrite of the proven one:
//!
//! - [`TokioPostgresBackend`] (the default) uses [`tokio_postgres`] in
//!   simple-query text mode. It cannot see column type OIDs (every column is
//!   advertised as text) or the backend's real command tag (it is rebuilt from
//!   the leading keyword). This is the behavior the router shipped with.
//! - [`PgWireBackend`] speaks the wire protocol directly (via pgwire's client),
//!   so a result carries the backend's **real column type OIDs** and its
//!   **verbatim command tag**. This is what a sound cross-shard ORDER BY merge
//!   (which needs typed sort keys) and type-aware clients require.
//!
//! Both return the same backend-agnostic [`BackendResult`], so the wire layer
//! builds a frontend response the same way regardless of which is in use.

use std::sync::Arc;

use async_trait::async_trait;
use futures::{Sink, SinkExt};
use pgwire::api::Type;
use pgwire::api::client::auth::DefaultStartupHandler;
use pgwire::api::client::query::SimpleQueryHandler;
use pgwire::api::client::{ClientInfo, Config, ReadyState};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo};
use pgwire::error::{ErrorInfo, PgWireClientError, PgWireError, PgWireResult};
use pgwire::messages::data::{DataRow, RowDescription};
use pgwire::messages::response::{CommandComplete, EmptyQueryResponse, ReadyForQuery};
use pgwire::messages::simplequery::Query;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::client::PgWireClient;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use crate::Endpoint;
use crate::wire::Backend;

/// One statement's result in a backend-agnostic form. Rows carry a schema whose
/// `FieldInfo`s hold the column type (a real OID on the verbatim backend, text
/// on the tokio one) plus the already-encoded [`DataRow`]s; a no-row statement
/// carries the command tag string to send back verbatim.
pub enum BackendResult {
    Rows {
        schema: Arc<Vec<FieldInfo>>,
        rows: Vec<DataRow>,
    },
    Command {
        tag: String,
    },
    Empty,
}

/// Run one simple query on a shard database and return one result per statement
/// (v1 forwards a single statement, so normally one).
#[async_trait]
pub trait BackendConnection: Send + Sync {
    async fn run(
        &self,
        endpoint: &Endpoint,
        database: &str,
        query: &str,
    ) -> PgWireResult<Vec<BackendResult>>;
}

fn user_error(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message,
    )))
}

// ---- The proven default: tokio-postgres, text mode --------------------------

/// The router's original backend: a fresh [`tokio_postgres`] connection per
/// query, text-mode simple query. Column type OIDs and the verbatim command tag
/// are not available on this path.
pub struct TokioPostgresBackend {
    creds: Backend,
}

impl TokioPostgresBackend {
    pub fn new(creds: Backend) -> Self {
        Self { creds }
    }
}

#[async_trait]
impl BackendConnection for TokioPostgresBackend {
    async fn run(
        &self,
        endpoint: &Endpoint,
        database: &str,
        query: &str,
    ) -> PgWireResult<Vec<BackendResult>> {
        let conn = format!(
            "host={} port={} user={} password={} dbname={}",
            endpoint.host, endpoint.port, self.creds.user, self.creds.password, database
        );
        let (client, connection) = tokio_postgres::connect(&conn, NoTls)
            .await
            .map_err(tokio_backend_error)?;
        // The connection task drives the protocol; it ends when `client` drops.
        let driver = tokio::spawn(connection);
        let result = client
            .simple_query(query)
            .await
            .map_err(tokio_backend_error);
        drop(client);
        let _ = driver.await;
        Ok(vec![text_result(query, result?)?])
    }
}

/// Pull one statement's result out of `simple_query`'s messages, encoding text
/// rows into `DataRow`s and rebuilding a command tag from the leading keyword —
/// the behavior the router shipped with. v1 forwards one statement, so if a
/// second `RowDescription` arrives the rows are reset to stay in sync with it.
fn text_result(query: &str, messages: Vec<SimpleQueryMessage>) -> PgWireResult<BackendResult> {
    let mut schema: Option<Arc<Vec<FieldInfo>>> = None;
    let mut text_rows: Vec<Vec<Option<String>>> = Vec::new();
    let mut affected: u64 = 0;
    for msg in messages {
        match msg {
            SimpleQueryMessage::RowDescription(cols) => {
                text_rows.clear();
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
                text_rows.push((0..r.len()).map(|i| r.get(i).map(str::to_owned)).collect());
            }
            SimpleQueryMessage::CommandComplete(n) => affected = n,
            _ => {}
        }
    }
    match schema {
        Some(schema) => Ok(BackendResult::Rows {
            rows: encode_text_rows(&schema, &text_rows)?,
            schema,
        }),
        None => Ok(BackendResult::Command {
            tag: text_command_tag(query, affected),
        }),
    }
}

/// Encode already-fetched text values into `DataRow`s under `schema`.
fn encode_text_rows(
    schema: &Arc<Vec<FieldInfo>>,
    rows: &[Vec<Option<String>>],
) -> PgWireResult<Vec<DataRow>> {
    let mut encoded = Vec::with_capacity(rows.len());
    for row in rows {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for value in row {
            encoder.encode_field(&value.as_deref())?;
        }
        encoded.push(encoder.take_row());
    }
    Ok(encoded)
}

/// The command tag for a no-row statement, derived from the leading keyword and
/// the affected-row count. Matches the shape PostgreSQL emits closely enough for
/// libpq's `PQcmdTuples`: `INSERT` carries the zero-oid form (`INSERT 0 n`),
/// every other verb is `verb n`.
fn text_command_tag(query: &str, affected: u64) -> String {
    let command = command_keyword(query);
    if command == "INSERT" {
        format!("INSERT 0 {affected}")
    } else {
        format!("{command} {affected}")
    }
}

/// The leading keyword of a statement, uppercased.
fn command_keyword(query: &str) -> String {
    query
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == '(' || c == ';')
        .next()
        .filter(|w| !w.is_empty())
        .map(|w| w.to_uppercase())
        .unwrap_or_else(|| "OK".to_owned())
}

fn tokio_backend_error(e: tokio_postgres::Error) -> PgWireError {
    // Surface the backend's SQLSTATE when it has one, else a generic failure.
    match e.as_db_error() {
        Some(db) => user_error(db.code().code(), db.message().to_owned()),
        None => user_error("08006", format!("backend connection failed: {e}")),
    }
}

// ---- The verbatim path: pgwire client ---------------------------------------

/// A backend that speaks the wire protocol directly via pgwire's client, so a
/// result keeps the backend's real column type OIDs and verbatim command tag.
/// Auth is SCRAM-SHA-256 (PG18's default) over a plain connection; TLS, the
/// extended protocol, COPY, and pooling are later slices.
pub struct PgWireBackend {
    creds: Backend,
}

impl PgWireBackend {
    pub fn new(creds: Backend) -> Self {
        Self { creds }
    }
}

#[async_trait]
impl BackendConnection for PgWireBackend {
    async fn run(
        &self,
        endpoint: &Endpoint,
        database: &str,
        query: &str,
    ) -> PgWireResult<Vec<BackendResult>> {
        // No TLS connector forces a plain connection regardless of ssl_mode.
        let mut config = Config::new();
        config
            .host(endpoint.host.clone())
            .port(endpoint.port)
            .user(self.creds.user.clone())
            .password(self.creds.password.clone())
            .dbname(database.to_owned());

        let mut client =
            PgWireClient::connect(Arc::new(config), DefaultStartupHandler::new(), None)
                .await
                .map_err(client_error)?;
        client
            .simple_query(ResultCollector::default(), query)
            .await
            .map_err(client_error)
    }
}

/// Translate a pgwire client error into a frontend error, preserving the
/// backend's SQLSTATE when the failure was a remote error response.
fn client_error(e: PgWireClientError) -> PgWireError {
    match e {
        PgWireClientError::RemoteError(info) => user_error(&info.code, info.message),
        other => user_error("08006", format!("backend connection failed: {other}")),
    }
}

/// A simple-query handler that collects each statement's result into a
/// [`BackendResult`]. Unlike pgwire's default handler it keeps the **raw**
/// `CommandComplete` tag string rather than parsing it into a structured `Tag`
/// (whose parser rejects multi-word tags like `CREATE TABLE` / `DISCARD ALL`),
/// and it preserves the backend's real column types from the `RowDescription`.
#[derive(Default)]
struct ResultCollector {
    /// The schema+rows of a row-returning statement in progress, awaiting its
    /// `CommandComplete`.
    pending: Option<(Vec<FieldInfo>, Vec<DataRow>)>,
    out: Vec<BackendResult>,
}

#[async_trait]
impl SimpleQueryHandler for ResultCollector {
    type QueryResponse = BackendResult;

    // The default dispatch rejects any message it does not expect, but a backend
    // may interleave benign asynchronous messages — a reported GUC change (a
    // `SET application_name`/`search_path` triggers a `ParameterStatus`), a
    // notice, or a `LISTEN` notification — which must be ignored, not treated as
    // an error.
    async fn on_message<C>(
        &mut self,
        client: &mut C,
        message: PgWireBackendMessage,
    ) -> Result<ReadyState<Vec<BackendResult>>, PgWireClientError>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        match message {
            PgWireBackendMessage::RowDescription(m) => self.on_row_description(client, m).await?,
            PgWireBackendMessage::DataRow(m) => self.on_data_row(client, m).await?,
            PgWireBackendMessage::CommandComplete(m) => self.on_command_complete(client, m).await?,
            PgWireBackendMessage::EmptyQueryResponse(m) => self.on_empty_query(client, m).await?,
            PgWireBackendMessage::ReadyForQuery(m) => {
                return Ok(ReadyState::Ready(self.on_ready_for_query(client, m).await?));
            }
            PgWireBackendMessage::ErrorResponse(e) => return Err(ErrorInfo::from(e).into()),
            PgWireBackendMessage::NoticeResponse(_)
            | PgWireBackendMessage::ParameterStatus(_)
            | PgWireBackendMessage::NotificationResponse(_) => {}
            other => return Err(PgWireClientError::UnexpectedMessage(Box::new(other))),
        }
        Ok(ReadyState::Pending)
    }

    async fn simple_query<C>(
        &mut self,
        client: &mut C,
        query: &str,
    ) -> Result<(), PgWireClientError>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        client
            .send(PgWireFrontendMessage::Query(Query::new(query.to_owned())))
            .await?;
        Ok(())
    }

    async fn on_row_description<C>(
        &mut self,
        _client: &mut C,
        message: RowDescription,
    ) -> Result<(), PgWireClientError>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        // `FieldDescription -> FieldInfo` sets the datatype from the wire type
        // OID, so the real column type flows through unchanged.
        let fields = message.fields.into_iter().map(Into::into).collect();
        self.pending = Some((fields, Vec::new()));
        Ok(())
    }

    async fn on_data_row<C>(
        &mut self,
        _client: &mut C,
        message: DataRow,
    ) -> Result<(), PgWireClientError>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        match self.pending.as_mut() {
            Some((_, rows)) => {
                rows.push(message);
                Ok(())
            }
            None => Err(PgWireClientError::UnexpectedMessage(Box::new(
                PgWireBackendMessage::DataRow(message),
            ))),
        }
    }

    async fn on_command_complete<C>(
        &mut self,
        _client: &mut C,
        message: CommandComplete,
    ) -> Result<(), PgWireClientError>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        match self.pending.take() {
            Some((schema, rows)) => self.out.push(BackendResult::Rows {
                schema: Arc::new(schema),
                rows,
            }),
            // The verbatim backend command tag, kept as its raw string.
            None => self.out.push(BackendResult::Command { tag: message.tag }),
        }
        Ok(())
    }

    async fn on_empty_query<C>(
        &mut self,
        _client: &mut C,
        _message: EmptyQueryResponse,
    ) -> Result<(), PgWireClientError>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        self.out.push(BackendResult::Empty);
        Ok(())
    }

    async fn on_ready_for_query<C>(
        &mut self,
        _client: &mut C,
        _message: ReadyForQuery,
    ) -> Result<Vec<BackendResult>, PgWireClientError>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        Ok(std::mem::take(&mut self.out))
    }
}
