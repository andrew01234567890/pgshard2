//! The `AgentService` gRPC surface. This first agent implements the HA path the
//! operator already drives — status polling and the epoch-guarded
//! promote/fence/rejoin handshake — over the [`Instance`] abstraction. The
//! remaining RPCs (backups, restore, stanzas, replication, DDL, CDC) are wired
//! in later steps and currently return `Unimplemented`.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use pgshard_proto::v1;
use v1::agent_service_server::AgentService;

use crate::epoch::{EpochError, EpochGuard, Outcome};
use crate::instance::{ForeignDatabase, Instance, RestorePoint};
use crate::schema::{Claim, SchemaError, SchemaLog};
use crate::status::to_status;

pub struct AgentSvc<I: Instance> {
    instance: Arc<I>,
    /// This instance's pod name (`PGSHARD_POD`); a Promote aimed at a different
    /// target is refused.
    pod: String,
    /// This pod's Kubernetes UID (`PGSHARD_POD_UID`, downward API). Pod IPs
    /// are reusable, so identity-sensitive requests carry the intended pod
    /// UID and are refused on a mismatch. Empty = unwired (checks skipped).
    pod_uid: String,
    epoch: EpochGuard,
    schema: SchemaLog,
    /// Restore points already created, by name — one async slot per name, so
    /// creation is SINGLE-FLIGHT. PostgreSQL does not deduplicate restore-point
    /// names (a second create writes a second record, and recovery by name
    /// stops at the FIRST), so a retried barrier must replay its original
    /// point, and two CONCURRENT same-name calls must never both execute: the
    /// loser would get a different LSN than the manifest records. The per-name
    /// lock is held across the PostgreSQL call; a failed create leaves the slot
    /// empty so a retry re-executes. In-memory, like the schema log: a retry
    /// after an agent restart is out of scope for M1.
    restore_points:
        tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<Option<RestorePoint>>>>>,
}

impl<I: Instance> AgentSvc<I> {
    pub fn new(instance: Arc<I>, pod: String) -> Self {
        Self::with_pod_uid(instance, pod, String::new())
    }

    pub fn with_pod_uid(instance: Arc<I>, pod: String, pod_uid: String) -> Self {
        Self {
            instance,
            pod,
            pod_uid,
            epoch: EpochGuard::new(),
            schema: SchemaLog::new(),
            restore_points: tokio::sync::Mutex::new(HashMap::new()),
        }
    }
}

fn restore_point_response(rp: RestorePoint) -> v1::CreateRestorePointResponse {
    v1::CreateRestorePointResponse {
        lsn: Some(v1::Lsn { value: rp.lsn }),
        timeline: rp.timeline,
    }
}

fn schema_status(err: SchemaError) -> Status {
    match err {
        SchemaError::EmptyId | SchemaError::DifferentSql(_) => {
            Status::invalid_argument(err.to_string())
        }
        SchemaError::InFlight(_) => Status::already_exists(err.to_string()),
    }
}

fn epoch_status(err: EpochError) -> Status {
    match err {
        EpochError::Zero => Status::invalid_argument(err.to_string()),
        EpochError::Stale { .. } | EpochError::Conflict { .. } => {
            Status::failed_precondition(err.to_string())
        }
    }
}

fn internal(err: anyhow::Error) -> Status {
    Status::internal(err.to_string())
}

/// PostgreSQL's `NAMEDATALEN - 1`: identifiers longer than this are silently
/// truncated. A truncated database name could resolve to a *different* existing
/// database (and a create could then falsely succeed via a duplicate-database
/// error, or a drop delete the wrong database), so reject overlong identifiers
/// up front rather than let PostgreSQL truncate them.
const MAX_IDENT_BYTES: usize = 63;

/// Validate a database identifier: `required` names must be nonempty, and any
/// value must fit in [`MAX_IDENT_BYTES`] so PostgreSQL does not truncate it. An
/// empty optional value (e.g. no owner) passes.
fn check_ident(what: &str, value: &str, required: bool) -> Result<(), Status> {
    if value.is_empty() {
        return if required {
            Err(Status::invalid_argument(format!("{what} is required")))
        } else {
            Ok(())
        };
    }
    if value.len() > MAX_IDENT_BYTES {
        return Err(Status::invalid_argument(format!(
            "{what} exceeds PostgreSQL's {MAX_IDENT_BYTES}-byte identifier limit"
        )));
    }
    Ok(())
}

/// Bounds the provenance marker (a Kubernetes UID in practice) and keeps the
/// stamped comment trivially parseable and safe to embed.
const MAX_PROVENANCE_BYTES: usize = 128;

fn check_provenance(value: &str) -> Result<(), Status> {
    if value.len() > MAX_PROVENANCE_BYTES {
        return Err(Status::invalid_argument(format!(
            "provenance exceeds {MAX_PROVENANCE_BYTES} bytes"
        )));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-'))
    {
        return Err(Status::invalid_argument(
            "provenance may only contain [A-Za-z0-9._:-]",
        ));
    }
    Ok(())
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl<I: Instance> AgentService for AgentSvc<I> {
    async fn get_status(
        &self,
        _request: Request<v1::GetStatusRequest>,
    ) -> Result<Response<v1::GetStatusResponse>, Status> {
        let snap = self.instance.snapshot().await.map_err(internal)?;
        Ok(Response::new(v1::GetStatusResponse {
            status: Some(to_status(&snap)),
        }))
    }

    async fn promote(
        &self,
        request: Request<v1::PromoteRequest>,
    ) -> Result<Response<v1::PromoteResponse>, Status> {
        let req = request.into_inner();
        if req.target_primary != self.pod {
            return Err(Status::failed_precondition(format!(
                "promote targets {}, but this agent serves {}",
                req.target_primary, self.pod
            )));
        }
        let key = format!("promote:{}", req.target_primary);
        match self
            .epoch
            .check(req.decision_epoch, &key)
            .map_err(epoch_status)?
        {
            Outcome::Apply => {
                let timeline = self.instance.promote().await.map_err(internal)?;
                // The promotion happened; record the epoch before the (read-only)
                // status fetch so a failure there cannot cause a re-promote.
                self.epoch.commit(req.decision_epoch, &key);
                let snap = self.instance.snapshot().await.map_err(internal)?;
                Ok(Response::new(v1::PromoteResponse {
                    new_timeline: timeline,
                    promote_lsn: Some(v1::Lsn {
                        value: snap.write_lsn,
                    }),
                }))
            }
            Outcome::Replay => {
                let snap = self.instance.snapshot().await.map_err(internal)?;
                Ok(Response::new(v1::PromoteResponse {
                    new_timeline: snap.timeline,
                    promote_lsn: Some(v1::Lsn {
                        value: snap.write_lsn,
                    }),
                }))
            }
        }
    }

    async fn fence(
        &self,
        request: Request<v1::FenceRequest>,
    ) -> Result<Response<v1::FenceResponse>, Status> {
        let req = request.into_inner();
        let key = format!("fence:{}", req.fenced);
        if let Outcome::Apply = self
            .epoch
            .check(req.decision_epoch, &key)
            .map_err(epoch_status)?
        {
            self.instance
                .set_fenced(req.fenced)
                .await
                .map_err(internal)?;
            self.epoch.commit(req.decision_epoch, &key);
        }
        Ok(Response::new(v1::FenceResponse {}))
    }

    async fn rejoin_as_standby(
        &self,
        request: Request<v1::RejoinAsStandbyRequest>,
    ) -> Result<Response<v1::RejoinAsStandbyResponse>, Status> {
        let req = request.into_inner();
        let upstream = req
            .upstream
            .ok_or_else(|| Status::invalid_argument("upstream is required"))?;
        let target = format!("{}:{}", upstream.host, upstream.port);
        let key = format!("rejoin:{target}:{}", req.allow_rewind);
        match self
            .epoch
            .check(req.decision_epoch, &key)
            .map_err(epoch_status)?
        {
            Outcome::Apply => {
                let rewound = self
                    .instance
                    .rejoin(&target, req.allow_rewind)
                    .await
                    .map_err(internal)?;
                self.epoch.commit(req.decision_epoch, &key);
                Ok(Response::new(v1::RejoinAsStandbyResponse { rewound }))
            }
            // The rewind is not repeated on a retry; report what the first apply
            // would have done.
            Outcome::Replay => Ok(Response::new(v1::RejoinAsStandbyResponse {
                rewound: req.allow_rewind,
            })),
        }
    }

    async fn create_restore_point(
        &self,
        request: Request<v1::CreateRestorePointRequest>,
    ) -> Result<Response<v1::CreateRestorePointResponse>, Status> {
        let req = request.into_inner();
        // Reject an empty or over-63-byte name up front: PostgreSQL silently
        // truncates a restore-point name to 63 bytes, so two long names sharing
        // a prefix would collide onto one record.
        check_ident("restore point name", &req.name, true)?;
        // Single-flight per name (see the field doc): the map lock is only held
        // to fetch/create the name's slot; the slot lock is held across the
        // PostgreSQL call, so a concurrent same-name caller blocks and then
        // replays the winner's point instead of writing a second record at a
        // different LSN. The work runs in a SPAWNED task that owns the slot:
        // a cancelled RPC (client hangup) must not abandon a create after
        // PostgreSQL wrote the point but before the slot recorded it — the
        // task finishes and stores it, and the retry replays. The PostgreSQL
        // call is bounded so a hung backend cannot wedge same-name retries
        // forever; a timed-out create is outcome-unknown, and if it DID land,
        // the retry's duplicate makes the barrier fail verification loudly
        // (recovery-by-name + recorded-LSN comparison) rather than serve a
        // divergent point silently.
        let slot = self
            .restore_points
            .lock()
            .await
            .entry(req.name.clone())
            .or_default()
            .clone();
        let instance = self.instance.clone();
        let name = req.name.clone();
        let rp = tokio::spawn(async move {
            let mut point = slot.lock().await;
            if let Some(rp) = *point {
                return Ok(rp);
            }
            let rp = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                instance.create_restore_point(&name),
            )
            .await
            .map_err(|_| anyhow::anyhow!("restore point {name:?} timed out"))??;
            *point = Some(rp);
            Ok::<_, anyhow::Error>(rp)
        })
        .await
        .map_err(|e| Status::internal(format!("restore point task: {e}")))?
        .map_err(internal)?;
        Ok(Response::new(restore_point_response(rp)))
    }

    async fn switch_wal(
        &self,
        request: Request<v1::SwitchWalRequest>,
    ) -> Result<Response<v1::SwitchWalResponse>, Status> {
        let lsn = self
            .instance
            .switch_wal(request.into_inner().wait_archived)
            .await
            .map_err(internal)?;
        Ok(Response::new(v1::SwitchWalResponse {
            lsn: Some(v1::Lsn { value: lsn }),
        }))
    }

    // ---- Not yet implemented (later steps) -------------------------------

    async fn reload_config(
        &self,
        _r: Request<v1::ReloadConfigRequest>,
    ) -> Result<Response<v1::ReloadConfigResponse>, Status> {
        Err(Status::unimplemented("reload_config"))
    }
    async fn run_backup(
        &self,
        _r: Request<v1::RunBackupRequest>,
    ) -> Result<Response<v1::RunBackupResponse>, Status> {
        Err(Status::unimplemented("run_backup"))
    }
    async fn run_restore(
        &self,
        _r: Request<v1::RunRestoreRequest>,
    ) -> Result<Response<v1::RunRestoreResponse>, Status> {
        Err(Status::unimplemented("run_restore"))
    }
    async fn stanza_create(
        &self,
        _r: Request<v1::StanzaCreateRequest>,
    ) -> Result<Response<v1::StanzaCreateResponse>, Status> {
        Err(Status::unimplemented("stanza_create"))
    }
    async fn stanza_check(
        &self,
        _r: Request<v1::StanzaCheckRequest>,
    ) -> Result<Response<v1::StanzaCheckResponse>, Status> {
        Err(Status::unimplemented("stanza_check"))
    }
    async fn prepare_source(
        &self,
        _r: Request<v1::PrepareSourceRequest>,
    ) -> Result<Response<v1::PrepareSourceResponse>, Status> {
        Err(Status::unimplemented("prepare_source"))
    }
    async fn start_workflow(
        &self,
        _r: Request<v1::StartWorkflowRequest>,
    ) -> Result<Response<v1::StartWorkflowResponse>, Status> {
        Err(Status::unimplemented("start_workflow"))
    }
    async fn stop_workflow(
        &self,
        _r: Request<v1::StopWorkflowRequest>,
    ) -> Result<Response<v1::StopWorkflowResponse>, Status> {
        Err(Status::unimplemented("stop_workflow"))
    }
    type WatchWorkflowsStream = BoxStream<v1::WatchWorkflowsResponse>;
    async fn watch_workflows(
        &self,
        _r: Request<v1::WatchWorkflowsRequest>,
    ) -> Result<Response<Self::WatchWorkflowsStream>, Status> {
        Err(Status::unimplemented("watch_workflows"))
    }
    async fn checkpoint(
        &self,
        _r: Request<v1::CheckpointRequest>,
    ) -> Result<Response<v1::CheckpointResponse>, Status> {
        Err(Status::unimplemented("checkpoint"))
    }
    async fn emit_journal(
        &self,
        _r: Request<v1::EmitJournalRequest>,
    ) -> Result<Response<v1::EmitJournalResponse>, Status> {
        Err(Status::unimplemented("emit_journal"))
    }
    type ShardStreamStream = BoxStream<v1::ShardStreamResponse>;
    async fn shard_stream(
        &self,
        _r: Request<v1::ShardStreamRequest>,
    ) -> Result<Response<Self::ShardStreamStream>, Status> {
        Err(Status::unimplemented("shard_stream"))
    }
    async fn drop_slot(
        &self,
        _r: Request<v1::DropSlotRequest>,
    ) -> Result<Response<v1::DropSlotResponse>, Status> {
        Err(Status::unimplemented("drop_slot"))
    }
    /// `req.sql` must be a single statement. The operator parses and guarantees
    /// this; the agent has no parser. Idempotent retry is only safe for a single
    /// statement — a multi-statement batch that commits part of its work before
    /// failing cannot be re-executed cleanly, and neither the agent nor its
    /// [`SchemaLog`](crate::schema::SchemaLog) can detect a partial commit.
    async fn exec_schema(
        &self,
        request: Request<v1::ExecSchemaRequest>,
    ) -> Result<Response<v1::ExecSchemaResponse>, Status> {
        let req = request.into_inner();
        match self
            .schema
            .claim(&req.operation_id, &req.sql)
            .map_err(schema_status)?
        {
            Claim::Execute => match self.instance.exec_sql(&req.sql).await {
                Ok(()) => {
                    self.schema.mark_done(&req.operation_id);
                    Ok(Response::new(v1::ExecSchemaResponse {}))
                }
                // Mark the claim failed (keeping its sql binding) so the
                // operator's same-sql retry re-executes rather than replaying a
                // success that never happened, while a different-sql reuse of the
                // id stays rejected.
                Err(e) => {
                    self.schema.mark_failed(&req.operation_id);
                    Err(internal(e))
                }
            },
            Claim::Replay => Ok(Response::new(v1::ExecSchemaResponse {})),
        }
    }
    async fn migration_step(
        &self,
        _r: Request<v1::MigrationStepRequest>,
    ) -> Result<Response<v1::MigrationStepResponse>, Status> {
        Err(Status::unimplemented("migration_step"))
    }
    async fn create_database(
        &self,
        request: Request<v1::CreateDatabaseRequest>,
    ) -> Result<Response<v1::CreateDatabaseResponse>, Status> {
        let req = request.into_inner();
        // A reassigned pod IP could route this to another node incarnation;
        // with `adopt` that would silently re-stamp the wrong database.
        if !req.target_pod_uid.is_empty()
            && !self.pod_uid.is_empty()
            && req.target_pod_uid != self.pod_uid
        {
            // ABORTED, not FAILED_PRECONDITION: this is a routing accident (a
            // reassigned pod IP), not a data verdict — the caller must retry
            // against a re-resolved address, never treat it as a database that
            // needs adopting.
            return Err(Status::aborted(format!(
                "request targets pod uid {}, but this agent serves {}",
                req.target_pod_uid, self.pod_uid
            )));
        }
        check_ident("database name", &req.name, true)?;
        check_ident("owner", &req.owner, false)?;
        check_provenance(&req.provenance)?;
        if req.adopt && req.provenance.is_empty() {
            return Err(Status::invalid_argument(
                "adopt requires a provenance marker to stamp",
            ));
        }
        self.instance
            .create_database(&req.name, &req.owner, &req.provenance, req.adopt)
            .await
            .map_err(|e| match e.downcast::<ForeignDatabase>() {
                Ok(foreign) => Status::failed_precondition(foreign.to_string()),
                Err(other) => internal(other),
            })?;
        // Attest what was actually verified: a legacy agent returns an empty
        // response, which the caller must treat as unverified.
        Ok(Response::new(v1::CreateDatabaseResponse {
            verified_provenance: req.provenance,
            served_pod_uid: self.pod_uid.clone(),
        }))
    }
    async fn drop_database(
        &self,
        request: Request<v1::DropDatabaseRequest>,
    ) -> Result<Response<v1::DropDatabaseResponse>, Status> {
        let req = request.into_inner();
        check_ident("database name", &req.name, true)?;
        self.instance
            .drop_database(&req.name)
            .await
            .map_err(internal)?;
        Ok(Response::new(v1::DropDatabaseResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance::fake::FakeInstance;

    fn svc(instance: FakeInstance) -> AgentSvc<FakeInstance> {
        AgentSvc::new(Arc::new(instance), "pod-0".into())
    }

    #[tokio::test]
    async fn get_status_reports_role() {
        let s = svc(FakeInstance::standby());
        let resp = s
            .get_status(Request::new(v1::GetStatusRequest {}))
            .await
            .unwrap();
        assert_eq!(
            resp.into_inner().status.unwrap().role,
            v1::InstanceRole::Standby as i32
        );
    }

    #[tokio::test]
    async fn promote_is_epoch_guarded_and_idempotent() {
        let s = svc(FakeInstance::standby());
        // Wrong epoch (0) rejected.
        assert!(
            s.promote(Request::new(v1::PromoteRequest {
                target_primary: "pod-0".into(),
                decision_epoch: 0,
            }))
            .await
            .is_err()
        );
        // Apply.
        let first = s
            .promote(Request::new(v1::PromoteRequest {
                target_primary: "pod-0".into(),
                decision_epoch: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(first.new_timeline, 2);
        // Idempotent retry returns the same timeline, does not promote again.
        let retry = s
            .promote(Request::new(v1::PromoteRequest {
                target_primary: "pod-0".into(),
                decision_epoch: 1,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(retry.new_timeline, 2);
        // Now a primary.
        let st = s
            .get_status(Request::new(v1::GetStatusRequest {}))
            .await
            .unwrap();
        assert_eq!(
            st.into_inner().status.unwrap().role,
            v1::InstanceRole::Primary as i32
        );
    }

    #[tokio::test]
    async fn a_failed_promote_retries_instead_of_replaying_success() {
        let instance = FakeInstance::standby();
        instance.set_promote_fails(true);
        let s = svc(instance);
        let req = || {
            Request::new(v1::PromoteRequest {
                target_primary: "pod-0".into(),
                decision_epoch: 1,
            })
        };
        // First attempt fails inside the instance.
        assert!(s.promote(req()).await.is_err());
        // The retry must re-execute (and fail again) — not replay a success that
        // never happened.
        assert!(s.promote(req()).await.is_err());
        // Once the instance can promote, the same epoch finally applies.
        s.instance.set_promote_fails(false);
        let ok = s.promote(req()).await.unwrap().into_inner();
        assert_eq!(ok.new_timeline, 2);
    }

    #[tokio::test]
    async fn promote_refuses_a_foreign_target() {
        let s = svc(FakeInstance::standby());
        let err = s
            .promote(Request::new(v1::PromoteRequest {
                target_primary: "pod-9".into(),
                decision_epoch: 1,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn fence_sets_and_clears() {
        let s = svc(FakeInstance::primary());
        s.fence(Request::new(v1::FenceRequest {
            fenced: true,
            decision_epoch: 1,
        }))
        .await
        .unwrap();
        let st = s
            .get_status(Request::new(v1::GetStatusRequest {}))
            .await
            .unwrap();
        let status = st.into_inner().status.unwrap();
        assert!(status.fenced);
        assert!(!status.ready, "a fenced instance is not ready");
    }

    #[tokio::test]
    async fn exec_schema_is_idempotent_by_operation_id() {
        let s = svc(FakeInstance::primary());
        let req = |op: &str, sql: &str| {
            Request::new(v1::ExecSchemaRequest {
                operation_id: op.into(),
                sql: sql.into(),
            })
        };
        s.exec_schema(req("op1", "CREATE TABLE t()")).await.unwrap();
        // A retry with the same id + sql replays without re-executing.
        s.exec_schema(req("op1", "CREATE TABLE t()")).await.unwrap();
        assert_eq!(s.instance.executed(), vec!["CREATE TABLE t()".to_string()]);
        // The same id with different sql, and an empty id, are rejected.
        assert_eq!(
            s.exec_schema(req("op1", "DROP TABLE t"))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            s.exec_schema(req("", "SELECT 1")).await.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn exec_schema_failure_retries_instead_of_replaying() {
        let instance = FakeInstance::primary();
        instance.set_exec_fails(true);
        let s = svc(instance);
        let req = || {
            Request::new(v1::ExecSchemaRequest {
                operation_id: "op1".into(),
                sql: "CREATE TABLE t()".into(),
            })
        };
        assert!(s.exec_schema(req()).await.is_err());
        // The failed op was not recorded done; once the instance recovers, the
        // same id re-executes rather than replaying a phantom success.
        s.instance.set_exec_fails(false);
        s.exec_schema(req()).await.unwrap();
        assert_eq!(s.instance.executed(), vec!["CREATE TABLE t()".to_string()]);
    }

    #[tokio::test]
    async fn create_database_is_idempotent_and_records_owner() {
        let s = svc(FakeInstance::primary());
        let req = |name: &str, owner: &str| {
            Request::new(v1::CreateDatabaseRequest {
                name: name.into(),
                owner: owner.into(),
                ..Default::default()
            })
        };
        s.create_database(req("mycl-x40-x80", "app")).await.unwrap();
        // A repeat is a success and leaves the original owner untouched.
        s.create_database(req("mycl-x40-x80", "other"))
            .await
            .unwrap();
        assert_eq!(s.instance.databases(), vec!["mycl-x40-x80".to_string()]);
        assert_eq!(s.instance.owner_of("mycl-x40-x80").as_deref(), Some("app"));
    }

    #[tokio::test]
    async fn create_database_rejects_an_empty_name() {
        let s = svc(FakeInstance::primary());
        let err = s
            .create_database(Request::new(v1::CreateDatabaseRequest {
                name: String::new(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn database_ops_reject_overlong_identifiers() {
        let s = svc(FakeInstance::primary());
        let too_long = "a".repeat(64); // PostgreSQL truncates beyond 63 bytes.
        // An overlong name is rejected on both create and drop, and nothing is
        // executed against the instance (no truncated collision).
        assert_eq!(
            s.create_database(Request::new(v1::CreateDatabaseRequest {
                name: too_long.clone(),
                ..Default::default()
            }))
            .await
            .unwrap_err()
            .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            s.drop_database(Request::new(v1::DropDatabaseRequest {
                name: too_long.clone(),
            }))
            .await
            .unwrap_err()
            .code(),
            tonic::Code::InvalidArgument
        );
        // An overlong owner is rejected too.
        assert_eq!(
            s.create_database(Request::new(v1::CreateDatabaseRequest {
                name: "ok".into(),
                owner: too_long,
                ..Default::default()
            }))
            .await
            .unwrap_err()
            .code(),
            tonic::Code::InvalidArgument
        );
        assert!(s.instance.databases().is_empty());
    }

    #[tokio::test]
    async fn drop_database_is_idempotent() {
        let s = svc(FakeInstance::primary());
        s.create_database(Request::new(v1::CreateDatabaseRequest {
            name: "shard-a".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
        let drop = || {
            Request::new(v1::DropDatabaseRequest {
                name: "shard-a".into(),
            })
        };
        s.drop_database(drop()).await.unwrap();
        // Dropping an already-absent database still succeeds.
        s.drop_database(drop()).await.unwrap();
        assert!(s.instance.databases().is_empty());
    }

    #[tokio::test]
    async fn database_op_failure_maps_to_internal() {
        let instance = FakeInstance::primary();
        instance.set_db_fails(true);
        let s = svc(instance);
        let err = s
            .create_database(Request::new(v1::CreateDatabaseRequest {
                name: "shard-a".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
    }

    #[tokio::test]
    async fn create_database_stamps_and_verifies_provenance() {
        let s = svc(FakeInstance::primary());
        let req = |provenance: &str, adopt: bool| {
            Request::new(v1::CreateDatabaseRequest {
                name: "mycl-x40-x80".into(),
                provenance: provenance.into(),
                adopt,
                ..Default::default()
            })
        };
        s.create_database(req("uid-a", false)).await.unwrap();
        assert_eq!(
            s.instance.marker_of("mycl-x40-x80").as_deref(),
            Some("pgshard-provenance:uid-a")
        );
        // Same placement retries idempotently.
        s.create_database(req("uid-a", false)).await.unwrap();
        // A different placement is fenced out, and the marker is untouched.
        let err = s.create_database(req("uid-b", false)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            s.instance.marker_of("mycl-x40-x80").as_deref(),
            Some("pgshard-provenance:uid-a")
        );
        // Explicit adoption re-stamps.
        s.create_database(req("uid-b", true)).await.unwrap();
        assert_eq!(
            s.instance.marker_of("mycl-x40-x80").as_deref(),
            Some("pgshard-provenance:uid-b")
        );
    }

    #[tokio::test]
    async fn create_database_fences_an_unmarked_existing_database() {
        let s = svc(FakeInstance::primary());
        // A retained database with no marker (pre-provenance data, or a crash
        // between CREATE DATABASE and the comment) is never silently adopted.
        s.instance.seed_database("mycl-x40-x80", "app", None);
        let req = |adopt: bool| {
            Request::new(v1::CreateDatabaseRequest {
                name: "mycl-x40-x80".into(),
                provenance: "uid-a".into(),
                adopt,
                ..Default::default()
            })
        };
        let err = s.create_database(req(false)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        s.create_database(req(true)).await.unwrap();
        assert_eq!(
            s.instance.marker_of("mycl-x40-x80").as_deref(),
            Some("pgshard-provenance:uid-a")
        );
    }

    #[tokio::test]
    async fn create_database_validates_provenance_and_adopt() {
        let s = svc(FakeInstance::primary());
        let req = |provenance: String, adopt: bool| {
            Request::new(v1::CreateDatabaseRequest {
                name: "shard-a".into(),
                provenance,
                adopt,
                ..Default::default()
            })
        };
        // Adoption without a marker to stamp is meaningless.
        let err = s
            .create_database(req(String::new(), true))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        // Characters outside [A-Za-z0-9._:-] and overlong values are rejected.
        let err = s
            .create_database(req("uid'; DROP--".into(), false))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        let err = s
            .create_database(req("a".repeat(129), false))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(s.instance.databases().is_empty());
    }

    #[tokio::test]
    async fn create_restore_point_records_and_returns_lsn_and_timeline() {
        let instance = FakeInstance::primary();
        instance.set(|s| {
            s.write_lsn = 0x1234;
            s.timeline = 3;
        });
        let s = svc(instance);
        let resp = s
            .create_restore_point(Request::new(v1::CreateRestorePointRequest {
                name: "pgshard_barrier_1".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.lsn.unwrap().value, 0x1234);
        assert_eq!(resp.timeline, 3);
        assert_eq!(s.instance.restore_points(), vec!["pgshard_barrier_1"]);
    }

    #[tokio::test]
    async fn create_restore_point_rejects_an_empty_name() {
        let s = svc(FakeInstance::primary());
        let err = s
            .create_restore_point(Request::new(v1::CreateRestorePointRequest {
                name: String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn create_restore_point_is_rejected_on_a_standby() {
        // A restore point can only be created on a primary; a standby errors.
        let s = svc(FakeInstance::standby());
        let err = s
            .create_restore_point(Request::new(v1::CreateRestorePointRequest {
                name: "pgshard_barrier_1".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(s.instance.restore_points().is_empty());
    }

    #[tokio::test]
    async fn create_restore_point_is_idempotent_by_name() {
        let instance = FakeInstance::primary();
        instance.set(|s| s.write_lsn = 0xAA);
        let s = svc(instance);
        let req = || {
            Request::new(v1::CreateRestorePointRequest {
                name: "pgshard_barrier_1".into(),
            })
        };
        let first = s.create_restore_point(req()).await.unwrap().into_inner();
        // The instance advances between attempts, but a retry of the same barrier
        // returns the original point, not a divergent second one.
        s.instance.set(|st| st.write_lsn = 0xBB);
        let retry = s.create_restore_point(req()).await.unwrap().into_inner();
        assert_eq!(retry.lsn.unwrap().value, first.lsn.unwrap().value);
        assert_eq!(retry.timeline, first.timeline);
        // Only one PostgreSQL restore point was written.
        assert_eq!(s.instance.restore_points(), vec!["pgshard_barrier_1"]);
    }

    #[tokio::test]
    async fn concurrent_same_name_restore_points_are_single_flight() {
        let s = Arc::new(svc(FakeInstance::primary()));
        let gate = Arc::new(tokio::sync::Notify::new());
        s.instance.set_restore_point_gate(gate.clone());

        let spawn_call = |s: Arc<AgentSvc<FakeInstance>>| {
            tokio::spawn(async move {
                s.create_restore_point(Request::new(v1::CreateRestorePointRequest {
                    name: "pgshard_b1".into(),
                }))
                .await
            })
        };
        let first = spawn_call(s.clone());
        // Let the first call reach PostgreSQL (park at the fake's gate) before
        // the second arrives: without single-flight both pass the exists-check
        // and write the same name twice at different LSNs — and recovery by
        // name stops at the FIRST record, not the one the manifest holds.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        let second = spawn_call(s.clone());
        for _ in 0..20 {
            gate.notify_waiters();
            tokio::task::yield_now().await;
        }
        let a = first.await.unwrap().unwrap().into_inner();
        let b = second.await.unwrap().unwrap().into_inner();
        assert_eq!(a.lsn.unwrap().value, b.lsn.unwrap().value);
        assert_eq!(
            s.instance.restore_points(),
            vec!["pgshard_b1".to_string()],
            "exactly one restore point may be written per name, however many concurrent callers"
        );
    }

    #[tokio::test]
    async fn a_cancelled_caller_does_not_lose_or_duplicate_the_restore_point() {
        let s = Arc::new(svc(FakeInstance::primary()));
        s.instance.set(|st| st.write_lsn = 0xAA);
        let entered = Arc::new(tokio::sync::Notify::new());
        let gate = Arc::new(tokio::sync::Notify::new());
        s.instance
            .set_restore_point_gate_with_entered(entered.clone(), gate.clone());

        // The caller is aborted while PostgreSQL is mid-create: the slot-owning
        // task must survive the cancellation, store the point, and a retry must
        // REPLAY it — a lost point would make the barrier unrestorable, and a
        // re-create would write the same name at a second LSN.
        let entered_wait = entered.notified();
        let first = {
            let s = s.clone();
            tokio::spawn(async move {
                s.create_restore_point(Request::new(v1::CreateRestorePointRequest {
                    name: "pgshard_cancel".into(),
                }))
                .await
            })
        };
        entered_wait.await;
        first.abort();
        gate.notify_waiters();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while s.instance.restore_points().is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "the detached task must complete the create after the caller was aborted"
            );
            tokio::task::yield_now().await;
        }

        // A re-create would now land at a DIFFERENT LSN; the retry must
        // replay the stored 0xAA, proving the point was never re-created.
        s.instance.set(|st| st.write_lsn = 0xBB);
        let retry = s
            .create_restore_point(Request::new(v1::CreateRestorePointRequest {
                name: "pgshard_cancel".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            s.instance.restore_points(),
            vec!["pgshard_cancel".to_string()],
            "exactly one point: the retry replays, never re-creates"
        );
        assert_eq!(retry.lsn.unwrap().value, 0xAA);
    }

    #[tokio::test]
    async fn create_restore_point_rejects_an_overlong_name() {
        // PostgreSQL truncates a restore-point name at 63 bytes; reject upfront so
        // two long names cannot collide onto one record.
        let s = svc(FakeInstance::primary());
        let err = s
            .create_restore_point(Request::new(v1::CreateRestorePointRequest {
                name: "b".repeat(64),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn switch_wal_returns_the_switch_lsn() {
        let instance = FakeInstance::primary();
        instance.set(|s| s.write_lsn = 0x5000);
        let s = svc(instance);
        let resp = s
            .switch_wal(Request::new(v1::SwitchWalRequest {
                wait_archived: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.lsn.unwrap().value, 0x5000);
        assert_eq!(s.instance.wal_switches(), 1);
    }

    #[tokio::test]
    async fn switch_wal_rejects_wait_archived_without_switching() {
        let s = svc(FakeInstance::primary());
        let err = s
            .switch_wal(Request::new(v1::SwitchWalRequest {
                wait_archived: true,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert_eq!(
            s.instance.wal_switches(),
            0,
            "no switch happens on rejection"
        );
    }
}
