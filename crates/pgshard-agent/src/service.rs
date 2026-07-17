//! The `AgentService` gRPC surface. This first agent implements the HA path the
//! operator already drives — status polling and the epoch-guarded
//! promote/fence/rejoin handshake — over the [`Instance`] abstraction. The
//! remaining RPCs (backups, restore, stanzas, replication, DDL, CDC) are wired
//! in later steps and currently return `Unimplemented`.

use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use pgshard_proto::v1;
use v1::agent_service_server::AgentService;

use crate::epoch::{EpochError, EpochGuard, Outcome};
use crate::instance::Instance;
use crate::status::to_status;

pub struct AgentSvc<I: Instance> {
    instance: Arc<I>,
    /// This instance's pod name (`PGSHARD_POD`); a Promote aimed at a different
    /// target is refused.
    pod: String,
    epoch: EpochGuard,
}

impl<I: Instance> AgentSvc<I> {
    pub fn new(instance: Arc<I>, pod: String) -> Self {
        Self {
            instance,
            pod,
            epoch: EpochGuard::new(),
        }
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

    // ---- Not yet implemented (later steps) -------------------------------

    async fn reload_config(
        &self,
        _r: Request<v1::ReloadConfigRequest>,
    ) -> Result<Response<v1::ReloadConfigResponse>, Status> {
        Err(Status::unimplemented("reload_config"))
    }
    async fn create_restore_point(
        &self,
        _r: Request<v1::CreateRestorePointRequest>,
    ) -> Result<Response<v1::CreateRestorePointResponse>, Status> {
        Err(Status::unimplemented("create_restore_point"))
    }
    async fn switch_wal(
        &self,
        _r: Request<v1::SwitchWalRequest>,
    ) -> Result<Response<v1::SwitchWalResponse>, Status> {
        Err(Status::unimplemented("switch_wal"))
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
    async fn exec_schema(
        &self,
        _r: Request<v1::ExecSchemaRequest>,
    ) -> Result<Response<v1::ExecSchemaResponse>, Status> {
        Err(Status::unimplemented("exec_schema"))
    }
    async fn migration_step(
        &self,
        _r: Request<v1::MigrationStepRequest>,
    ) -> Result<Response<v1::MigrationStepResponse>, Status> {
        Err(Status::unimplemented("migration_step"))
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
}
