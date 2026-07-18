// Package fakes provides an in-process pgshard-agent gRPC server with
// scriptable state, so controller tests run before (and without) the real
// Rust agent. It enforces the same contracts the real agent must: monotonic
// decision epochs, promote/rejoin role transitions, idempotent ExecSchema.
package fakes

import (
	"context"
	"fmt"
	"maps"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
)

// FakeAgent is one fake instance-manager endpoint.
type FakeAgent struct {
	pgshardv1.UnimplementedAgentServiceServer

	mu sync.Mutex

	// Status returned by GetStatus; script it via SetRole or by mutating
	// under the same lock in tests.
	Status *pgshardv1.InstanceStatus

	// Highest decision epoch applied, per the contract on PromoteRequest.
	DecisionEpoch uint64

	// Request key applied at DecisionEpoch: an equal-epoch retry is only
	// accepted when its key matches (idempotent replay), a differing one is
	// rejected. Empty until the first epoch-guarded command.
	appliedKey string

	// Calls records method names in arrival order; controller tests assert
	// command sequences (e.g. Fence before Promote) against it.
	Calls []string

	executedSchemaOps map[string]string

	// databases records shard databases created via CreateDatabase (name ->
	// owner) so controller tests can assert placement provisioning.
	databases map[string]string

	// dbProvenance mirrors the real agent's database-comment marker (name ->
	// provenance value; absent key = unmarked database).
	dbProvenance map[string]string

	// emptyStatus makes GetStatus return a response with a nil Status, to
	// exercise the operator's empty-status handling.
	emptyStatus bool

	// podUID, when set, mirrors the real agent's downward-API identity check:
	// identity-sensitive requests naming another pod uid are refused.
	podUID string

	// legacyResponses models a pre-provenance agent: requests succeed but the
	// response carries no attestation (proto3 ignores the unknown fields).
	legacyResponses bool

	server   *grpc.Server
	listener net.Listener
}

// SetLegacyResponses makes the fake behave like a pre-provenance agent:
// success with an empty (unattested) response, all new fields ignored.
func (f *FakeAgent) SetLegacyResponses(legacy bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.legacyResponses = legacy
}

// SetPodUID scripts the agent's own pod UID (downward API identity).
func (f *FakeAgent) SetPodUID(uid string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.podUID = uid
}

// SetEmptyStatus makes GetStatus return a nil Status message.
func (f *FakeAgent) SetEmptyStatus(empty bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.emptyStatus = empty
}

// SetReady scripts the ready flag.
func (f *FakeAgent) SetReady(ready bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Status.Ready = ready
}

// SetReceivedLSN scripts the received WAL position (failover election input).
func (f *FakeAgent) SetReceivedLSN(value uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Status.WalReceiveLsn = &pgshardv1.Lsn{Value: value}
}

// SetSystemID models a foreign data lineage (a reused PVC, a restore).
func (f *FakeAgent) SetSystemID(id uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Status.SystemId = id
}

// SetTimeline models an instance on a divergent WAL timeline.
func (f *FakeAgent) SetTimeline(tl uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Status.Timeline = tl
}

// Role reads the current instance role.
func (f *FakeAgent) Role() pgshardv1.InstanceRole {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Status.Role
}

// AppliedEpoch reads the highest decision epoch applied.
func (f *FakeAgent) AppliedEpoch() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.DecisionEpoch
}

// Client dials this fake over its own listener and returns an insecure
// in-process AgentService client.
func (f *FakeAgent) Client() (pgshardv1.AgentServiceClient, error) {
	host, port := f.Addr()
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", host, port),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return pgshardv1.NewAgentServiceClient(conn), nil
}

// NewFakeAgent starts a fake agent on an ephemeral localhost port.
func NewFakeAgent() (*FakeAgent, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	f := &FakeAgent{
		Status: &pgshardv1.InstanceStatus{
			Role:     pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY,
			Ready:    true,
			Timeline: 1,
			// A stable nonzero identity so envtest exercises the identity
			// fencing; tests set a divergent value to model a foreign PVC.
			SystemId: 4242,
		},
		executedSchemaOps: map[string]string{},
		databases:         map[string]string{},
		dbProvenance:      map[string]string{},
		server:            grpc.NewServer(),
		listener:          listener,
	}
	pgshardv1.RegisterAgentServiceServer(f.server, f)
	go func() { _ = f.server.Serve(listener) }()
	return f, nil
}

func (f *FakeAgent) Stop() {
	f.server.Stop()
}

// Addr returns host, port of the listening fake.
func (f *FakeAgent) Addr() (string, int32) {
	addr := f.listener.Addr().(*net.TCPAddr)
	return addr.IP.String(), int32(addr.Port)
}

func (f *FakeAgent) record(call string) {
	f.Calls = append(f.Calls, call)
}

// SetRole scripts the instance role.
func (f *FakeAgent) SetRole(role pgshardv1.InstanceRole) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Status.Role = role
}

func (f *FakeAgent) GetStatus(
	ctx context.Context, _ *pgshardv1.GetStatusRequest,
) (*pgshardv1.GetStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("GetStatus")
	if f.emptyStatus {
		return &pgshardv1.GetStatusResponse{}, nil
	}
	clone, ok := proto.Clone(f.Status).(*pgshardv1.InstanceStatus)
	if !ok {
		return nil, status.Error(codes.Internal, "clone failed")
	}
	return &pgshardv1.GetStatusResponse{Status: clone}, nil
}

// epochOutcome is what checkEpochLocked tells the caller to do.
type epochOutcome int

const (
	epochApply  epochOutcome = iota // fresh, higher epoch: execute
	epochReplay                     // identical equal-epoch retry: return prior result, do not re-execute
)

// checkEpochLocked enforces the decision-epoch contract from agent.proto:
// zero is rejected, a lower epoch is rejected, an equal epoch is accepted
// only when the request key is identical to the one already applied (then
// the caller replays its prior response without re-executing), and a higher
// epoch applies. key uniquely identifies the request payload.
func (f *FakeAgent) checkEpochLocked(epoch uint64, key string) (epochOutcome, error) {
	if epoch == 0 {
		return epochApply, status.Error(codes.InvalidArgument, "decision_epoch must be > 0")
	}
	if epoch < f.DecisionEpoch {
		return epochApply, status.Errorf(codes.FailedPrecondition,
			"stale decision epoch %d < %d", epoch, f.DecisionEpoch)
	}
	if epoch == f.DecisionEpoch {
		if f.appliedKey != key {
			return epochApply, status.Errorf(codes.FailedPrecondition,
				"decision epoch %d already applied a different request", epoch)
		}
		return epochReplay, nil
	}
	f.DecisionEpoch = epoch
	f.appliedKey = key
	return epochApply, nil
}

func (f *FakeAgent) Promote(
	ctx context.Context, req *pgshardv1.PromoteRequest,
) (*pgshardv1.PromoteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("Promote")
	outcome, err := f.checkEpochLocked(req.DecisionEpoch, "promote:"+req.TargetPrimary)
	if err != nil {
		return nil, err
	}
	if outcome == epochReplay {
		return &pgshardv1.PromoteResponse{NewTimeline: f.Status.Timeline}, nil
	}
	f.Status.Role = pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY
	f.Status.Timeline++
	return &pgshardv1.PromoteResponse{NewTimeline: f.Status.Timeline}, nil
}

func (f *FakeAgent) RejoinAsStandby(
	ctx context.Context, req *pgshardv1.RejoinAsStandbyRequest,
) (*pgshardv1.RejoinAsStandbyResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("RejoinAsStandby")
	outcome, err := f.checkEpochLocked(req.DecisionEpoch,
		fmt.Sprintf("rejoin:%v:%v", req.Upstream, req.AllowRewind))
	if err != nil {
		return nil, err
	}
	if outcome == epochReplay {
		return &pgshardv1.RejoinAsStandbyResponse{Rewound: req.AllowRewind}, nil
	}
	f.Status.Role = pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY
	return &pgshardv1.RejoinAsStandbyResponse{Rewound: req.AllowRewind}, nil
}

func (f *FakeAgent) Fence(
	ctx context.Context, req *pgshardv1.FenceRequest,
) (*pgshardv1.FenceResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("Fence")
	outcome, err := f.checkEpochLocked(req.DecisionEpoch, fmt.Sprintf("fence:%v", req.Fenced))
	if err != nil {
		return nil, err
	}
	if outcome == epochReplay {
		return &pgshardv1.FenceResponse{}, nil
	}
	f.Status.Fenced = req.Fenced
	return &pgshardv1.FenceResponse{}, nil
}

func (f *FakeAgent) ReloadConfig(
	ctx context.Context, _ *pgshardv1.ReloadConfigRequest,
) (*pgshardv1.ReloadConfigResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("ReloadConfig")
	return &pgshardv1.ReloadConfigResponse{}, nil
}

func (f *FakeAgent) CreateRestorePoint(
	ctx context.Context, req *pgshardv1.CreateRestorePointRequest,
) (*pgshardv1.CreateRestorePointResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("CreateRestorePoint:" + req.Name)
	return &pgshardv1.CreateRestorePointResponse{
		Lsn:      &pgshardv1.Lsn{Value: 42},
		Timeline: f.Status.Timeline,
	}, nil
}

func (f *FakeAgent) ExecSchema(
	ctx context.Context, req *pgshardv1.ExecSchemaRequest,
) (*pgshardv1.ExecSchemaResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("ExecSchema:" + req.OperationId)
	if req.OperationId == "" {
		return nil, status.Error(codes.InvalidArgument, "operation_id required")
	}
	if sql, done := f.executedSchemaOps[req.OperationId]; done {
		if sql != req.Sql {
			return nil, status.Error(codes.InvalidArgument,
				"operation_id reused with different sql")
		}
		return &pgshardv1.ExecSchemaResponse{}, nil
	}
	f.executedSchemaOps[req.OperationId] = req.Sql
	return &pgshardv1.ExecSchemaResponse{}, nil
}

func (f *FakeAgent) StanzaCreate(
	ctx context.Context, req *pgshardv1.StanzaCreateRequest,
) (*pgshardv1.StanzaCreateResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(fmt.Sprintf("StanzaCreate:%s", req.Stanza))
	return &pgshardv1.StanzaCreateResponse{}, nil
}

func (f *FakeAgent) CreateDatabase(
	ctx context.Context, req *pgshardv1.CreateDatabaseRequest,
) (*pgshardv1.CreateDatabaseResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("CreateDatabase:" + req.Name)
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "database name is required")
	}
	// Mirror the real agent's NAMEDATALEN-1 limit so tests can exercise the
	// controller's terminal-error handling.
	if len(req.Name) > 63 || len(req.Owner) > 63 {
		return nil, status.Error(codes.InvalidArgument, "identifier exceeds 63 bytes")
	}
	if f.legacyResponses {
		// A pre-provenance agent: ignores every new field, reports bare
		// success, attests nothing.
		if _, ok := f.databases[req.Name]; !ok {
			f.databases[req.Name] = req.Owner
		}
		return &pgshardv1.CreateDatabaseResponse{}, nil
	}
	if req.TargetPodUid != "" && f.podUID != "" && req.TargetPodUid != f.podUID {
		// Mirror the real agent: a routing accident is ABORTED (retry), never
		// FAILED_PRECONDITION (which reads as a database needing adoption).
		return nil, status.Errorf(codes.Aborted,
			"request targets pod uid %s, but this agent serves %s", req.TargetPodUid, f.podUID)
	}
	if req.Adopt && req.Provenance == "" {
		return nil, status.Error(codes.InvalidArgument, "adopt requires a provenance marker to stamp")
	}
	// Mirror the real agent's provenance validation so envtests cannot pass
	// requests the Rust service rejects.
	if len(req.Provenance) > 128 {
		return nil, status.Error(codes.InvalidArgument, "provenance exceeds 128 bytes")
	}
	for _, c := range req.Provenance {
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '.' || c == '_' || c == ':' || c == '-'
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "provenance may only contain [A-Za-z0-9._:-]")
		}
	}
	if _, ok := f.databases[req.Name]; ok {
		if req.Provenance != "" && f.dbProvenance[req.Name] != req.Provenance {
			if !req.Adopt {
				return nil, status.Errorf(codes.FailedPrecondition,
					"database %q already exists with provenance %q; refusing to adopt without explicit authorization",
					req.Name, f.dbProvenance[req.Name])
			}
			f.dbProvenance[req.Name] = req.Provenance
		}
		return f.attestedResponse(req), nil
	}
	f.databases[req.Name] = req.Owner
	if req.Provenance != "" {
		f.dbProvenance[req.Name] = req.Provenance
	}
	return f.attestedResponse(req), nil
}

func (f *FakeAgent) attestedResponse(req *pgshardv1.CreateDatabaseRequest) *pgshardv1.CreateDatabaseResponse {
	return &pgshardv1.CreateDatabaseResponse{
		VerifiedProvenance: req.Provenance,
		ServedPodUid:       f.podUID,
	}
}

func (f *FakeAgent) DropDatabase(
	ctx context.Context, req *pgshardv1.DropDatabaseRequest,
) (*pgshardv1.DropDatabaseResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("DropDatabase:" + req.Name)
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "database name is required")
	}
	delete(f.databases, req.Name)
	delete(f.dbProvenance, req.Name)
	return &pgshardv1.DropDatabaseResponse{}, nil
}

// Databases returns the shard databases the fake currently holds.
func (f *FakeAgent) Databases() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]string, len(f.databases))
	maps.Copy(out, f.databases)
	return out
}

// DatabaseProvenance returns the provenance marker stamped on a database
// (empty = unmarked or absent).
func (f *FakeAgent) DatabaseProvenance(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dbProvenance[name]
}

// SeedDatabase plants a pre-existing database with an arbitrary provenance
// marker (empty = unmarked), as a retained volume from another placement
// would leave behind.
func (f *FakeAgent) SeedDatabase(name, owner, provenance string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.databases[name] = owner
	if provenance == "" {
		delete(f.dbProvenance, name)
	} else {
		f.dbProvenance[name] = provenance
	}
}
