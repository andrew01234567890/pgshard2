// Package fakes provides an in-process pgshard-agent gRPC server with
// scriptable state, so controller tests run before (and without) the real
// Rust agent. It enforces the same contracts the real agent must: monotonic
// decision epochs, promote/rejoin role transitions, idempotent ExecSchema.
package fakes

import (
	"context"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
)

// FakeAgent is one fake instance-manager endpoint.
type FakeAgent struct {
	pgshardv1.UnimplementedAgentServiceServer

	mu sync.Mutex

	// Status returned by GetStatus; mutate via WithStatus/SetRole.
	Status *pgshardv1.InstanceStatus

	// Highest decision epoch applied, per the contract on PromoteRequest.
	DecisionEpoch uint64

	// Calls records method names in arrival order.
	Calls []string

	executedSchemaOps map[string]string

	server   *grpc.Server
	listener net.Listener
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
		},
		executedSchemaOps: map[string]string{},
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
	clone, ok := proto.Clone(f.Status).(*pgshardv1.InstanceStatus)
	if !ok {
		return nil, status.Error(codes.Internal, "clone failed")
	}
	return &pgshardv1.GetStatusResponse{Status: clone}, nil
}

func (f *FakeAgent) checkEpochLocked(epoch uint64) error {
	if epoch == 0 {
		return status.Error(codes.InvalidArgument, "decision_epoch must be > 0")
	}
	if epoch < f.DecisionEpoch {
		return status.Errorf(codes.FailedPrecondition,
			"stale decision epoch %d < %d", epoch, f.DecisionEpoch)
	}
	f.DecisionEpoch = epoch
	return nil
}

func (f *FakeAgent) Promote(
	ctx context.Context, req *pgshardv1.PromoteRequest,
) (*pgshardv1.PromoteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("Promote")
	if err := f.checkEpochLocked(req.DecisionEpoch); err != nil {
		return nil, err
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
	if err := f.checkEpochLocked(req.DecisionEpoch); err != nil {
		return nil, err
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
	if err := f.checkEpochLocked(req.DecisionEpoch); err != nil {
		return nil, err
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
