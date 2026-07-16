package agentclient

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
	"github.com/andrew01234567890/pgshard2/operator/test/fakes"
)

func dialFake(t *testing.T) (pgshardv1.AgentServiceClient, *fakes.FakeAgent) {
	t.Helper()
	fake, err := fakes.NewFakeAgent()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fake.Stop)
	pool := NewPool()
	t.Cleanup(pool.Close)
	host, port := fake.Addr()
	client, err := pool.Get(host, port)
	if err != nil {
		t.Fatal(err)
	}
	return client, fake
}

func TestStatusAndPromoteHandshake(t *testing.T) {
	client, _ := dialFake(t)
	ctx := context.Background()

	st, err := client.GetStatus(ctx, &pgshardv1.GetStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if st.Status.Role != pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY {
		t.Fatalf("expected standby, got %v", st.Status.Role)
	}

	promoted, err := client.Promote(ctx, &pgshardv1.PromoteRequest{
		TargetPrimary: "pod-0", DecisionEpoch: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if promoted.NewTimeline != 2 {
		t.Fatalf("expected timeline 2, got %d", promoted.NewTimeline)
	}

	st, _ = client.GetStatus(ctx, &pgshardv1.GetStatusRequest{})
	if st.Status.Role != pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY {
		t.Fatal("promotion must flip role to primary")
	}
}

func TestStaleAndZeroDecisionEpochsRejected(t *testing.T) {
	client, _ := dialFake(t)
	ctx := context.Background()

	if _, err := client.Promote(ctx, &pgshardv1.PromoteRequest{DecisionEpoch: 7}); err != nil {
		t.Fatal(err)
	}
	_, err := client.Fence(ctx, &pgshardv1.FenceRequest{Fenced: false, DecisionEpoch: 6})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale epoch must be FailedPrecondition, got %v", err)
	}
	_, err = client.Promote(ctx, &pgshardv1.PromoteRequest{DecisionEpoch: 0})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("zero epoch must be InvalidArgument, got %v", err)
	}
}

func TestExecSchemaIdempotency(t *testing.T) {
	const opID = "op-1"
	client, _ := dialFake(t)
	ctx := context.Background()

	if _, err := client.ExecSchema(ctx, &pgshardv1.ExecSchemaRequest{
		Sql: "CREATE TABLE t (id int8 PRIMARY KEY)", OperationId: opID,
	}); err != nil {
		t.Fatal(err)
	}
	// Retry with the same id and sql succeeds without re-executing.
	if _, err := client.ExecSchema(ctx, &pgshardv1.ExecSchemaRequest{
		Sql: "CREATE TABLE t (id int8 PRIMARY KEY)", OperationId: opID,
	}); err != nil {
		t.Fatal(err)
	}
	// Same id, different sql is rejected.
	_, err := client.ExecSchema(ctx, &pgshardv1.ExecSchemaRequest{
		Sql: "DROP TABLE t", OperationId: opID,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("id reuse with different sql must fail, got %v", err)
	}
	// Missing id is rejected.
	_, err = client.ExecSchema(ctx, &pgshardv1.ExecSchemaRequest{Sql: "SELECT 1"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing operation_id must fail, got %v", err)
	}
}
