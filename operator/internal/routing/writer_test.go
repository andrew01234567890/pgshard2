package routing

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
)

func newClient(t *testing.T) *fake.ClientBuilder {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := pgshardv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme)
}

func baseSpec() pgshardv1alpha1.PgShardRoutingSpec {
	return pgshardv1alpha1.PgShardRoutingSpec{
		HashFunction: "xxhash64_v1",
		Shards: []pgshardv1alpha1.RoutingShard{{
			Name:     "c-min-max",
			KeyRange: pgshardv1alpha1.KeyRange{},
			State:    pgshardv1alpha1.RoutingServing,
			Primary:  &pgshardv1alpha1.RoutingEndpoint{Pod: "p1", Host: "10.0.0.1", Port: 5432},
		}},
	}
}

func TestWriteEpochAndGenerationSemantics(t *testing.T) {
	ctx := context.Background()
	c := newClient(t).Build()
	key := types.NamespacedName{Name: "c", Namespace: "default"}

	// First write creates epoch 1 / generation 1.
	epoch, changed, err := Write(ctx, c, key, baseSpec())
	if err != nil || !changed || epoch != 1 {
		t.Fatalf("create: epoch=%d changed=%v err=%v", epoch, changed, err)
	}

	// Identical spec: no write, epoch unchanged.
	epoch, changed, err = Write(ctx, c, key, baseSpec())
	if err != nil || changed || epoch != 1 {
		t.Fatalf("no-op: epoch=%d changed=%v err=%v", epoch, changed, err)
	}

	// Endpoint-only change (failover): epoch bumps, generation does not.
	moved := baseSpec()
	moved.Shards[0].Primary = &pgshardv1alpha1.RoutingEndpoint{Pod: "p2", Host: "10.0.0.2", Port: 5432}
	epoch, changed, err = Write(ctx, c, key, moved)
	if err != nil || !changed || epoch != 2 {
		t.Fatalf("failover write: epoch=%d changed=%v err=%v", epoch, changed, err)
	}
	var current pgshardv1alpha1.PgShardRouting
	if err := c.Get(ctx, key, &current); err != nil {
		t.Fatal(err)
	}
	if current.Spec.TopologyGeneration != 1 {
		t.Fatalf("endpoint change must not bump generation: %d", current.Spec.TopologyGeneration)
	}

	// Structural change (shard split): epoch AND generation bump.
	split := moved
	split.Shards = []pgshardv1alpha1.RoutingShard{
		{Name: "c-min-80", KeyRange: pgshardv1alpha1.KeyRange{End: "80"}, State: pgshardv1alpha1.RoutingServing},
		{Name: "c-80-max", KeyRange: pgshardv1alpha1.KeyRange{Start: "80"}, State: pgshardv1alpha1.RoutingServing},
	}
	epoch, changed, err = Write(ctx, c, key, split)
	if err != nil || !changed || epoch != 3 {
		t.Fatalf("split write: epoch=%d changed=%v err=%v", epoch, changed, err)
	}
	if err := c.Get(ctx, key, &current); err != nil {
		t.Fatal(err)
	}
	if current.Spec.TopologyGeneration != 2 {
		t.Fatalf("structural change must bump generation: %d", current.Spec.TopologyGeneration)
	}

	// Table catalog change is structural too.
	tables := split
	tables.Tables = []pgshardv1alpha1.RoutingTable{{
		Schema: "public", Name: "orders",
		Type: pgshardv1alpha1.RoutingTableType("sharded"), ShardKeyColumn: "customer_id",
	}}
	if _, _, err = Write(ctx, c, key, tables); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, key, &current); err != nil {
		t.Fatal(err)
	}
	if current.Spec.Epoch != 4 || current.Spec.TopologyGeneration != 3 {
		t.Fatalf("catalog change: epoch=%d gen=%d",
			current.Spec.Epoch, current.Spec.TopologyGeneration)
	}
}
