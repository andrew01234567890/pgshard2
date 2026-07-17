package routing

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
)

func tableConfig(name string, tables ...string) pgshardv1alpha1.PgShardTableConfig {
	c := pgshardv1alpha1.PgShardTableConfig{}
	c.Name = name
	for _, tbl := range tables {
		c.Spec.Tables = append(c.Spec.Tables, pgshardv1alpha1.TableEntry{
			Name: tbl, Type: pgshardv1alpha1.TableSharded, ShardKeyColumn: "id",
		})
	}
	return c
}

func cluster() *pgshardv1alpha1.PgShardCluster {
	c := &pgshardv1alpha1.PgShardCluster{}
	c.Name = "c"
	c.Spec.Postgres.HashFunction = "xxhash64_v1"
	c.Spec.Router = &pgshardv1alpha1.RouterSpec{WriteLeaseSeconds: 10}
	return c
}

func shard(name, start, end string, serving bool, primary string, role pgshardv1alpha1.ShardRole) pgshardv1alpha1.PgShardShard {
	s := pgshardv1alpha1.PgShardShard{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: pgshardv1alpha1.PgShardShardSpec{
			ClusterRef: "c",
			KeyRange:   pgshardv1alpha1.KeyRange{Start: start, End: end},
			Serving:    serving,
			Role:       role,
			Replicas:   2,
		},
	}
	s.Status.CurrentPrimary = primary
	s.Status.Instances = []pgshardv1alpha1.InstanceState{
		{Pod: primary, Role: "primary", Ready: true},
		{Pod: primary + "-r", Role: "replica", Ready: true},
	}
	return s
}

func endpoints(pods ...string) map[string]pgshardv1alpha1.RoutingEndpoint {
	out := map[string]pgshardv1alpha1.RoutingEndpoint{}
	for i, pod := range pods {
		out[pod] = pgshardv1alpha1.RoutingEndpoint{
			Pod: pod, Host: "10.0.0." + string(rune('1'+i)), Port: 5432,
		}
	}
	return out
}

func TestCompileProducesOrderedServingView(t *testing.T) {
	in := CompileInputs{
		Cluster: cluster(),
		Shards: []pgshardv1alpha1.PgShardShard{
			shard("c-80-max", "80", "", true, "p2", pgshardv1alpha1.ShardRoleData),
			shard("c-min-80", "", "80", true, "p1", pgshardv1alpha1.ShardRoleData),
			shard("c-system", "", "", true, "ps", pgshardv1alpha1.ShardRoleSystem),
		},
		Endpoints: endpoints("p1", "p2", "ps", "p1-r", "p2-r"),
	}
	spec, err := Compile(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Shards) != 2 || spec.Shards[0].Name != "c-min-80" {
		t.Fatalf("expected sorted data shards, got %+v", spec.Shards)
	}
	if spec.Shards[0].Primary == nil || spec.Shards[0].Primary.Pod != "p1" {
		t.Fatalf("primary endpoint missing: %+v", spec.Shards[0])
	}
	if len(spec.Shards[0].Replicas) != 1 || !spec.Shards[0].Replicas[0].CanRead {
		t.Fatalf("replica endpoint missing: %+v", spec.Shards[0].Replicas)
	}
	if spec.SequenceEndpoint == nil || spec.SequenceEndpoint.Pod != "ps" {
		t.Fatalf("system shard must publish the sequence endpoint: %+v", spec.SequenceEndpoint)
	}
}

func TestCompileRejectsPartitionGaps(t *testing.T) {
	in := CompileInputs{
		Cluster: cluster(),
		Shards: []pgshardv1alpha1.PgShardShard{
			shard("a", "", "40", true, "p1", pgshardv1alpha1.ShardRoleData),
			shard("b", "80", "", true, "p2", pgshardv1alpha1.ShardRoleData),
		},
		Endpoints: endpoints("p1", "p2"),
	}
	if _, err := Compile(in); err == nil {
		t.Fatal("gap in serving ranges must fail compilation")
	}
	// A hidden shard covering the gap does not help — only serving counts.
	in.Shards = append(in.Shards, shard("mid", "40", "80", false, "p3", pgshardv1alpha1.ShardRoleData))
	if _, err := Compile(in); err == nil {
		t.Fatal("hidden shards must not satisfy the partition")
	}
	// Serving it does.
	in.Shards[2].Spec.Serving = true
	if _, err := Compile(in); err != nil {
		t.Fatal(err)
	}
}

func TestCompileDetectsDuplicateTablesAcrossConfigs(t *testing.T) {
	tc := func(name, table string) pgshardv1alpha1.PgShardTableConfig {
		c := pgshardv1alpha1.PgShardTableConfig{}
		c.Name = name
		c.Spec.Tables = []pgshardv1alpha1.TableEntry{{
			Name: table, Type: pgshardv1alpha1.TableSharded, ShardKeyColumn: "id",
		}}
		return c
	}
	in := CompileInputs{
		Cluster: cluster(),
		Shards: []pgshardv1alpha1.PgShardShard{
			shard("all", "", "", true, "p1", pgshardv1alpha1.ShardRoleData),
		},
		Endpoints:    endpoints("p1"),
		TableConfigs: []pgshardv1alpha1.PgShardTableConfig{tc("team-a", "orders"), tc("team-b", "orders")},
	}
	_, err := Compile(in)
	var dup *DuplicateTableError
	if !errors.As(err, &dup) || dup.FirstConfig != "team-a" || dup.SecondConfig != "team-b" {
		t.Fatalf("expected duplicate-table error naming both configs, got %v", err)
	}
}

func TestCompileRejectsDuplicateGateIDs(t *testing.T) {
	in := CompileInputs{
		Cluster:   cluster(),
		Shards:    []pgshardv1alpha1.PgShardShard{shard("all", "", "", true, "p1", pgshardv1alpha1.ShardRoleData)},
		Endpoints: endpoints("p1"),
		Gates:     []pgshardv1alpha1.RoutingGate{{ID: "g1"}, {ID: "g1"}},
	}
	if _, err := Compile(in); err == nil {
		t.Fatal("duplicate gate id must be rejected (would make gate order nondeterministic)")
	}
}

func TestCompileExcludesNonReplicaInstances(t *testing.T) {
	s := shard("c-min-max", "", "", true, "p1", pgshardv1alpha1.ShardRoleData)
	// A Ready instance with an empty role must not be published as a replica.
	s.Status.Instances = append(s.Status.Instances, pgshardv1alpha1.InstanceState{Pod: "ghost", Ready: true})
	in := CompileInputs{
		Cluster:   cluster(),
		Shards:    []pgshardv1alpha1.PgShardShard{s},
		Endpoints: endpoints("p1", "p1-r", "ghost"),
	}
	spec, err := Compile(in)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range spec.Shards[0].Replicas {
		if r.Pod == "ghost" {
			t.Fatalf("empty-role instance must not be a replica: %+v", spec.Shards[0].Replicas)
		}
	}
	if len(spec.Shards[0].Replicas) != 1 || spec.Shards[0].Replicas[0].Pod != "p1-r" {
		t.Fatalf("want only the explicit replica p1-r, got %+v", spec.Shards[0].Replicas)
	}
}

func TestCompileFoldsIdentifierCaseForDuplicates(t *testing.T) {
	// PostgreSQL folds unquoted identifiers, so orders and Orders are one table.
	in := CompileInputs{
		Cluster:   cluster(),
		Shards:    []pgshardv1alpha1.PgShardShard{shard("all", "", "", true, "p1", pgshardv1alpha1.ShardRoleData)},
		Endpoints: endpoints("p1"),
		TableConfigs: []pgshardv1alpha1.PgShardTableConfig{
			tableConfig("team-a", "orders"), tableConfig("team-b", "Orders"),
		},
	}
	var dup *DuplicateTableError
	if _, err := Compile(in); !errors.As(err, &dup) {
		t.Fatalf("case-variant table names must be detected as duplicates, got %v", err)
	}
}

func TestCompileIsDeterministicUnderInputPermutation(t *testing.T) {
	// Includes a reshard source [40,80) (hidden) that shares Start=40 with the
	// split target [40,60) — the case a Start-only sort key cannot order.
	shards := []pgshardv1alpha1.PgShardShard{
		shard("c-min-40", "", "40", true, "p1", pgshardv1alpha1.ShardRoleData),
		shard("c-40-60", "40", "60", true, "p4", pgshardv1alpha1.ShardRoleData),
		shard("c-60-80", "60", "80", true, "p5", pgshardv1alpha1.ShardRoleData),
		shard("c-80-max", "80", "", true, "p2", pgshardv1alpha1.ShardRoleData),
		shard("c-40-80", "40", "80", false, "p3", pgshardv1alpha1.ShardRoleData),
		shard("c-system", "", "", true, "ps", pgshardv1alpha1.ShardRoleSystem),
	}
	eps := endpoints("p1", "p2", "p3", "p4", "p5", "ps")
	gates := []pgshardv1alpha1.RoutingGate{{ID: "g2"}, {ID: "g1"}}
	configs := []pgshardv1alpha1.PgShardTableConfig{
		tableConfig("team-b", "items", "orders"), tableConfig("team-a", "customers"),
	}
	inA := CompileInputs{Cluster: cluster(), Shards: shards, Endpoints: eps, Gates: gates, TableConfigs: configs}

	shardsRev := slices.Clone(shards)
	slices.Reverse(shardsRev)
	gatesRev := slices.Clone(gates)
	slices.Reverse(gatesRev)
	configsRev := slices.Clone(configs)
	slices.Reverse(configsRev)
	inB := CompileInputs{Cluster: cluster(), Shards: shardsRev, Endpoints: eps, Gates: gatesRev, TableConfigs: configsRev}

	a, err := Compile(inA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Compile(inB)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("compile output depends on input order:\n%+v\nvs\n%+v", a, b)
	}
}
