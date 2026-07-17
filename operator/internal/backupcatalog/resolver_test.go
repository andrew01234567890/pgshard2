package backupcatalog

import (
	"errors"
	"testing"
	"time"
)

// Fixture timeline (the topology-drift story the resolver exists for):
//
//	t0  gen1 topology: 2 shards (A: -80, B: 80-)
//	t1  full backups of A and B
//	t2  barrier b1
//	t3  reshard commit -> gen2 topology: 4 shards (C,D,E,F); A,B decommissioned
//	t4  full backups of C..F
//	t5  barrier b2
const (
	stanzaA = "c-A-g1"
	stanzaB = "c-B-g1"
	stanzaC = "c-C-g1"
	stanzaD = "c-D-g1"
	stanzaE = "c-E-g1"
	stanzaF = "c-F-g1"
	point1  = "pgshard_b1"
	point2  = "pgshard_b2"
)

var (
	t0 = time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
	t1 = t0.Add(30 * time.Minute)
	t2 = t0.Add(60 * time.Minute)
	t3 = t0.Add(90 * time.Minute)
	t4 = t0.Add(120 * time.Minute)
	t5 = t0.Add(150 * time.Minute)
)

func driftCatalog() Catalog {
	return Catalog{
		Topologies: []TopologySnapshot{
			{Generation: 1, ValidFrom: t0, Epoch: 1, Shards: []ShardTopology{
				{Name: "A", KeyRange: KeyRangeRef{End: "80"}, Stanza: stanzaA},
				{Name: "B", KeyRange: KeyRangeRef{Start: "80"}, Stanza: stanzaB},
			}},
			{Generation: 2, ValidFrom: t3, Epoch: 10, Shards: []ShardTopology{
				{Name: "C", KeyRange: KeyRangeRef{End: "40"}, Stanza: stanzaC},
				{Name: "D", KeyRange: KeyRangeRef{Start: "40", End: "80"}, Stanza: stanzaD},
				{Name: "E", KeyRange: KeyRangeRef{Start: "80", End: "c0"}, Stanza: stanzaE},
				{Name: "F", KeyRange: KeyRangeRef{Start: "c0"}, Stanza: stanzaF},
			}},
		},
		Barriers: []BarrierManifest{
			{ID: "b1", Time: t2, TopologyGeneration: 1, Shards: []BarrierShard{
				{Name: "A", Stanza: stanzaA, LSN: "0/5000000", Timeline: 1, RestorePoint: point1},
				{Name: "B", Stanza: stanzaB, LSN: "0/6000000", Timeline: 2, RestorePoint: point1},
			}},
			{ID: "b2", Time: t5, TopologyGeneration: 2, Shards: []BarrierShard{
				{Name: "C", Stanza: stanzaC, Timeline: 1, RestorePoint: point2},
				{Name: "D", Stanza: stanzaD, Timeline: 1, RestorePoint: point2},
				{Name: "E", Stanza: stanzaE, Timeline: 1, RestorePoint: point2},
				{Name: "F", Stanza: stanzaF, Timeline: 1, RestorePoint: point2},
			}},
		},
		Backups: []BackupManifest{
			{ID: "bk1", CompletedAt: t1, TopologyGeneration: 1, Shards: []BackupShard{
				{Name: "A", Stanza: stanzaA, Label: "20260716-1F", Timeline: 1, StopTime: t1},
				{Name: "B", Stanza: stanzaB, Label: "20260716-2F", Timeline: 2, StopTime: t1},
			}},
			{ID: "bk2", CompletedAt: t4, TopologyGeneration: 2, Shards: []BackupShard{
				{Name: "C", Stanza: stanzaC, Label: "20260716-3F", Timeline: 1, StopTime: t4},
				{Name: "D", Stanza: stanzaD, Label: "20260716-4F", Timeline: 1, StopTime: t4},
				{Name: "E", Stanza: stanzaE, Label: "20260716-5F", Timeline: 1, StopTime: t4},
				{Name: "F", Stanza: stanzaF, Label: "20260716-6F", Timeline: 1, StopTime: t4},
			}},
		},
	}
}

func TestBarrierBeforeReshardRestoresOldTopology(t *testing.T) {
	plan, err := Resolve(driftCatalog(), Target{BarrierID: "b1"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Topology.Generation != 1 || len(plan.Shards) != 2 {
		t.Fatalf("must restore the 2-shard gen1 layout, got gen%d/%d shards",
			plan.Topology.Generation, len(plan.Shards))
	}
	a := plan.Shards[0]
	if a.Set != "20260716-1F" || a.TargetType != "name" ||
		a.TargetValue != point1 || a.TargetTimeline != "1" {
		t.Fatalf("shard A plan wrong: %+v", a)
	}
	if plan.Shards[1].TargetTimeline != "2" {
		t.Fatalf("shard B must carry its recorded timeline: %+v", plan.Shards[1])
	}
}

func TestTimeInsideCutoverWindowRoundsToPreCutoverGeneration(t *testing.T) {
	target := t3.Add(-time.Second)
	plan, err := Resolve(driftCatalog(), Target{Time: &target})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Topology.Generation != 1 {
		t.Fatalf("t just before the reshard commit must resolve to gen1, got gen%d",
			plan.Topology.Generation)
	}
	if plan.Shards[0].TargetType != "time" {
		t.Fatalf("expected time target: %+v", plan.Shards[0])
	}
}

func TestTimeAfterReshardUsesNewGenerationAndItsBackups(t *testing.T) {
	target := t4.Add(time.Minute)
	plan, err := Resolve(driftCatalog(), Target{Time: &target})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Topology.Generation != 2 || len(plan.Shards) != 4 {
		t.Fatalf("expected gen2 4-shard plan, got %+v", plan.Topology)
	}
}

func TestTimeAfterReshardButBeforeNewBackupsFails(t *testing.T) {
	// gen2 is live but its shards have no completed backups yet: the target
	// is honest-unrestorable (reshard targets get baseline backups before
	// cutover in the real workflow precisely to close this hole).
	target := t3.Add(time.Minute)
	_, err := Resolve(driftCatalog(), Target{Time: &target})
	var resolveErr *ResolveError
	if !errors.As(err, &resolveErr) {
		t.Fatalf("expected resolve error, got %v", err)
	}
}

func TestDecommissionedShardsRemainRestorable(t *testing.T) {
	// After the reshard, A and B are gone from the live cluster — but any
	// target before the cutover must still resolve against their stanzas.
	target := t2.Add(time.Minute)
	plan, err := Resolve(driftCatalog(), Target{Time: &target})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Shards[0].Stanza != stanzaA || plan.Shards[1].Stanza != stanzaB {
		t.Fatalf("decommissioned stanzas must be used: %+v", plan.Shards)
	}
}

func TestBackupAndLatestTargets(t *testing.T) {
	plan, err := Resolve(driftCatalog(), Target{BackupID: "bk1"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Topology.Generation != 1 || plan.Shards[0].TargetType != "none" {
		t.Fatalf("backup target plan wrong: %+v", plan)
	}
	latest, err := Resolve(driftCatalog(), Target{Latest: true})
	if err != nil {
		t.Fatal(err)
	}
	if latest.Topology.Generation != 2 {
		t.Fatalf("latest must pick bk2/gen2: %+v", latest.Topology)
	}
}

func TestErrorsAreExplicit(t *testing.T) {
	catalog := driftCatalog()
	cases := []Target{
		{},
		{BarrierID: "nope"},
		{BackupID: "nope"},
	}
	for _, target := range cases {
		if _, err := Resolve(catalog, target); err == nil {
			t.Fatalf("target %+v must fail", target)
		}
	}
	early := t0.Add(-time.Hour)
	if _, err := Resolve(catalog, Target{Time: &early}); err == nil {
		t.Fatal("target before first snapshot must fail")
	}
}

func TestTimeTargetFollowsLatestTimeline(t *testing.T) {
	// A time target can fall after an unrecorded failover, so it must follow
	// the survivor lineage rather than pinning the base backup's timeline.
	target := t2.Add(time.Minute)
	plan, err := Resolve(driftCatalog(), Target{Time: &target})
	if err != nil {
		t.Fatal(err)
	}
	for _, shard := range plan.Shards {
		if shard.TargetTimeline != "latest" {
			t.Fatalf("time target must follow latest timeline: %+v", shard)
		}
	}
}

func TestSwappedStanzaIsRejected(t *testing.T) {
	catalog := driftCatalog()
	// b1 names the right shards (A, B) but points each at the OTHER's stanza:
	// names/counts still cover gen1, so only a topology-authoritative stanza
	// check catches the silent cross-shard swap.
	catalog.Barriers[0].Shards[0].Stanza = stanzaB
	catalog.Barriers[0].Shards[1].Stanza = stanzaA
	if _, err := Resolve(catalog, Target{BarrierID: "b1"}); err == nil {
		t.Fatal("barrier with swapped stanzas must be rejected")
	}
}

func TestEmptyBackupSetIsRejected(t *testing.T) {
	catalog := driftCatalog()
	catalog.Backups[0].Shards[0].Label = ""
	if _, err := Resolve(catalog, Target{BackupID: "bk1"}); err == nil {
		t.Fatal("backup shard with an empty set label must be rejected")
	}
}

func TestExactlyOneSelectorRequired(t *testing.T) {
	catalog := driftCatalog()
	if _, err := Resolve(catalog, Target{BarrierID: "b1", Latest: true}); err == nil {
		t.Fatal("target with two selectors must be rejected")
	}
}
