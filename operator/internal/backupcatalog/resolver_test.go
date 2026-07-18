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
	backup1 = "bk1"
	stopA   = "0/3000000"
	stanzaS = "c-sys-g1"
	pointS  = "0/1000000"
)

var (
	t0 = time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
	t1 = t0.Add(30 * time.Minute)
	t2 = t0.Add(60 * time.Minute)
	t3 = t0.Add(90 * time.Minute)
	t4 = t0.Add(120 * time.Minute)
	t5 = t0.Add(150 * time.Minute)
)

func driftTables() []TableTopology {
	return []TableTopology{
		{Schema: "public", Name: "orders", Type: TableSharded,
			ShardKeyColumn: "customer_id", ShardKeyType: "int8",
			Sequences: []SequenceTopology{{Column: "id", Sequence: "orders_id"}}},
		{Schema: "public", Name: "currencies", Type: TableGlobal},
	}
}

func driftCatalog() Catalog {
	return Catalog{
		Topologies: []TopologySnapshot{
			{Generation: 1, ValidFrom: t0, Epoch: 1, HashFunction: "xxhash64_v1",
				Tables: driftTables(), Shards: []ShardTopology{
					{Name: "A", KeyRange: KeyRangeRef{End: "80"}, Stanza: stanzaA, Role: RoleData, State: StateServing},
					{Name: "B", KeyRange: KeyRangeRef{Start: "80"}, Stanza: stanzaB, Role: RoleData, State: StateServing},
					{Name: "sys", Stanza: stanzaS, Role: RoleSystem, State: StateServing},
				}},
			{Generation: 2, ValidFrom: t3, Epoch: 10, HashFunction: "xxhash64_v1",
				Tables: driftTables(), Shards: []ShardTopology{
					{Name: "C", KeyRange: KeyRangeRef{End: "40"}, Stanza: stanzaC, Role: RoleData, State: StateServing},
					{Name: "D", KeyRange: KeyRangeRef{Start: "40", End: "80"}, Stanza: stanzaD, Role: RoleData, State: StateServing},
					{Name: "E", KeyRange: KeyRangeRef{Start: "80", End: "c0"}, Stanza: stanzaE, Role: RoleData, State: StateServing},
					{Name: "F", KeyRange: KeyRangeRef{Start: "c0"}, Stanza: stanzaF, Role: RoleData, State: StateServing},
					{Name: "sys", Stanza: stanzaS, Role: RoleSystem, State: StateServing},
				}},
		},
		Barriers: []BarrierManifest{
			{ID: "b1", Time: t2, TopologyGeneration: 1, Shards: []BarrierShard{
				{Name: "A", Stanza: stanzaA, LSN: "0/5000000", Timeline: 1, RestorePoint: point1},
				{Name: "B", Stanza: stanzaB, LSN: "0/6000000", Timeline: 2, RestorePoint: point1},
				{Name: "sys", Stanza: stanzaS, LSN: pointS, Timeline: 1, RestorePoint: point1},
			}},
			{ID: "b2", Time: t5, TopologyGeneration: 2, Shards: []BarrierShard{
				{Name: "C", Stanza: stanzaC, LSN: "0/8000000", Timeline: 1, RestorePoint: point2},
				{Name: "D", Stanza: stanzaD, LSN: "0/8000010", Timeline: 1, RestorePoint: point2},
				{Name: "E", Stanza: stanzaE, LSN: "0/8000020", Timeline: 1, RestorePoint: point2},
				{Name: "F", Stanza: stanzaF, LSN: "0/8000030", Timeline: 1, RestorePoint: point2},
				{Name: "sys", Stanza: stanzaS, LSN: pointS, Timeline: 1, RestorePoint: point2},
			}},
		},
		Backups: []BackupManifest{
			{ID: backup1, CompletedAt: t1, TopologyGeneration: 1, Shards: []BackupShard{
				{Name: "A", Stanza: stanzaA, Label: "20260716-1F", StopLSN: stopA, Timeline: 1, StopTime: t1},
				{Name: "B", Stanza: stanzaB, Label: "20260716-2F", StopLSN: "0/4000000", Timeline: 2, StopTime: t1},
				{Name: "sys", Stanza: stanzaS, Label: "20260716-SF", StopLSN: pointS, Timeline: 1, StopTime: t1},
			}},
			{ID: "bk2", CompletedAt: t4, TopologyGeneration: 2, Shards: []BackupShard{
				{Name: "C", Stanza: stanzaC, Label: "20260716-3F", StopLSN: "0/7000000", Timeline: 1, StopTime: t4},
				{Name: "D", Stanza: stanzaD, Label: "20260716-4F", StopLSN: "0/7000010", Timeline: 1, StopTime: t4},
				{Name: "E", Stanza: stanzaE, Label: "20260716-5F", StopLSN: "0/7000020", Timeline: 1, StopTime: t4},
				{Name: "F", Stanza: stanzaF, Label: "20260716-6F", StopLSN: "0/7000030", Timeline: 1, StopTime: t4},
				{Name: "sys", Stanza: stanzaS, Label: "20260716-S2", StopLSN: pointS, Timeline: 1, StopTime: t4},
			}},
		},
	}
}

func TestBarrierBeforeReshardRestoresOldTopology(t *testing.T) {
	plan, err := Resolve(driftCatalog(), Target{BarrierID: "b1"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Topology.Generation != 1 || len(plan.Shards) != 3 {
		t.Fatalf("must restore the gen1 layout incl. the system shard, got gen%d/%d shards",
			plan.Topology.Generation, len(plan.Shards))
	}
	a := plan.Shards[0]
	if a.Set != "20260716-1F" || a.TargetType != targetTypeName ||
		a.TargetValue != point1 || a.TargetTimeline != "1" {
		t.Fatalf("shard A plan wrong: %+v", a)
	}
	if a.VerifyLSN != "0/5000000" {
		t.Fatalf("a name target must carry the barrier's recorded LSN to verify against "+
			"(recovery-by-name stops at the FIRST matching point): %+v", a)
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
	if plan.Shards[0].TargetType != targetTypeTime {
		t.Fatalf("expected time target: %+v", plan.Shards[0])
	}
}

func TestTimeAfterReshardUsesNewGenerationAndItsBackups(t *testing.T) {
	target := t4.Add(time.Minute)
	plan, err := Resolve(driftCatalog(), Target{Time: &target})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Topology.Generation != 2 || len(plan.Shards) != 5 {
		t.Fatalf("expected the 5-shard gen2 plan (incl. system), got %+v", plan.Topology)
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
	plan, err := Resolve(driftCatalog(), Target{BackupID: backup1})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Topology.Generation != 1 {
		t.Fatalf("backup target plan wrong: %+v", plan)
	}
	// "Restore this backup" recovers to its recorded stop LSN and promotes
	// there — never --type=none, which writes no recovery configuration and
	// reaches whatever WAL the archive happens to hold.
	a := plan.Shards[0]
	if a.TargetType != targetTypeLSN || a.TargetValue != stopA ||
		a.VerifyLSN != stopA || a.TargetTimeline != "1" {
		t.Fatalf("backup restore must target the recorded stop LSN: %+v", a)
	}
	if !a.TargetExclusive {
		t.Fatal("stop LSN is an end-of-record pointer: inclusive recovery would apply one extra record")
	}
	latest, err := Resolve(driftCatalog(), Target{Latest: true})
	if err != nil {
		t.Fatal(err)
	}
	if latest.Topology.Generation != 2 {
		t.Fatalf("latest must pick bk2/gen2: %+v", latest.Topology)
	}
}

func TestBackupWithoutStopLSNIsRejected(t *testing.T) {
	catalog := driftCatalog()
	catalog.Backups[0].Shards[1].StopLSN = ""
	if _, err := Resolve(catalog, Target{BackupID: backup1}); err == nil {
		t.Fatal("a backup shard without a recorded stop LSN cannot be restored to a verifiable point")
	}
}

func TestBarrierWithoutRecordedLSNIsRejected(t *testing.T) {
	catalog := driftCatalog()
	catalog.Barriers[0].Shards[0].LSN = ""
	if _, err := Resolve(catalog, Target{BarrierID: "b1"}); err == nil {
		t.Fatal("a barrier restore point without a recorded LSN cannot be verified after recovery")
	}
}

func TestIncompleteTopologySnapshotIsRejected(t *testing.T) {
	noHash := driftCatalog()
	noHash.Topologies[0].HashFunction = ""
	if _, err := Resolve(noHash, Target{BackupID: backup1}); err == nil {
		t.Fatal("a snapshot without the hash function cannot reconstruct routing")
	}

	noKeyType := driftCatalog()
	noKeyType.Topologies[0].Tables[0].ShardKeyType = ""
	if _, err := Resolve(noKeyType, Target{BackupID: backup1}); err == nil {
		t.Fatal("a sharded table without its shard-key type would hash literals wrongly after restore")
	}

	badType := driftCatalog()
	badType.Topologies[0].Tables[1].Type = "reference"
	if _, err := Resolve(badType, Target{BackupID: backup1}); err == nil {
		t.Fatal("an unknown table type must be rejected, not restored under guessed rules")
	}

	dup := driftCatalog()
	dup.Topologies[0].Tables = append(dup.Topologies[0].Tables, dup.Topologies[0].Tables[0])
	if _, err := Resolve(dup, Target{BackupID: backup1}); err == nil {
		t.Fatal("a duplicated table entry is ambiguous and must be rejected")
	}

	noSystem := driftCatalog()
	noSystem.Topologies[0].Shards = noSystem.Topologies[0].Shards[:2] // drop sys
	noSystem.Backups[0].Shards = noSystem.Backups[0].Shards[:2]
	if _, err := Resolve(noSystem, Target{BackupID: backup1}); err == nil {
		t.Fatal("a snapshot without its system shard cannot restore sequences/migrations")
	}

	twoSystems := driftCatalog()
	twoSystems.Topologies[0].Shards = append(twoSystems.Topologies[0].Shards, ShardTopology{
		Name: "sys2", Stanza: "c-sys2-g1", Role: RoleSystem, State: StateServing,
	})
	if _, err := Resolve(twoSystems, Target{BackupID: backup1}); err == nil {
		t.Fatal("two system shards are ambiguous; a restorable snapshot records exactly one")
	}

	noRole := driftCatalog()
	noRole.Topologies[0].Shards[0].Role = ""
	if _, err := Resolve(noRole, Target{BackupID: backup1}); err == nil {
		t.Fatal("a shard without a role cannot identify the sequence host after restore")
	}

	noState := driftCatalog()
	noState.Topologies[0].Shards[0].State = ""
	if _, err := Resolve(noState, Target{BackupID: backup1}); err == nil {
		t.Fatal("a shard without a routing state is ambiguous mid-reshard (overlapping ranges)")
	}

	caseDup := driftCatalog()
	caseDup.Topologies[0].Tables = append(caseDup.Topologies[0].Tables, TableTopology{
		Schema: "PUBLIC", Name: "Orders", Type: TableSharded,
		ShardKeyColumn: "customer_id", ShardKeyType: "int8",
	})
	if _, err := Resolve(caseDup, Target{BackupID: backup1}); err == nil {
		t.Fatal("PostgreSQL folds unquoted identifiers: a case-variant duplicate table is the same table")
	}

	// The check is scoped to the generation being restored: gen2 corruption
	// must not block a gen1 restore.
	otherGen := driftCatalog()
	otherGen.Topologies[1].HashFunction = ""
	if _, err := Resolve(otherGen, Target{BackupID: backup1}); err != nil {
		t.Fatalf("an unrelated malformed generation must not block this restore: %v", err)
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
	if _, err := Resolve(catalog, Target{BackupID: backup1}); err == nil {
		t.Fatal("backup shard with an empty set label must be rejected")
	}
}

func TestExactlyOneSelectorRequired(t *testing.T) {
	catalog := driftCatalog()
	if _, err := Resolve(catalog, Target{BarrierID: "b1", Latest: true}); err == nil {
		t.Fatal("target with two selectors must be rejected")
	}
}

func TestSharedTopologyStanzaIsRejected(t *testing.T) {
	catalog := driftCatalog()
	// Two distinct shards of gen1 pointed at one stanza: restoring one physical
	// backup as two keyranges. Keep plan<->topology stanzas consistent so only
	// the topology's own duplicate-stanza check can catch it.
	catalog.Topologies[0].Shards[1].Stanza = stanzaA
	catalog.Backups[0].Shards[1].Stanza = stanzaA
	if _, err := Resolve(catalog, Target{BackupID: backup1}); err == nil {
		t.Fatal("topology mapping two shards to one stanza must be rejected")
	}
}

func TestDuplicateManifestIDIsRejected(t *testing.T) {
	catalog := driftCatalog()
	catalog.Backups = append(catalog.Backups, catalog.Backups[0])
	// Rejected for an explicit id AND for Latest, which never looks up by id.
	if _, err := Resolve(catalog, Target{BackupID: backup1}); err == nil {
		t.Fatal("two backup manifests sharing an id must be rejected as ambiguous")
	}
	if _, err := Resolve(catalog, Target{Latest: true}); err == nil {
		t.Fatal("latest must also reject a catalog with duplicate backup ids")
	}
}
