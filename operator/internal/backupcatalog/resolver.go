package backupcatalog

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// pgBackRest --type values. "none" is deliberately never emitted: pgBackRest
// documents it as writing no recovery configuration at all — no recovery
// target, no timeline selection, no promote — so what it reaches depends on
// whatever WAL happens to be in the archive, not on the plan.
const (
	targetTypeName = "name"
	targetTypeTime = "time"
	targetTypeLSN  = "lsn"
)

// Target selects the restore point. Exactly one field is set.
type Target struct {
	BarrierID string
	Time      *time.Time
	BackupID  string
	Latest    bool
}

// ShardPlan is one shard's pgBackRest restore invocation.
type ShardPlan struct {
	Shard  string
	Stanza string
	// pgBackRest --set: the backup set to restore from.
	Set string
	// pgBackRest --type: "name" (barrier restore point), "time", or "lsn"
	// (a backup's recorded stop LSN). The executor always adds
	// --target-action=promote: recovery must end at the plan's target, on a
	// new timeline, never idle in recovery waiting for more WAL.
	TargetType string
	// --target value (restore point name, RFC3339 time, or LSN).
	TargetValue string
	// pgBackRest --target-timeline. For name/none targets this is the exact
	// timeline recorded in the manifest. For a time target it is "latest":
	// the requested time can fall after a failover that branched a new
	// timeline, so following the survivor lineage is the only safe choice —
	// pinning the base backup's timeline would recover the abandoned
	// pre-failover branch and silently under-restore. "latest" == highest
	// timeline is the survivor only because fencing stops a deposed primary
	// from advancing an abandoned sibling timeline, and each restore lands on
	// a fresh stanza, so a stanza's timelines form one linear failover chain.
	TargetTimeline string
	// TargetExclusive: the executor must add --target-exclusive. A backup's
	// stop LSN is an END-of-record pointer, and inclusive LSN recovery stops
	// only after applying the first record whose START is >= the target — a
	// commit written between backup-end and the WAL switch would be silently
	// included in "restore this backup". Exclusive recovery stops before it.
	TargetExclusive bool
	// VerifyLSN is the LSN recovery must have reached — EXACTLY — for the
	// restore to be declared complete: the barrier's recorded LSN for name
	// targets, the backup's stop LSN for lsn targets. Recovery-by-name stops
	// at the FIRST matching restore point on the followed timeline, so a
	// duplicate name at an earlier LSN would otherwise silently
	// under-restore; and an over-shot recovery includes records the manifest
	// never promised. The executor compares pg_last_wal_replay_lsn() against
	// this after recovery and treats BOTH < and > as errors. Empty for time
	// targets (no recorded expectation exists for an arbitrary timestamp).
	VerifyLSN string
}

// RestorePlan materializes the topology at the target point.
type RestorePlan struct {
	// The shard layout to provision — the layout live at the target point,
	// NOT the current one. Convergence to a different desired layout is a
	// separate online reshard after the cluster is up.
	Topology TopologySnapshot
	Shards   []ShardPlan
}

type ResolveError struct {
	Reason string
}

func (e *ResolveError) Error() string { return e.Reason }

func errf(format string, args ...any) error {
	return &ResolveError{Reason: fmt.Sprintf(format, args...)}
}

// Resolve maps a target to a full restore plan against the catalog.
func Resolve(catalog Catalog, target Target) (RestorePlan, error) {
	selectors := 0
	if target.BarrierID != "" {
		selectors++
	}
	if target.Time != nil {
		selectors++
	}
	if target.BackupID != "" {
		selectors++
	}
	if target.Latest {
		selectors++
	}
	if selectors != 1 {
		return RestorePlan{}, errf("restore target must set exactly one selector, got %d", selectors)
	}
	if err := validate(catalog); err != nil {
		return RestorePlan{}, err
	}
	switch {
	case target.BarrierID != "":
		return resolveBarrier(catalog, target.BarrierID)
	case target.Time != nil:
		return resolveTime(catalog, *target.Time)
	case target.BackupID != "":
		return resolveBackup(catalog, target.BackupID)
	default: // target.Latest
		return resolveLatest(catalog)
	}
}

func resolveBarrier(catalog Catalog, id string) (RestorePlan, error) {
	barrier, err := findBarrier(catalog, id)
	if err != nil {
		return RestorePlan{}, err
	}
	topology, err := topologyByGeneration(catalog, barrier.TopologyGeneration)
	if err != nil {
		return RestorePlan{}, err
	}
	plan := RestorePlan{Topology: topology}
	for _, shard := range barrier.Shards {
		// Newest backup of this stanza finished before the barrier:
		// pgBackRest auto-selects sets for --type=time but NOT for
		// --type=name, so the resolver picks explicitly.
		backup, ok := newestBackupBefore(catalog, shard.Stanza, barrier.Time)
		if !ok {
			return RestorePlan{}, errf(
				"barrier %s: no backup of stanza %s completed before %s",
				id, shard.Stanza, barrier.Time.Format(time.RFC3339))
		}
		plan.Shards = append(plan.Shards, ShardPlan{
			Shard:          shard.Name,
			Stanza:         shard.Stanza,
			Set:            backup.Label,
			TargetType:     targetTypeName,
			TargetValue:    shard.RestorePoint,
			TargetTimeline: timelineArg(shard.Timeline),
			VerifyLSN:      shard.LSN,
		})
	}
	return plan, sanity(plan)
}

func resolveTime(catalog Catalog, t time.Time) (RestorePlan, error) {
	topology, err := topologyAt(catalog, t)
	if err != nil {
		return RestorePlan{}, err
	}
	plan := RestorePlan{Topology: topology}
	for _, shard := range topology.Shards {
		backup, ok := newestBackupBefore(catalog, shard.Stanza, t)
		if !ok {
			return RestorePlan{}, errf(
				"time %s: no backup of stanza %s completed before target",
				t.Format(time.RFC3339), shard.Stanza)
		}
		plan.Shards = append(plan.Shards, ShardPlan{
			Shard:          shard.Name,
			Stanza:         shard.Stanza,
			Set:            backup.Label,
			TargetType:     targetTypeTime,
			TargetValue:    t.Format(time.RFC3339Nano),
			TargetTimeline: "latest",
		})
	}
	return plan, sanity(plan)
}

func resolveBackup(catalog Catalog, id string) (RestorePlan, error) {
	backup, err := findBackup(catalog, id)
	if err != nil {
		return RestorePlan{}, err
	}
	return planFromBackup(catalog, backup)
}

func resolveLatest(catalog Catalog) (RestorePlan, error) {
	if len(catalog.Backups) == 0 {
		return RestorePlan{}, errf("catalog has no backups")
	}
	newest := catalog.Backups[0]
	for _, backup := range catalog.Backups[1:] {
		if backup.CompletedAt.After(newest.CompletedAt) ||
			(backup.CompletedAt.Equal(newest.CompletedAt) && backup.ID > newest.ID) {
			newest = backup
		}
	}
	return planFromBackup(catalog, newest)
}

func planFromBackup(catalog Catalog, backup BackupManifest) (RestorePlan, error) {
	topology, err := topologyByGeneration(catalog, backup.TopologyGeneration)
	if err != nil {
		return RestorePlan{}, err
	}
	plan := RestorePlan{Topology: topology}
	for _, shard := range backup.Shards {
		// "Restore this backup" means "the cluster exactly as the backup
		// finished": recover to the recorded stop LSN and promote there. A
		// manifest without one cannot be restored to a verifiable point.
		if shard.StopLSN == "" {
			return RestorePlan{}, errf(
				"backup %s: shard %s records no stop LSN; refusing an unverifiable restore",
				backup.ID, shard.Name)
		}
		plan.Shards = append(plan.Shards, ShardPlan{
			Shard:      shard.Name,
			Stanza:     shard.Stanza,
			Set:        shard.Label,
			TargetType: targetTypeLSN,
			// StopLSN is an end-of-record pointer: exclusive recovery, or an
			// unrelated record starting at exactly this LSN would be applied.
			TargetValue:     shard.StopLSN,
			TargetExclusive: true,
			TargetTimeline:  timelineArg(shard.Timeline),
			VerifyLSN:       shard.StopLSN,
		})
	}
	return plan, sanity(plan)
}

// validate enforces the catalog-wide identity invariants every resolution
// path relies on: manifest ids and topology generations are unique, so a
// first-match lookup (and resolveLatest, which never looks up by id) can never
// silently pick a catalog-order-dependent manifest.
func validate(catalog Catalog) error {
	barrierIDs := make(map[string]bool, len(catalog.Barriers))
	for _, b := range catalog.Barriers {
		if barrierIDs[b.ID] {
			return errf("barrier id %s is not unique in the catalog", b.ID)
		}
		barrierIDs[b.ID] = true
	}
	backupIDs := make(map[string]bool, len(catalog.Backups))
	for _, b := range catalog.Backups {
		if backupIDs[b.ID] {
			return errf("backup id %s is not unique in the catalog", b.ID)
		}
		backupIDs[b.ID] = true
	}
	generations := make(map[int64]bool, len(catalog.Topologies))
	for _, s := range catalog.Topologies {
		if generations[s.Generation] {
			return errf("topology generation %d is not unique in the catalog", s.Generation)
		}
		generations[s.Generation] = true
	}
	return nil
}

func findBarrier(catalog Catalog, id string) (BarrierManifest, error) {
	for _, barrier := range catalog.Barriers {
		if barrier.ID == id {
			return barrier, nil
		}
	}
	return BarrierManifest{}, errf("barrier %s not found", id)
}

func findBackup(catalog Catalog, id string) (BackupManifest, error) {
	for _, backup := range catalog.Backups {
		if backup.ID == id {
			return backup, nil
		}
	}
	return BackupManifest{}, errf("backup %s not found", id)
}

func topologyByGeneration(catalog Catalog, generation int64) (TopologySnapshot, error) {
	for _, snapshot := range catalog.Topologies {
		if snapshot.Generation == generation {
			return snapshot, nil
		}
	}
	return TopologySnapshot{}, errf("topology generation %d not in catalog", generation)
}

// topologyAt: the snapshot whose validity interval contains t. Snapshots are
// valid from their commit until the next snapshot's commit, so a target inside
// a cutover window resolves to the PRE-cutover generation. On the (malformed)
// tie of two snapshots sharing a ValidFrom, the higher generation — the later
// structural change — wins, keeping the result independent of catalog order.
func topologyAt(catalog Catalog, t time.Time) (TopologySnapshot, error) {
	var current *TopologySnapshot
	for i := range catalog.Topologies {
		s := &catalog.Topologies[i]
		if s.ValidFrom.After(t) {
			continue
		}
		if current == nil || s.ValidFrom.After(current.ValidFrom) ||
			(s.ValidFrom.Equal(current.ValidFrom) && s.Generation > current.Generation) {
			current = s
		}
	}
	if current == nil {
		return TopologySnapshot{}, errf(
			"target %s predates the first topology snapshot", t.Format(time.RFC3339))
	}
	return *current, nil
}

func newestBackupBefore(catalog Catalog, stanza string, t time.Time) (BackupShard, bool) {
	var best BackupShard
	found := false
	for _, backup := range catalog.Backups {
		for _, shard := range backup.Shards {
			if shard.Stanza != stanza || shard.StopTime.After(t) {
				continue
			}
			if !found || shard.StopTime.After(best.StopTime) {
				best, found = shard, true
			}
		}
	}
	return best, found
}

func timelineArg(tl int32) string {
	return strconv.FormatInt(int64(tl), 10)
}

// snapshotComplete checks the resolved generation is a COMPLETE structural
// routing view. A fresh-cluster restore reconstructs routing from the
// snapshot alone; restoring data without the hash function or shard-key
// types it was written under would misroute rows instead of failing here.
// Only the generation being restored is checked — an unrelated malformed
// snapshot elsewhere in the catalog must not block a good restore.
func snapshotComplete(topology TopologySnapshot) error {
	if topology.HashFunction == "" {
		return errf("topology generation %d records no hash function; a restored cluster could not route its data",
			topology.Generation)
	}
	systemShards := 0
	for _, sh := range topology.Shards {
		switch sh.Role {
		case RoleData:
		case RoleSystem:
			systemShards++
		default:
			return errf("topology generation %d: shard %s has unknown role %q (need data|system)",
				topology.Generation, sh.Name, sh.Role)
		}
		switch sh.State {
		case StateServing, StateBuffered, StateReadOnly, StateDraining, StateHidden:
		default:
			return errf("topology generation %d: shard %s has unknown routing state %q",
				topology.Generation, sh.Name, sh.State)
		}
	}
	// Exactly one system shard: it hosts the sequences and migration records
	// a restored cluster cannot function without, and ranges alone cannot
	// identify it (it has no range, like a full-range data shard).
	if systemShards != 1 {
		return errf("topology generation %d has %d system shards; a restorable snapshot records exactly one",
			topology.Generation, systemShards)
	}
	seen := make(map[string]bool, len(topology.Tables))
	for _, t := range topology.Tables {
		if t.Schema == "" || t.Name == "" {
			return errf("topology generation %d has a table entry without schema or name",
				topology.Generation)
		}
		// PostgreSQL folds unquoted identifiers; the routing compiler
		// deduplicates case-insensitively, so the snapshot must too.
		key := strings.ToLower(t.Schema) + "." + strings.ToLower(t.Name)
		if seen[key] {
			return errf("topology generation %d lists table %s twice", topology.Generation, key)
		}
		seen[key] = true
		switch t.Type {
		case TableSharded:
			if t.ShardKeyColumn == "" || t.ShardKeyType == "" {
				return errf("topology generation %d: sharded table %s records no shard key column/type",
					topology.Generation, key)
			}
		case TableGlobal:
		default:
			return errf("topology generation %d: table %s has unknown type %q",
				topology.Generation, key, t.Type)
		}
	}
	return nil
}

// sanity validates the plan against its topology generation as the source of
// truth: exactly one plan entry per topology shard, each restoring from the
// stanza the topology records for that shard (not a stanza the manifest chose),
// and no empty restore instructions. This is the chokepoint that turns a
// manifest/topology disagreement into an error instead of a silent wrong or
// cross-shard restore.
func sanity(plan RestorePlan) error {
	if err := snapshotComplete(plan.Topology); err != nil {
		return err
	}
	if len(plan.Shards) != len(plan.Topology.Shards) {
		return errf("plan covers %d shards but topology generation %d has %d",
			len(plan.Shards), plan.Topology.Generation, len(plan.Topology.Shards))
	}
	topoStanza := make(map[string]string, len(plan.Topology.Shards))
	stanzaOwner := make(map[string]string, len(plan.Topology.Shards))
	for _, s := range plan.Topology.Shards {
		if s.Stanza == "" {
			return errf("topology generation %d shard %s has no stanza",
				plan.Topology.Generation, s.Name)
		}
		if _, dup := topoStanza[s.Name]; dup {
			return errf("topology generation %d lists shard %s twice",
				plan.Topology.Generation, s.Name)
		}
		// Each shard is a distinct physical stanza; two shards sharing one
		// would restore a single backup as two keyranges.
		if other, dup := stanzaOwner[s.Stanza]; dup {
			return errf("topology generation %d maps shards %s and %s to the same stanza %q",
				plan.Topology.Generation, other, s.Name, s.Stanza)
		}
		topoStanza[s.Name] = s.Stanza
		stanzaOwner[s.Stanza] = s.Name
	}
	planned := map[string]bool{}
	for _, p := range plan.Shards {
		stanza, inTopo := topoStanza[p.Shard]
		if !inTopo {
			return errf("shard %s planned but not in topology generation %d",
				p.Shard, plan.Topology.Generation)
		}
		if planned[p.Shard] {
			return errf("shard %s planned twice", p.Shard)
		}
		planned[p.Shard] = true
		if p.Stanza != stanza {
			return errf("shard %s planned from stanza %q but topology generation %d records %q",
				p.Shard, p.Stanza, plan.Topology.Generation, stanza)
		}
		if p.Set == "" {
			return errf("shard %s has no backup set", p.Shard)
		}
		if (p.TargetType == targetTypeName || p.TargetType == targetTypeLSN) && p.TargetValue == "" {
			return errf("shard %s: %s target with empty target value", p.Shard, p.TargetType)
		}
		// Recovery-by-name stops at the FIRST matching restore point on the
		// followed timeline; without a recorded LSN to compare against, a
		// duplicate name at an earlier LSN silently under-restores.
		if p.TargetType == targetTypeName && p.VerifyLSN == "" {
			return errf("shard %s: restore point %q has no recorded LSN to verify against",
				p.Shard, p.TargetValue)
		}
	}
	for name := range topoStanza {
		if !planned[name] {
			return errf("shard %s in topology generation %d has no restore plan",
				name, plan.Topology.Generation)
		}
	}
	return nil
}
