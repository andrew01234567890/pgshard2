package backupcatalog

import (
	"fmt"
	"slices"
	"time"
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
	// pgBackRest --type: "name" (barrier restore point), "time", or
	// "none" (recover to the end of the backup's WAL).
	TargetType string
	// --target value (restore point name or RFC3339 time); empty for none.
	TargetValue string
	// --target-timeline recorded at backup/barrier time; disambiguates
	// divergent timelines from earlier restores or failovers.
	TargetTimeline int32
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
	switch {
	case target.BarrierID != "":
		return resolveBarrier(catalog, target.BarrierID)
	case target.Time != nil:
		return resolveTime(catalog, *target.Time)
	case target.BackupID != "":
		return resolveBackup(catalog, target.BackupID)
	case target.Latest:
		return resolveLatest(catalog)
	default:
		return RestorePlan{}, errf("empty restore target")
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
			TargetType:     "name",
			TargetValue:    shard.RestorePoint,
			TargetTimeline: shard.Timeline,
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
			TargetType:     "time",
			TargetValue:    t.Format(time.RFC3339Nano),
			TargetTimeline: backup.Timeline,
		})
	}
	return plan, sanity(plan)
}

func resolveBackup(catalog Catalog, id string) (RestorePlan, error) {
	for _, backup := range catalog.Backups {
		if backup.ID != id {
			continue
		}
		topology, err := topologyByGeneration(catalog, backup.TopologyGeneration)
		if err != nil {
			return RestorePlan{}, err
		}
		plan := RestorePlan{Topology: topology}
		for _, shard := range backup.Shards {
			plan.Shards = append(plan.Shards, ShardPlan{
				Shard:          shard.Name,
				Stanza:         shard.Stanza,
				Set:            shard.Label,
				TargetType:     "none",
				TargetTimeline: shard.Timeline,
			})
		}
		return plan, sanity(plan)
	}
	return RestorePlan{}, errf("backup %s not found", id)
}

func resolveLatest(catalog Catalog) (RestorePlan, error) {
	if len(catalog.Backups) == 0 {
		return RestorePlan{}, errf("catalog has no backups")
	}
	newest := catalog.Backups[0]
	for _, backup := range catalog.Backups[1:] {
		if backup.CompletedAt.After(newest.CompletedAt) {
			newest = backup
		}
	}
	return resolveBackup(catalog, newest.ID)
}

func findBarrier(catalog Catalog, id string) (BarrierManifest, error) {
	for _, barrier := range catalog.Barriers {
		if barrier.ID == id {
			return barrier, nil
		}
	}
	return BarrierManifest{}, errf("barrier %s not found", id)
}

func topologyByGeneration(catalog Catalog, generation int64) (TopologySnapshot, error) {
	for _, snapshot := range catalog.Topologies {
		if snapshot.Generation == generation {
			return snapshot, nil
		}
	}
	return TopologySnapshot{}, errf("topology generation %d not in catalog", generation)
}

// topologyAt: the snapshot whose validity interval contains t. Snapshots
// are valid from their commit until the next snapshot's commit, so a target
// inside a cutover window resolves to the PRE-cutover generation.
func topologyAt(catalog Catalog, t time.Time) (TopologySnapshot, error) {
	snapshots := append([]TopologySnapshot(nil), catalog.Topologies...)
	slices.SortFunc(snapshots, func(a, b TopologySnapshot) int {
		return a.ValidFrom.Compare(b.ValidFrom)
	})
	var current *TopologySnapshot
	for i := range snapshots {
		if !snapshots[i].ValidFrom.After(t) {
			current = &snapshots[i]
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
	var bestTime time.Time
	found := false
	for _, backup := range catalog.Backups {
		for _, shard := range backup.Shards {
			if shard.Stanza != stanza || shard.StopTime.After(t) {
				continue
			}
			if !found || shard.StopTime.After(bestTime) {
				best, bestTime, found = shard, shard.StopTime, true
			}
		}
	}
	return best, found
}

// sanity: every topology shard must have exactly one plan entry.
func sanity(plan RestorePlan) error {
	if len(plan.Shards) != len(plan.Topology.Shards) {
		return errf("plan covers %d shards but topology generation %d has %d",
			len(plan.Shards), plan.Topology.Generation, len(plan.Topology.Shards))
	}
	planned := map[string]bool{}
	for _, p := range plan.Shards {
		if planned[p.Shard] {
			return errf("shard %s planned twice", p.Shard)
		}
		planned[p.Shard] = true
	}
	for _, s := range plan.Topology.Shards {
		if !planned[s.Name] {
			return errf("shard %s in topology generation %d has no restore plan",
				s.Name, plan.Topology.Generation)
		}
	}
	return nil
}
