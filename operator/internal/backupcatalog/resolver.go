package backupcatalog

import (
	"fmt"
	"strconv"
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
	// pgBackRest --target-timeline. For name/none targets this is the exact
	// timeline recorded in the manifest. For a time target it is "latest":
	// the requested time can fall after a failover that branched a new
	// timeline, so following the survivor lineage is the only safe choice —
	// pinning the base backup's timeline would recover the abandoned
	// pre-failover branch and silently under-restore.
	TargetTimeline string
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
			TargetType:     "name",
			TargetValue:    shard.RestorePoint,
			TargetTimeline: timelineArg(shard.Timeline),
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
		plan.Shards = append(plan.Shards, ShardPlan{
			Shard:          shard.Name,
			Stanza:         shard.Stanza,
			Set:            shard.Label,
			TargetType:     "none",
			TargetTimeline: timelineArg(shard.Timeline),
		})
	}
	return plan, sanity(plan)
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

// sanity validates the plan against its topology generation as the source of
// truth: exactly one plan entry per topology shard, each restoring from the
// stanza the topology records for that shard (not a stanza the manifest chose),
// and no empty restore instructions. This is the chokepoint that turns a
// manifest/topology disagreement into an error instead of a silent wrong or
// cross-shard restore.
func sanity(plan RestorePlan) error {
	if len(plan.Shards) != len(plan.Topology.Shards) {
		return errf("plan covers %d shards but topology generation %d has %d",
			len(plan.Shards), plan.Topology.Generation, len(plan.Topology.Shards))
	}
	topoStanza := make(map[string]string, len(plan.Topology.Shards))
	for _, s := range plan.Topology.Shards {
		if _, dup := topoStanza[s.Name]; dup {
			return errf("topology generation %d lists shard %s twice",
				plan.Topology.Generation, s.Name)
		}
		topoStanza[s.Name] = s.Stanza
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
		if p.Stanza == "" {
			return errf("shard %s has no stanza", p.Shard)
		}
		if p.Set == "" {
			return errf("shard %s has no backup set", p.Shard)
		}
		if p.TargetType == "name" && p.TargetValue == "" {
			return errf("shard %s: name target with empty restore point", p.Shard)
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
