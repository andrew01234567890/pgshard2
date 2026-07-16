// Package routing compiles the cluster's scattered state (shards, table
// configs, workflow gates) into the single atomically-versioned
// PgShardRouting object that routers and agents watch. Only the
// leader-elected operator writes it; every change is one write with a
// strictly monotonic epoch, so consumers can order updates with one rule:
// apply iff epoch > last applied.
package routing

import (
	"context"
	"fmt"
	"slices"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard2/operator/internal/topology"
)

// CompileInputs gathers everything the compiled view derives from.
type CompileInputs struct {
	Cluster      *pgshardv1alpha1.PgShardCluster
	Shards       []pgshardv1alpha1.PgShardShard
	TableConfigs []pgshardv1alpha1.PgShardTableConfig
	// Primary/replica endpoints resolved from pod IPs, keyed by pod name.
	Endpoints map[string]pgshardv1alpha1.RoutingEndpoint
	// Buffering gates requested by in-flight workflows.
	Gates []pgshardv1alpha1.RoutingGate
}

// A DuplicateTableError names the offending config so the controller can
// surface it on that object's status instead of failing the whole compile
// anonymously.
type DuplicateTableError struct {
	Table        string
	FirstConfig  string
	SecondConfig string
}

func (e *DuplicateTableError) Error() string {
	return fmt.Sprintf("table %s declared in both %s and %s",
		e.Table, e.FirstConfig, e.SecondConfig)
}

// Compile produces the desired spec WITHOUT epoch/topologyGeneration — the
// writer assigns those. Pure and deterministic (sorted output).
func Compile(in CompileInputs) (pgshardv1alpha1.PgShardRoutingSpec, error) {
	spec := pgshardv1alpha1.PgShardRoutingSpec{
		WriteLeaseSeconds: in.Cluster.Spec.Router.WriteLeaseSeconds,
		HashFunction:      in.Cluster.Spec.Postgres.HashFunction,
		Gates:             in.Gates,
	}

	shards := slices.Clone(in.Shards)
	slices.SortFunc(shards, func(a, b pgshardv1alpha1.PgShardShard) int {
		switch {
		case a.Spec.KeyRange.Start < b.Spec.KeyRange.Start:
			return -1
		case a.Spec.KeyRange.Start > b.Spec.KeyRange.Start:
			return 1
		default:
			return 0
		}
	})

	var servingRanges []topology.KeyRange
	for _, shard := range shards {
		if shard.Spec.Role == pgshardv1alpha1.ShardRoleSystem {
			// The system shard is not part of the sharded keyspace; it is
			// published as the sequence endpoint instead.
			if ep, ok := in.Endpoints[shard.Status.CurrentPrimary]; ok {
				spec.SequenceEndpoint = &ep
			}
			continue
		}
		kr, err := topology.ParseKeyRange(
			shard.Spec.KeyRange.Start + "-" + shard.Spec.KeyRange.End)
		if err != nil {
			return spec, fmt.Errorf("shard %s: %w", shard.Name, err)
		}
		state := pgshardv1alpha1.RoutingHidden
		if shard.Spec.Serving {
			state = pgshardv1alpha1.RoutingServing
			servingRanges = append(servingRanges, kr)
		}
		entry := pgshardv1alpha1.RoutingShard{
			Name:     shard.Name,
			KeyRange: shard.Spec.KeyRange,
			State:    state,
		}
		if ep, ok := in.Endpoints[shard.Status.CurrentPrimary]; ok {
			entry.Primary = &ep
		}
		for _, inst := range shard.Status.Instances {
			if inst.Pod == shard.Status.CurrentPrimary {
				continue
			}
			if ep, ok := in.Endpoints[inst.Pod]; ok {
				ep.CanRead = inst.Ready
				entry.Replicas = append(entry.Replicas, ep)
			}
		}
		spec.Shards = append(spec.Shards, entry)
	}
	if err := topology.ValidatePartition(servingRanges); err != nil {
		return spec, fmt.Errorf("serving shards do not partition the keyspace: %w", err)
	}

	configs := slices.Clone(in.TableConfigs)
	slices.SortFunc(configs, func(a, b pgshardv1alpha1.PgShardTableConfig) int {
		switch {
		case a.Name < b.Name:
			return -1
		case a.Name > b.Name:
			return 1
		default:
			return 0
		}
	})
	owner := map[string]string{}
	for _, tc := range configs {
		for _, t := range tc.Spec.Tables {
			schema := t.Schema
			if schema == "" {
				schema = "public"
			}
			key := schema + "." + t.Name
			if first, dup := owner[key]; dup {
				return spec, &DuplicateTableError{
					Table: key, FirstConfig: first, SecondConfig: tc.Name,
				}
			}
			owner[key] = tc.Name
			spec.Tables = append(spec.Tables, pgshardv1alpha1.RoutingTable{
				Schema:         schema,
				Name:           t.Name,
				Type:           pgshardv1alpha1.RoutingTableType(t.Type),
				ShardKeyColumn: t.ShardKeyColumn,
				Sequences:      t.Sequences,
			})
		}
	}
	return spec, nil
}

// Write persists the compiled spec with a strictly monotonic epoch bump,
// retrying on conflict. topologyGeneration bumps only on structural change
// (shard set or table catalog). No-op when nothing changed.
func Write(
	ctx context.Context,
	c client.Client,
	key types.NamespacedName,
	desired pgshardv1alpha1.PgShardRoutingSpec,
) (epoch int64, changed bool, err error) {
	for {
		var current pgshardv1alpha1.PgShardRouting
		getErr := c.Get(ctx, key, &current)
		switch {
		case apierrors.IsNotFound(getErr):
			desired.Epoch = 1
			desired.TopologyGeneration = 1
			fresh := pgshardv1alpha1.PgShardRouting{Spec: desired}
			fresh.Name = key.Name
			fresh.Namespace = key.Namespace
			createErr := c.Create(ctx, &fresh)
			if apierrors.IsAlreadyExists(createErr) {
				continue
			}
			return 1, true, createErr
		case getErr != nil:
			return 0, false, getErr
		}

		if specEquivalent(current.Spec, desired) {
			return current.Spec.Epoch, false, nil
		}
		desired.Epoch = current.Spec.Epoch + 1
		desired.TopologyGeneration = current.Spec.TopologyGeneration
		if structuralChange(current.Spec, desired) {
			desired.TopologyGeneration++
		}
		current.Spec = desired
		updateErr := c.Update(ctx, &current)
		if apierrors.IsConflict(updateErr) {
			continue
		}
		return desired.Epoch, updateErr == nil, updateErr
	}
}

// specEquivalent ignores the writer-assigned fields.
func specEquivalent(a, b pgshardv1alpha1.PgShardRoutingSpec) bool {
	a.Epoch, b.Epoch = 0, 0
	a.TopologyGeneration, b.TopologyGeneration = 0, 0
	return apiequality.Semantic.DeepEqual(a, b)
}

// structuralChange: the parts a restore pins and resharding changes —
// shard set (names, ranges, states) and the table catalog.
func structuralChange(old, updated pgshardv1alpha1.PgShardRoutingSpec) bool {
	if len(old.Shards) != len(updated.Shards) || len(old.Tables) != len(updated.Tables) {
		return true
	}
	for i := range old.Shards {
		if old.Shards[i].Name != updated.Shards[i].Name ||
			old.Shards[i].KeyRange != updated.Shards[i].KeyRange ||
			old.Shards[i].State != updated.Shards[i].State {
			return true
		}
	}
	for i := range old.Tables {
		if !apiequality.Semantic.DeepEqual(old.Tables[i], updated.Tables[i]) {
			return true
		}
	}
	return false
}
