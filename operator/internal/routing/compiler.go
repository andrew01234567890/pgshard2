// Package routing compiles the cluster's scattered state (shards, table
// configs, workflow gates) into the single atomically-versioned
// PgShardRouting object that routers and agents watch. Only the
// leader-elected operator writes it; every change is one write with a
// strictly monotonic epoch, so consumers can order updates with one rule:
// apply iff epoch > last applied.
package routing

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard2/operator/internal/topology"
)

// defaultWriteLeaseSeconds mirrors RouterSpec.writeLeaseSeconds's CRD default,
// used when a cluster leaves the router (or its lease) unset.
const defaultWriteLeaseSeconds int32 = 10

const defaultSchema = "public"

const (
	instanceRolePrimary pgshardv1alpha1.InstanceRole = "primary"
	instanceRoleReplica pgshardv1alpha1.InstanceRole = "replica"
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
	// Router is optional; when absent (or its lease left unset) fall back to the
	// RouterSpec.writeLeaseSeconds default rather than nil-deref / emit 0.
	writeLease := defaultWriteLeaseSeconds
	if rt := in.Cluster.Spec.Router; rt != nil && rt.WriteLeaseSeconds > 0 {
		writeLease = rt.WriteLeaseSeconds
	}
	// Every emitted list is sorted into a canonical order: the compiled spec is
	// compared byte-for-byte (specEquivalent) and index-aligned (structuralChange),
	// so any input-order dependence would churn the epoch / topologyGeneration.
	gates := slices.Clone(in.Gates)
	gateIDs := make(map[string]bool, len(gates))
	for _, g := range gates {
		if gateIDs[g.ID] {
			return pgshardv1alpha1.PgShardRoutingSpec{}, fmt.Errorf("duplicate gate id %q", g.ID)
		}
		gateIDs[g.ID] = true
	}
	// Gate IDs are unique (enforced above), so ID alone is a total order.
	slices.SortFunc(gates, func(a, b pgshardv1alpha1.RoutingGate) int {
		return cmp.Compare(a.ID, b.ID)
	})
	spec := pgshardv1alpha1.PgShardRoutingSpec{
		WriteLeaseSeconds: writeLease,
		HashFunction:      in.Cluster.Spec.Postgres.HashFunction,
		Gates:             gates,
	}

	shards := slices.Clone(in.Shards)
	slices.SortFunc(shards, func(a, b pgshardv1alpha1.PgShardShard) int {
		// Total order: Start alone ties whenever a reshard source and a split
		// target share a bound, so tiebreak on End then Name. Names are unique
		// (Kubernetes object identity), so this is a strict order for any real
		// input list.
		return cmp.Or(
			cmp.Compare(a.Spec.KeyRange.Start, b.Spec.KeyRange.Start),
			cmp.Compare(a.Spec.KeyRange.End, b.Spec.KeyRange.End),
			cmp.Compare(a.Name, b.Name),
		)
	})

	var servingRanges []topology.KeyRange
	systemSeen := false
	for _, shard := range shards {
		if shard.Spec.Role == pgshardv1alpha1.ShardRoleSystem {
			// The system shard is not part of the sharded keyspace; it is
			// published as the sequence endpoint instead. There must be exactly
			// one, or the sequence host is ambiguous.
			if systemSeen {
				return spec, fmt.Errorf("cluster has more than one system shard")
			}
			systemSeen = true
			if ep, ok := primaryEndpoint(in.Endpoints, shard.Status.Instances, shard.Status.CurrentPrimary); ok {
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
		if ep, ok := primaryEndpoint(in.Endpoints, shard.Status.Instances, shard.Status.CurrentPrimary); ok {
			entry.Primary = &ep
		}
		entry.Replicas = replicaEndpoints(in.Endpoints, shard.Status.Instances, shard.Status.CurrentPrimary)
		spec.Shards = append(spec.Shards, entry)
	}
	if err := topology.ValidatePartition(servingRanges); err != nil {
		return spec, fmt.Errorf("serving shards do not partition the keyspace: %w", err)
	}

	tables, err := compileTables(in.TableConfigs)
	if err != nil {
		return spec, err
	}
	spec.Tables = tables
	return spec, nil
}

// primaryEndpoint returns the current primary's endpoint only when that
// instance is present, Ready, and primary-role — so routing never publishes an
// unconfirmed writer.
func primaryEndpoint(
	endpoints map[string]pgshardv1alpha1.RoutingEndpoint,
	instances []pgshardv1alpha1.InstanceState,
	primary string,
) (pgshardv1alpha1.RoutingEndpoint, bool) {
	if primary == "" {
		return pgshardv1alpha1.RoutingEndpoint{}, false
	}
	for _, inst := range instances {
		if inst.Pod != primary {
			continue
		}
		if !inst.Ready || inst.Role != instanceRolePrimary {
			return pgshardv1alpha1.RoutingEndpoint{}, false
		}
		ep, ok := endpoints[primary]
		return ep, ok
	}
	return pgshardv1alpha1.RoutingEndpoint{}, false
}

// replicaEndpoints returns the shard's replica endpoints in canonical (pod)
// order, excluding the current primary and any stale primary-role instance (an
// old primary after failover must not be published as a read replica).
func replicaEndpoints(
	endpoints map[string]pgshardv1alpha1.RoutingEndpoint,
	instances []pgshardv1alpha1.InstanceState,
	primary string,
) []pgshardv1alpha1.RoutingEndpoint {
	var out []pgshardv1alpha1.RoutingEndpoint
	for _, inst := range instances {
		// Only explicit replicas are readable — an empty or primary role could
		// be a stale writer, never safe to route reads to.
		if inst.Role != instanceRoleReplica || inst.Pod == primary {
			continue
		}
		ep, ok := endpoints[inst.Pod]
		if !ok {
			continue
		}
		ep.CanRead = inst.Ready
		out = append(out, ep)
	}
	slices.SortFunc(out, func(a, b pgshardv1alpha1.RoutingEndpoint) int {
		return cmp.Compare(a.Pod, b.Pod)
	})
	return out
}

// compileTables folds every table config's entries into one deduplicated,
// canonically-ordered catalog. Duplicate detection and output order both fold
// identifiers to lower case, matching PostgreSQL's unquoted-identifier rules.
func compileTables(configs []pgshardv1alpha1.PgShardTableConfig) ([]pgshardv1alpha1.RoutingTable, error) {
	// Process configs in name order so duplicate-table error attribution
	// (FirstConfig/SecondConfig) is deterministic regardless of input order.
	sorted := slices.Clone(configs)
	slices.SortFunc(sorted, func(a, b pgshardv1alpha1.PgShardTableConfig) int {
		return cmp.Compare(a.Name, b.Name)
	})
	owner := map[string]string{}
	var tables []pgshardv1alpha1.RoutingTable
	for _, tc := range sorted {
		for _, t := range tc.Spec.Tables {
			schema := t.Schema
			if schema == "" {
				schema = defaultSchema
			}
			key := strings.ToLower(schema + "." + t.Name)
			if first, dup := owner[key]; dup {
				return nil, &DuplicateTableError{Table: key, FirstConfig: first, SecondConfig: tc.Name}
			}
			owner[key] = tc.Name
			seqs := slices.Clone(t.Sequences)
			slices.SortFunc(seqs, func(a, b pgshardv1alpha1.RoutingSequence) int {
				return cmp.Or(cmp.Compare(a.Column, b.Column), cmp.Compare(a.Sequence, b.Sequence))
			})
			tables = append(tables, pgshardv1alpha1.RoutingTable{
				Schema:         schema,
				Name:           t.Name,
				Type:           t.Type,
				ShardKeyColumn: t.ShardKeyColumn,
				Sequences:      seqs,
			})
		}
	}
	slices.SortFunc(tables, func(a, b pgshardv1alpha1.RoutingTable) int {
		return cmp.Or(cmp.Compare(a.Schema, b.Schema), cmp.Compare(a.Name, b.Name))
	})
	return tables, nil
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
			if createErr != nil {
				return 0, false, createErr
			}
			return 1, true, nil
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

// structuralChange: the parts a restore pins and resharding changes — the
// shard set (names, ranges, states) and the table catalog. Endpoint/primary
// churn is deliberately excluded (it bumps the epoch but not the generation).
func structuralChange(old, updated pgshardv1alpha1.PgShardRoutingSpec) bool {
	return !slices.EqualFunc(old.Shards, updated.Shards, sameStructuralShard) ||
		!apiequality.Semantic.DeepEqual(old.Tables, updated.Tables)
}

// sameStructuralShard compares only a shard's structural identity. New
// structural fields (ones a reshard/restore changes) must be added here.
func sameStructuralShard(a, b pgshardv1alpha1.RoutingShard) bool {
	return a.Name == b.Name && a.KeyRange == b.KeyRange && a.State == b.State
}
