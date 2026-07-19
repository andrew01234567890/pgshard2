package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
)

const reshardSeededCondition = "Seeded"

// defaultSchema is PostgreSQL's schema for unqualified table names.
const defaultSchema = "public"

// postgresPort is where the runner reaches the source PostgreSQL (the agent
// gRPC port is agentPort; both live in the same pod).
const postgresPort = 5432

// seedIdent turns a Kubernetes object name into a safe PostgreSQL identifier
// fragment for publication/slot/workflow names: the agent accepts only
// [A-Za-z0-9_], while Kubernetes names may also carry '-' and '.'.
func seedIdent(name string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			return r
		default:
			return '_'
		}
	}, name)
}

// seedSuffixReserve bounds every per-target suffix ("_t" plus the index; 128
// targets max by the shard-count envelope).
const seedSuffixReserve = len("_t128")

// seedPublication is the publication PrepareSource provisions on the source
// shard for a reshard; every target workflow consumes it.
// The reshard UID makes the name collision-resistant: seedIdent is lossy
// ("a.b" and "a-b" both map to "a_b"), and a collision would let one
// reshard's conflict path stop ANOTHER reshard's healthy workflows. The name
// part is truncated so the publication plus the longest slot suffix always
// fits PostgreSQL's 63-byte identifier limit.
func seedPublication(reshard *pgshardv1alpha1.PgShardReshard) string {
	// The FULL sanitized UID (a truncated one is only collision-resistant,
	// and a collision would let the conflict path stop another reshard's
	// workflow); only the human-readable name part truncates.
	uid := seedIdent(string(reshard.UID))
	name := seedIdent(reshard.Name)
	maxName := maxDatabaseNameBytes - len("pgshard_") - 1 - len(uid) - seedSuffixReserve
	if len(name) > maxName {
		name = name[:maxName]
	}
	return fmt.Sprintf("pgshard_%s_%s", name, uid)
}

// seedWorkflowID names the target's workflow AND its replication slot: the
// index is the position in Status.TargetShards, fixed since provisioning.
func seedWorkflowID(reshard *pgshardv1alpha1.PgShardReshard, index int) string {
	return fmt.Sprintf("%s_t%d", seedPublication(reshard), index)
}

// primaryEndpoint resolves a shard placement to its verified primary pod:
// the node must be Ready with a committed CurrentPrimary, and the pod must be
// controlled by THIS node incarnation (a same-named pod of a recreated node
// must never be dialed). Returns nil without error while unready — seeding
// holds and retries.
func (r *PgShardReshardReconciler) primaryEndpoint(
	ctx context.Context, namespace, nodeRef string,
) (*corev1.Pod, *pgshardv1alpha1.PgShardNode, error) {
	if nodeRef == "" {
		return nil, nil, nil
	}
	var node pgshardv1alpha1.PgShardNode
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: nodeRef}, &node); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	if node.Status.Phase != pgshardv1alpha1.NodeReady || node.Status.CurrentPrimary == "" {
		return nil, nil, nil
	}
	var pod corev1.Pod
	if err := r.Get(ctx,
		client.ObjectKey{Namespace: namespace, Name: node.Status.CurrentPrimary}, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	if !metav1.IsControlledBy(&pod, &node) || pod.Status.PodIP == "" {
		return nil, nil, nil
	}
	return &pod, &node, nil
}

// databaseVerified reports whether a shard's database is verified against the
// EXACT placement (node incarnation and primary pod) it would be reached at:
// the DatabaseReady chain established by the shard controller's attested
// CreateDatabase protocol.
func databaseVerified(shard *pgshardv1alpha1.PgShardShard, node *pgshardv1alpha1.PgShardNode, pod *corev1.Pod) bool {
	return apimeta.IsStatusConditionTrue(shard.Status.Conditions, shardDatabaseReadyCondition) &&
		shard.Status.DatabaseNode == node.Name &&
		shard.Status.DatabaseNodeUID == string(node.UID) &&
		shard.Status.DatabasePodUID == string(pod.UID)
}

// shardedTables collects the cluster's sharded-table entries from every
// PgShardTableConfig, sorted by (schema, name) so the workflow spec is
// byte-identical across reconciles (StartWorkflow is idempotent only for an
// identical spec). Global tables live on the system shard and are not seeded.
func (r *PgShardReshardReconciler) shardedTables(
	ctx context.Context, namespace, clusterName string,
) ([]pgshardv1alpha1.TableEntry, error) {
	var configs pgshardv1alpha1.PgShardTableConfigList
	if err := r.List(ctx, &configs, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	var tables []pgshardv1alpha1.TableEntry
	for _, cfg := range configs.Items {
		if cfg.Spec.ClusterRef != clusterName {
			continue
		}
		for _, t := range cfg.Spec.Tables {
			if t.Type != pgshardv1alpha1.TableSharded {
				continue
			}
			if t.Schema == "" {
				t.Schema = defaultSchema
			}
			tables = append(tables, t)
		}
	}
	slices.SortFunc(tables, func(a, b pgshardv1alpha1.TableEntry) int {
		if c := strings.Compare(a.Schema, b.Schema); c != 0 {
			return c
		}
		if c := strings.Compare(a.Name, b.Name); c != 0 {
			return c
		}
		if c := strings.Compare(a.ShardKeyColumn, b.ShardKeyColumn); c != 0 {
			return c
		}
		return strings.Compare(string(a.ShardKeyType), string(b.ShardKeyType))
	})
	return tables, nil
}

func pinTables(tables []pgshardv1alpha1.TableEntry) []pgshardv1alpha1.ReshardSeedTable {
	pinned := make([]pgshardv1alpha1.ReshardSeedTable, 0, len(tables))
	for _, t := range tables {
		pinned = append(pinned, pgshardv1alpha1.ReshardSeedTable{
			Schema:         t.Schema,
			Name:           t.Name,
			ShardKeyColumn: t.ShardKeyColumn,
			ShardKeyType:   t.ShardKeyType,
		})
	}
	return pinned
}

func unpinTables(pinned []pgshardv1alpha1.ReshardSeedTable) []pgshardv1alpha1.TableEntry {
	tables := make([]pgshardv1alpha1.TableEntry, 0, len(pinned))
	for _, t := range pinned {
		tables = append(tables, pgshardv1alpha1.TableEntry{
			Schema:         t.Schema,
			Name:           t.Name,
			Type:           pgshardv1alpha1.TableSharded,
			ShardKeyColumn: t.ShardKeyColumn,
			ShardKeyType:   t.ShardKeyType,
		})
	}
	return tables
}

// tablesDiffer reports (as a message; empty = equal) how the live sharded
// schema diverged from the pinned one. Both are (schema, name)-sorted.
func tablesDiffer(pinned []pgshardv1alpha1.ReshardSeedTable, live []pgshardv1alpha1.TableEntry) string {
	if len(pinned) != len(live) {
		return fmt.Sprintf("the cluster now has %d sharded tables; %d were pinned when seeding began",
			len(live), len(pinned))
	}
	for i, p := range pinned {
		l := live[i]
		if p.Schema != l.Schema || p.Name != l.Name ||
			p.ShardKeyColumn != l.ShardKeyColumn || p.ShardKeyType != l.ShardKeyType {
			return fmt.Sprintf("sharded table %s.%s changed since seeding began", p.Schema, p.Name)
		}
	}
	return ""
}

// hold records why seeding is waiting and retries shortly. Holding is the
// default posture: everything here converges once the referenced pieces
// (pods, databases, workflows) settle.
func (r *PgShardReshardReconciler) hold(
	reshard *pgshardv1alpha1.PgShardReshard, reason, message string,
) (ctrl.Result, error) {
	setReshardCondition(reshard, reshardSeededCondition, metav1.ConditionFalse, reason, message)
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// seedPassMode selects the gate a seed pass advances through: Seeding waits
// for every workflow to stream; CatchingUp additionally waits for replication
// lag to fall under the cutover threshold.
type seedPassMode int

const (
	seedModeSeeding seedPassMode = iota
	seedModeCatchingUp
)

// catchupMaxLagBytes is how far behind the source's write position every
// target may be before the reshard is ready to cut over. The cutover slice
// re-verifies at the freeze point; this bound only keeps the gated write
// pause short.
const catchupMaxLagBytes = 16 << 20

// reconcileSeeding drives the Seeding phase: provision the source publication
// (PrepareSource), start one pull workflow per target (StartWorkflow on the
// TARGET's agent, which truncates and re-seeds — every identity in the spec
// is therefore verified: provenance, pod UIDs, verified database chain), and
// advance to CatchingUp once every workflow streams.
func (r *PgShardReshardReconciler) reconcileSeeding(
	ctx context.Context, reshard *pgshardv1alpha1.PgShardReshard,
) (ctrl.Result, error) {
	return r.runSeedPass(ctx, reshard, seedModeSeeding)
}

// reconcileCatchingUp re-runs the SAME identity-checked pass every reconcile
// — re-acking StartWorkflow keeps workflows alive across agent restarts (the
// registry is in-memory), and every mutable identity is re-verified — and
// advances to ReadyToCutover once all workflows stream within the lag bound.
func (r *PgShardReshardReconciler) reconcileCatchingUp(
	ctx context.Context, reshard *pgshardv1alpha1.PgShardReshard,
) (ctrl.Result, error) {
	return r.runSeedPass(ctx, reshard, seedModeCatchingUp)
}

func (r *PgShardReshardReconciler) runSeedPass(
	ctx context.Context, reshard *pgshardv1alpha1.PgShardReshard, mode seedPassMode,
) (ctrl.Result, error) {
	var cluster pgshardv1alpha1.PgShardCluster
	clusterKey := client.ObjectKey{Namespace: reshard.Namespace, Name: reshard.Spec.ClusterRef}
	if err := r.Get(ctx, clusterKey, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return r.hold(reshard, "ClusterNotFound",
				fmt.Sprintf("cluster %q not found", reshard.Spec.ClusterRef))
		}
		return ctrl.Result{}, err
	}

	// The pinned cluster identity must still hold: a same-named replacement
	// cluster was never validated.
	if reshard.Status.ClusterUID == "" || reshard.Status.ClusterUID != string(cluster.UID) {
		r.fail(reshard, reshardSeededCondition, "ClusterReplaced",
			fmt.Sprintf("cluster %q is not the object this reshard was validated against", cluster.Name))
		return ctrl.Result{}, nil
	}

	publication := seedPublication(reshard)
	// The slot name is the workflow id (publication + per-target suffix) and
	// must also fit PostgreSQL's identifier limit.
	if len(publication)+seedSuffixReserve > maxDatabaseNameBytes {
		r.fail(reshard, reshardSeededCondition, "InvalidPublicationName",
			fmt.Sprintf("publication name %q leaves no room for target slot suffixes within %d bytes",
				publication, maxDatabaseNameBytes))
		return ctrl.Result{}, nil
	}

	source, held, res, err := r.resolveSource(ctx, reshard)
	if err != nil {
		return ctrl.Result{}, err
	}
	if held {
		return res, nil
	}
	// The target list is DERIVED from the immutable spec, never trusted from
	// mutable status: a tampered status.targetShards entry would aim a
	// truncating workflow at a foreign database. Status must agree exactly.
	expected := make([]string, 0, len(reshard.Spec.TargetRanges))
	for _, tr := range reshard.Spec.TargetRanges {
		expected = append(expected, shardName(cluster.Name, tr.Start, tr.End))
	}
	if !slices.Equal(reshard.Status.TargetShards, expected) {
		r.fail(reshard, reshardSeededCondition, "TargetListMismatch",
			fmt.Sprintf("status.targetShards %v does not match the spec-derived targets %v",
				reshard.Status.TargetShards, expected))
		return ctrl.Result{}, nil
	}

	// Pin the sharded-table schema ONCE — and PERSIST the pin (requeue)
	// before any RPC side effect, so a crash between the pin and the status
	// write can never let a later reconcile re-pin edited configs after
	// workflows already started under the old schema. Specs build only from
	// the pinned copy.
	live, err := r.shardedTables(ctx, reshard.Namespace, cluster.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !reshard.Status.SeedTablesPinned {
		if mode != seedModeSeeding {
			// Seeding always persists the pin before advancing; a later
			// phase without one is a broken invariant, never a fresh start.
			r.fail(reshard, reshardSeededCondition, "SchemaUnpinned",
				"no pinned seed schema; the reshard cannot resume safely")
			return ctrl.Result{}, nil
		}
		reshard.Status.SeedTables = pinTables(live)
		reshard.Status.SeedTablesPinned = true
		setReshardCondition(reshard, reshardSeededCondition, metav1.ConditionFalse,
			"SchemaPinned", fmt.Sprintf("pinned %d sharded tables; starting workflows", len(live)))
		return ctrl.Result{Requeue: true}, nil
	}
	tables := unpinTables(reshard.Status.SeedTables)
	if drift := tablesDiffer(reshard.Status.SeedTables, live); drift != "" {
		// The workflows keep streaming the PINNED schema; advancing while
		// the desired schema moved would cut over an incomplete seed. A
		// mid-reshard schema change needs the reshard restarted.
		return r.hold(reshard, "SchemaDrift", drift)
	}
	if len(tables) == 0 {
		return r.advanceNothingToSeed(ctx, reshard, mode)
	}

	sourcePod, sourceNode, err := r.primaryEndpoint(ctx, reshard.Namespace, source.Spec.NodeRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if sourcePod == nil {
		return r.hold(reshard, "SourceUnready",
			fmt.Sprintf("source shard %q has no verified primary pod yet", source.Name))
	}
	// The source DATABASE must be verified against this exact placement —
	// the same chain that gates routing — before its content is copied out.
	if !databaseVerified(source, sourceNode, sourcePod) {
		return r.hold(reshard, "SourceUnverified",
			fmt.Sprintf("source shard %q database is not verified on its current primary", source.Name))
	}
	sourceDB := shardDatabaseName(source)

	tableRefs := make([]*pgshardv1.TableRef, 0, len(tables))
	for _, t := range tables {
		tableRefs = append(tableRefs, &pgshardv1.TableRef{Schema: t.Schema, Name: t.Name})
	}
	sourceAgent, err := r.agentClient(sourcePod.Status.PodIP, agentPort)
	if err != nil {
		return ctrl.Result{}, err
	}
	if _, err := sourceAgent.PrepareSource(ctx, &pgshardv1.PrepareSourceRequest{
		Publication:  publication,
		Tables:       tableRefs,
		Database:     sourceDB,
		TargetPodUid: string(sourcePod.UID),
	}); err != nil {
		switch grpcstatus.Code(err) {
		case codes.InvalidArgument:
			// A contract violation is permanent; the spec cannot converge.
			r.fail(reshard, reshardSeededCondition, "PrepareSourceRejected", err.Error())
			return ctrl.Result{}, nil
		default:
			// ABORTED (stale pod address), UNAVAILABLE, transient failures:
			// hold and re-resolve.
			return r.hold(reshard, "PrepareSourceFailed", err.Error())
		}
	}

	// Start (idempotently) one pull workflow per target and collect phases.
	streaming := 0
	applied := make([]*uint64, 0, len(reshard.Status.TargetShards))
	seed := seedInputs{
		cluster:     &cluster,
		source:      source,
		sourcePod:   sourcePod,
		sourceDB:    sourceDB,
		publication: publication,
		tables:      tables,
	}
	for i, targetName := range reshard.Status.TargetShards {
		isStreaming, appliedLsn, held, res, err := r.seedTarget(ctx, reshard, seed, i, targetName)
		if err != nil {
			return ctrl.Result{}, err
		}
		if held {
			return res, nil
		}
		if isStreaming {
			streaming++
			applied = append(applied, appliedLsn)
		}
	}

	if streaming < len(reshard.Status.TargetShards) {
		setReshardCondition(reshard, reshardSeededCondition, metav1.ConditionFalse, "Copying",
			fmt.Sprintf("%d/%d target workflows streaming", streaming, len(reshard.Status.TargetShards)))
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if mode == seedModeSeeding {
		setReshardCondition(reshard, reshardSeededCondition, metav1.ConditionTrue,
			"Streaming", "every target workflow is streaming")
		reshard.Status.Phase = pgshardv1alpha1.ReshardCatchingUp
		return ctrl.Result{Requeue: true}, nil
	}
	return r.gateCatchup(ctx, reshard, source, sourcePod, applied)
}

// gateCatchup advances CatchingUp to ReadyToCutover once the slowest workflow
// is within the lag bound of the source's CURRENT write position. Reading the
// source status through the same verified pod keeps the comparison anchored
// to the database being copied.
func (r *PgShardReshardReconciler) gateCatchup(
	ctx context.Context,
	reshard *pgshardv1alpha1.PgShardReshard,
	source *pgshardv1alpha1.PgShardShard,
	sourcePod *corev1.Pod,
	applied []*uint64,
) (ctrl.Result, error) {
	status, err := r.sourceStatus(ctx, sourcePod)
	if err != nil {
		return r.hold(reshard, "SourceStatusUnavailable", err.Error())
	}
	// The WAL position is only meaningful from a live, unfenced PRIMARY: a
	// standby or fenced instance after an intra-pass failover would report a
	// position from the wrong timeline or a frozen one.
	if status.GetRole() != pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY ||
		!status.GetReady() || status.GetFenced() {
		return r.hold(reshard, "SourceStatusUnavailable",
			fmt.Sprintf("source pod %s is not a ready unfenced primary", sourcePod.Name))
	}
	// Re-confirm the pod REMAINED the committed primary across the status
	// RPC: a failover between resolution and the read would hand us a
	// deposed instance's numbers.
	confirmPod, confirmNode, err := r.primaryEndpoint(ctx, reshard.Namespace, source.Spec.NodeRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if confirmPod == nil || confirmPod.UID != sourcePod.UID || !databaseVerified(source, confirmNode, confirmPod) {
		return r.hold(reshard, "SourceStatusUnavailable",
			fmt.Sprintf("source pod %s stopped being the verified primary during the status read", sourcePod.Name))
	}
	if status.GetWalWriteLsn() == nil {
		return r.hold(reshard, "LagUnmeasurable", "the source reports no WAL write position")
	}
	writeLsn := status.GetWalWriteLsn().GetValue()
	var minApplied uint64
	for i, a := range applied {
		if a == nil {
			// A streaming workflow with no watermark yet cannot be compared;
			// never advance on an unmeasured target.
			return r.hold(reshard, "LagUnmeasurable",
				"a target workflow reports no applied position yet")
		}
		if *a > writeLsn {
			// A position AHEAD of the source's write position means the
			// numbers are not from the same timeline/database — never
			// advance on nonsense, even if another target masks it.
			return r.hold(reshard, "LagUnmeasurable",
				fmt.Sprintf("applied position %#x is ahead of the source write position %#x", *a, writeLsn))
		}
		if i == 0 || *a < minApplied {
			minApplied = *a
		}
	}
	// NOTE: writeLsn is INSTANCE-wide while each workflow's watermark only
	// advances on decoded commits of ITS database — heavy traffic in other
	// databases on a shared node inflates apparent lag. That errs stuck, not
	// wrong; the cutover slice replaces this with a database-local barrier
	// acknowledged by every workflow.
	if lag := writeLsn - minApplied; lag > catchupMaxLagBytes {
		setReshardCondition(reshard, reshardSeededCondition, metav1.ConditionFalse, "Lagging",
			fmt.Sprintf("slowest target is %d bytes behind the source (bound %d)", lag, uint64(catchupMaxLagBytes)))
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	setReshardCondition(reshard, reshardSeededCondition, metav1.ConditionTrue, "CaughtUp",
		fmt.Sprintf("every target workflow is streaming within %d bytes of the source", uint64(catchupMaxLagBytes)))
	reshard.Status.Phase = pgshardv1alpha1.ReshardReadyToCutover
	return ctrl.Result{Requeue: true}, nil
}

// resolveSource fetches the source shard and re-validates every mutable
// identity BEFORE any shortcut or side effect: the pinned UID (a same-named
// replacement's data was never validated), Serving, and Role. held=true means
// the caller returns res.
func (r *PgShardReshardReconciler) resolveSource(
	ctx context.Context, reshard *pgshardv1alpha1.PgShardReshard,
) (*pgshardv1alpha1.PgShardShard, bool, ctrl.Result, error) {
	var source pgshardv1alpha1.PgShardShard
	sourceKey := client.ObjectKey{Namespace: reshard.Namespace, Name: reshard.Spec.SourceShard}
	if err := r.Get(ctx, sourceKey, &source); err != nil {
		if apierrors.IsNotFound(err) {
			res, _ := r.hold(reshard, "SourceShardMissing",
				fmt.Sprintf("source shard %q not found", reshard.Spec.SourceShard))
			return nil, true, res, nil
		}
		return nil, false, ctrl.Result{}, err
	}
	// The pinned source identity must still hold: a shard deleted and
	// recreated under the same name is a different placement whose data was
	// never validated — seeding would copy the WRONG database. Serving and
	// Role are mutable and re-checked on EVERY reconcile.
	if reshard.Status.SourceShardUID == "" || reshard.Status.SourceShardUID != string(source.UID) {
		r.fail(reshard, reshardSeededCondition, "SourceReplaced",
			fmt.Sprintf("source shard %q is not the object this reshard was validated against", source.Name))
		return nil, true, ctrl.Result{}, nil
	}
	if !source.Spec.Serving {
		r.fail(reshard, reshardSeededCondition, "SourceNotServing",
			fmt.Sprintf("source shard %q is no longer serving; its data is not authoritative", source.Name))
		return nil, true, ctrl.Result{}, nil
	}
	if source.Spec.Role == pgshardv1alpha1.ShardRoleSystem {
		r.fail(reshard, reshardSeededCondition, "SourceNotReshardable",
			fmt.Sprintf("source shard %q became the system shard", source.Name))
		return nil, true, ctrl.Result{}, nil
	}
	return &source, false, ctrl.Result{}, nil
}

// sourceStatus polls the source agent's instance status via the verified
// primary pod.
func (r *PgShardReshardReconciler) sourceStatus(
	ctx context.Context, sourcePod *corev1.Pod,
) (*pgshardv1.InstanceStatus, error) {
	agent, err := r.agentClient(sourcePod.Status.PodIP, agentPort)
	if err != nil {
		return nil, err
	}
	resp, err := agent.GetStatus(ctx, &pgshardv1.GetStatusRequest{})
	if err != nil {
		return nil, err
	}
	if resp.GetStatus() == nil {
		return nil, fmt.Errorf("source agent returned an empty status")
	}
	return resp.GetStatus(), nil
}

// advanceNothingToSeed advances an empty-schema reshard to CatchingUp — but
// only once the phase invariant (every target exists, is ours, hidden, and
// range-correct) holds, exactly as the workflow path enforces per target.
func (r *PgShardReshardReconciler) advanceNothingToSeed(
	ctx context.Context, reshard *pgshardv1alpha1.PgShardReshard, mode seedPassMode,
) (ctrl.Result, error) {
	for i, targetName := range reshard.Status.TargetShards {
		var target pgshardv1alpha1.PgShardShard
		if err := r.Get(ctx,
			client.ObjectKey{Namespace: reshard.Namespace, Name: targetName}, &target); err != nil {
			if apierrors.IsNotFound(err) {
				return r.hold(reshard, "TargetShardMissing",
					fmt.Sprintf("target shard %q not found", targetName))
			}
			return ctrl.Result{}, err
		}
		if reason, msg := r.foreignTarget(reshard, &target, i); reason != "" {
			r.fail(reshard, reshardSeededCondition, reason, msg)
			return ctrl.Result{}, nil
		}
	}
	setReshardCondition(reshard, reshardSeededCondition, metav1.ConditionTrue,
		"NothingToSeed", "the cluster has no sharded tables")
	if mode == seedModeSeeding {
		reshard.Status.Phase = pgshardv1alpha1.ReshardCatchingUp
	} else {
		reshard.Status.Phase = pgshardv1alpha1.ReshardReadyToCutover
	}
	return ctrl.Result{Requeue: true}, nil
}

// foreignTarget reports (reason, message) when a shard is NOT the hidden
// data-role target this reshard provisioned — only such a target may be
// truncated and seeded. Serving and Role are mutable, so both paths re-check
// on every reconcile.
func (r *PgShardReshardReconciler) foreignTarget(
	reshard *pgshardv1alpha1.PgShardReshard, target *pgshardv1alpha1.PgShardShard, i int,
) (string, string) {
	if !metav1.IsControlledBy(target, reshard) ||
		target.Spec.ClusterRef != reshard.Spec.ClusterRef ||
		target.Spec.KeyRange != reshard.Spec.TargetRanges[i] ||
		target.Spec.Serving ||
		target.Spec.Role == pgshardv1alpha1.ShardRoleSystem {
		return "TargetForeign",
			fmt.Sprintf("target shard %q is not the hidden data target this reshard provisioned", target.Name)
	}
	return "", ""
}

// seedInputs carries the resolved, reshard-wide seeding context.
type seedInputs struct {
	cluster     *pgshardv1alpha1.PgShardCluster
	source      *pgshardv1alpha1.PgShardShard
	sourcePod   *corev1.Pod
	sourceDB    string
	publication string
	tables      []pgshardv1alpha1.TableEntry
}

// seedTarget starts (idempotently) one target's workflow and reads its phase.
// held=true means the caller should return res (a hold or terminal failure).
func (r *PgShardReshardReconciler) seedTarget(
	ctx context.Context,
	reshard *pgshardv1alpha1.PgShardReshard,
	seed seedInputs,
	i int,
	targetName string,
) (isStreaming bool, appliedLsn *uint64, held bool, res ctrl.Result, err error) {
	holdOn := func(reason, message string) (bool, *uint64, bool, ctrl.Result, error) {
		res, _ := r.hold(reshard, reason, message)
		return false, nil, true, res, nil
	}
	var target pgshardv1alpha1.PgShardShard
	if err := r.Get(ctx,
		client.ObjectKey{Namespace: reshard.Namespace, Name: targetName}, &target); err != nil {
		if apierrors.IsNotFound(err) {
			return holdOn("TargetShardMissing",
				fmt.Sprintf("target shard %q not found", targetName))
		}
		return false, nil, false, ctrl.Result{}, err
	}
	if reason, msg := r.foreignTarget(reshard, &target, i); reason != "" {
		r.fail(reshard, reshardSeededCondition, reason, msg)
		return false, nil, true, ctrl.Result{}, nil
	}
	// The workflow TRUNCATES the target database; only the fully verified
	// placement chain (DatabaseReady on this node incarnation and pod) may
	// be seeded.
	if !apimeta.IsStatusConditionTrue(target.Status.Conditions, shardDatabaseReadyCondition) {
		return holdOn("TargetUnverified",
			fmt.Sprintf("target shard %q has no verified database yet", targetName))
	}
	targetPod, targetNode, err := r.primaryEndpoint(ctx, reshard.Namespace, target.Spec.NodeRef)
	if err != nil {
		return false, nil, false, ctrl.Result{}, err
	}
	if targetPod == nil || !databaseVerified(&target, targetNode, targetPod) {
		// A failover or pod replacement since verification: the shard
		// controller re-verifies, then seeding resumes.
		return holdOn("TargetUnready",
			fmt.Sprintf("target shard %q primary does not match its verified database placement", targetName))
	}

	id := seedWorkflowID(reshard, i)
	mappings := make([]*pgshardv1.TableMapping, 0, len(seed.tables))
	for _, t := range seed.tables {
		mappings = append(mappings, &pgshardv1.TableMapping{
			Source:         &pgshardv1.TableRef{Schema: t.Schema, Name: t.Name},
			ShardKeyColumn: t.ShardKeyColumn,
			ShardKeyType:   string(t.ShardKeyType),
		})
	}
	targetRange, err := toRange(target.Spec.KeyRange)
	if err != nil {
		// The range was validated at Validating; a malformed one here is
		// tampering or corruption, never transient.
		r.fail(reshard, reshardSeededCondition, "InvalidTargetRange", err.Error())
		return false, nil, true, ctrl.Result{}, nil
	}
	wireRange := &pgshardv1.KeyRange{Start: targetRange.Start()}
	if end, closed := targetRange.End(); closed {
		wireRange.End = &end
	}
	spec := &pgshardv1.WorkflowSpec{
		Id:          id,
		Kind:        pgshardv1.WorkflowKind_WORKFLOW_KIND_RESHARD,
		SourceShard: seed.source.Name,
		SourcePrimary: &pgshardv1.PgEndpoint{
			Host:     seed.sourcePod.Status.PodIP,
			Port:     postgresPort,
			Database: seed.sourceDB,
		},
		SourcePolicy: pgshardv1.SourcePolicy_SOURCE_POLICY_PRIMARY,
		Slot:         id,
		Publication:  seed.publication,
		Tables:       mappings,
		Filter: &pgshardv1.RowFilter{
			Filter: &pgshardv1.RowFilter_KeyRange{
				KeyRange: &pgshardv1.KeyRangeFilter{
					Range:        wireRange,
					HashFunction: seed.cluster.Spec.Postgres.HashFunction,
				},
			},
		},
		TargetDatabase: shardDatabaseName(&target),
		// The target database's provenance marker must match, or the
		// runner refuses to truncate — a misdirected spec fails closed.
		ExpectProvenance: string(target.UID),
	}
	targetAgent, err := r.agentClient(targetPod.Status.PodIP, agentPort)
	if err != nil {
		return false, nil, false, ctrl.Result{}, err
	}
	if _, err := targetAgent.StartWorkflow(ctx, &pgshardv1.StartWorkflowRequest{
		Spec:         spec,
		TargetPodUid: string(targetPod.UID),
	}); err != nil {
		switch grpcstatus.Code(err) {
		case codes.InvalidArgument:
			r.fail(reshard, reshardSeededCondition, "WorkflowRejected", err.Error())
			return false, nil, true, ctrl.Result{}, nil
		case codes.Unimplemented:
			// The agent has no replication credentials configured — a
			// deployment gap, not a data error. Surface and wait.
			return holdOn("RunnerNotConfigured", err.Error())
		case codes.FailedPrecondition:
			// Conflict: an older, differing spec still runs under OUR id
			// (e.g. the source pod IP changed) — it need not die on its own,
			// so signal a stop; the agent answers Stopping until the old
			// worker terminates, then the retry replaces it. Stopping an
			// unknown id (the TargetBusy case names ANOTHER id) is a no-op.
			if _, stopErr := targetAgent.StopWorkflow(ctx,
				&pgshardv1.StopWorkflowRequest{Id: id}); stopErr != nil {
				return holdOn("WorkflowConflict",
					fmt.Sprintf("%v (stopping the stale worker also failed: %v)", err, stopErr))
			}
			return holdOn("WorkflowConflict", err.Error())
		default:
			return holdOn("WorkflowStartFailed", err.Error())
		}
	}

	status, err := r.workflowStatus(ctx, targetAgent, id)
	if err != nil {
		return holdOn("WorkflowStatusUnavailable", err.Error())
	}
	// Re-confirm the TARGET pod remained the verified primary across the
	// status RPC, mirroring the source-side check: a failover during the
	// read would otherwise let a deposed instance's watermark gate cutover.
	confirmPod, confirmNode, err := r.primaryEndpoint(ctx, reshard.Namespace, target.Spec.NodeRef)
	if err != nil {
		return false, nil, false, ctrl.Result{}, err
	}
	if confirmPod == nil || confirmPod.UID != targetPod.UID ||
		!databaseVerified(&target, confirmNode, confirmPod) {
		return holdOn("TargetUnready",
			fmt.Sprintf("target shard %q primary changed during the status read", targetName))
	}
	switch status.GetPhase() {
	case pgshardv1.WorkflowPhase_WORKFLOW_PHASE_STREAMING:
		var applied *uint64
		if lsn := status.GetAppliedLsn(); lsn != nil {
			v := lsn.GetValue()
			applied = &v
		}
		return true, applied, false, ctrl.Result{}, nil
	case pgshardv1.WorkflowPhase_WORKFLOW_PHASE_ERROR:
		// The workflow failed loudly (preflight refusal, publication
		// drift, boundary crossing, ...). The NEXT reconcile's
		// StartWorkflow replaces the terminal workflow and re-seeds from
		// scratch; the error is surfaced meanwhile.
		return holdOn("WorkflowFailed",
			fmt.Sprintf("workflow %s: %s", id, status.GetError()))
	}
	return false, nil, false, ctrl.Result{}, nil
}

// workflowStatus reads one workflow's current status: WatchWorkflows streams
// snapshots (first one immediately), so a single bounded Recv is a poll.
func (r *PgShardReshardReconciler) workflowStatus(
	ctx context.Context, agent pgshardv1.AgentServiceClient, id string,
) (*pgshardv1.WorkflowStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	stream, err := agent.WatchWorkflows(ctx, &pgshardv1.WatchWorkflowsRequest{Ids: []string{id}})
	if err != nil {
		return nil, err
	}
	resp, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("reading workflow %s status: %w", id, err)
	}
	if resp.GetStatus().GetId() != id {
		return nil, fmt.Errorf("workflow %s status stream answered for %q", id, resp.GetStatus().GetId())
	}
	return resp.GetStatus(), nil
}
