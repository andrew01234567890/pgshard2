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
// fragment for publication/slot/workflow names (the agent accepts only
// [A-Za-z0-9_] and requires the pgshard_ prefix).
func seedIdent(name string) string {
	return strings.ReplaceAll(name, "-", "_")
}

// seedPublication is the publication PrepareSource provisions on the source
// shard for a reshard; every target workflow consumes it.
func seedPublication(reshard *pgshardv1alpha1.PgShardReshard) string {
	return "pgshard_" + seedIdent(reshard.Name)
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
) (*corev1.Pod, error) {
	if nodeRef == "" {
		return nil, nil
	}
	var node pgshardv1alpha1.PgShardNode
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: nodeRef}, &node); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if node.Status.Phase != pgshardv1alpha1.NodeReady || node.Status.CurrentPrimary == "" {
		return nil, nil
	}
	var pod corev1.Pod
	if err := r.Get(ctx,
		client.ObjectKey{Namespace: namespace, Name: node.Status.CurrentPrimary}, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if !metav1.IsControlledBy(&pod, &node) || pod.Status.PodIP == "" {
		return nil, nil
	}
	return &pod, nil
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
		if a.Schema != b.Schema {
			return strings.Compare(a.Schema, b.Schema)
		}
		return strings.Compare(a.Name, b.Name)
	})
	return tables, nil
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

// reconcileSeeding drives the Seeding phase: provision the source publication
// (PrepareSource), start one pull workflow per target (StartWorkflow on the
// TARGET's agent, which truncates and re-seeds — every identity in the spec
// is therefore verified: provenance, pod UIDs, verified database chain), and
// advance to CatchingUp once every workflow streams.
func (r *PgShardReshardReconciler) reconcileSeeding(
	ctx context.Context, reshard *pgshardv1alpha1.PgShardReshard,
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

	publication := seedPublication(reshard)
	if len(publication) > maxDatabaseNameBytes {
		r.fail(reshard, reshardSeededCondition, "InvalidPublicationName",
			fmt.Sprintf("publication name %q exceeds %d bytes", publication, maxDatabaseNameBytes))
		return ctrl.Result{}, nil
	}

	tables, err := r.shardedTables(ctx, reshard.Namespace, cluster.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(tables) == 0 {
		// Nothing to copy or stream; the targets are trivially caught up.
		setReshardCondition(reshard, reshardSeededCondition, metav1.ConditionTrue,
			"NothingToSeed", "the cluster has no sharded tables")
		reshard.Status.Phase = pgshardv1alpha1.ReshardCatchingUp
		return ctrl.Result{Requeue: true}, nil
	}

	// Resolve the SOURCE: shard -> node -> verified primary pod.
	var source pgshardv1alpha1.PgShardShard
	sourceKey := client.ObjectKey{Namespace: reshard.Namespace, Name: reshard.Spec.SourceShard}
	if err := r.Get(ctx, sourceKey, &source); err != nil {
		if apierrors.IsNotFound(err) {
			return r.hold(reshard, "SourceShardMissing",
				fmt.Sprintf("source shard %q not found", reshard.Spec.SourceShard))
		}
		return ctrl.Result{}, err
	}
	sourcePod, err := r.primaryEndpoint(ctx, reshard.Namespace, source.Spec.NodeRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if sourcePod == nil {
		return r.hold(reshard, "SourceUnready",
			fmt.Sprintf("source shard %q has no verified primary pod yet", source.Name))
	}
	sourceDB := shardDatabaseName(&source)

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
	seed := seedInputs{
		cluster:     &cluster,
		source:      &source,
		sourcePod:   sourcePod,
		sourceDB:    sourceDB,
		publication: publication,
		tables:      tables,
	}
	for i, targetName := range reshard.Status.TargetShards {
		isStreaming, held, res, err := r.seedTarget(ctx, reshard, seed, i, targetName)
		if err != nil {
			return ctrl.Result{}, err
		}
		if held {
			return res, nil
		}
		if isStreaming {
			streaming++
		}
	}

	if streaming == len(reshard.Status.TargetShards) {
		setReshardCondition(reshard, reshardSeededCondition, metav1.ConditionTrue,
			"Streaming", "every target workflow is streaming")
		reshard.Status.Phase = pgshardv1alpha1.ReshardCatchingUp
		return ctrl.Result{Requeue: true}, nil
	}
	setReshardCondition(reshard, reshardSeededCondition, metav1.ConditionFalse, "Copying",
		fmt.Sprintf("%d/%d target workflows streaming", streaming, len(reshard.Status.TargetShards)))
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
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
) (isStreaming, held bool, res ctrl.Result, err error) {
	holdOn := func(reason, message string) (bool, bool, ctrl.Result, error) {
		res, _ := r.hold(reshard, reason, message)
		return false, true, res, nil
	}
	var target pgshardv1alpha1.PgShardShard
	if err := r.Get(ctx,
		client.ObjectKey{Namespace: reshard.Namespace, Name: targetName}, &target); err != nil {
		if apierrors.IsNotFound(err) {
			return holdOn("TargetShardMissing",
				fmt.Sprintf("target shard %q not found", targetName))
		}
		return false, false, ctrl.Result{}, err
	}
	// The workflow TRUNCATES the target database; only the fully verified
	// placement chain (DatabaseReady on this node incarnation and pod) may
	// be seeded.
	if !apimeta.IsStatusConditionTrue(target.Status.Conditions, shardDatabaseReadyCondition) ||
		target.Status.DatabaseNodeUID == "" || target.Status.DatabasePodUID == "" {
		return holdOn("TargetUnverified",
			fmt.Sprintf("target shard %q has no verified database yet", targetName))
	}
	targetPod, err := r.primaryEndpoint(ctx, reshard.Namespace, target.Spec.NodeRef)
	if err != nil {
		return false, false, ctrl.Result{}, err
	}
	if targetPod == nil || string(targetPod.UID) != target.Status.DatabasePodUID {
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
		return false, true, ctrl.Result{}, nil
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
		return false, false, ctrl.Result{}, err
	}
	if _, err := targetAgent.StartWorkflow(ctx, &pgshardv1.StartWorkflowRequest{
		Spec:         spec,
		TargetPodUid: string(targetPod.UID),
	}); err != nil {
		switch grpcstatus.Code(err) {
		case codes.InvalidArgument:
			r.fail(reshard, reshardSeededCondition, "WorkflowRejected", err.Error())
			return false, true, ctrl.Result{}, nil
		case codes.Unimplemented:
			// The agent has no replication credentials configured — a
			// deployment gap, not a data error. Surface and wait.
			return holdOn("RunnerNotConfigured", err.Error())
		case codes.FailedPrecondition:
			// Conflict (an older spec still running — e.g. the source pod
			// IP changed) or a busy target. The old worker fails on its
			// own and winds down; retry replaces it.
			return holdOn("WorkflowConflict", err.Error())
		default:
			return holdOn("WorkflowStartFailed", err.Error())
		}
	}

	status, err := r.workflowStatus(ctx, targetAgent, id)
	if err != nil {
		return holdOn("WorkflowStatusUnavailable", err.Error())
	}
	switch status.GetPhase() {
	case pgshardv1.WorkflowPhase_WORKFLOW_PHASE_STREAMING:
		return true, false, ctrl.Result{}, nil
	case pgshardv1.WorkflowPhase_WORKFLOW_PHASE_ERROR:
		// The workflow failed loudly (preflight refusal, publication
		// drift, boundary crossing, ...). The NEXT reconcile's
		// StartWorkflow replaces the terminal workflow and re-seeds from
		// scratch; the error is surfaced meanwhile.
		return holdOn("WorkflowFailed",
			fmt.Sprintf("workflow %s: %s", id, status.GetError()))
	}
	return false, false, ctrl.Result{}, nil
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
