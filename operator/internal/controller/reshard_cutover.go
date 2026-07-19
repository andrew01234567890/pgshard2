package controller

import (
	"context"
	"fmt"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
)

const reshardCutoverCondition = "CutOver"

// cutoverGateWindow is how long the gate may stand before an uncommitted
// cutover rolls back to CatchingUp — the bound on the write-refusal window.
const cutoverGateWindow = 120 * time.Second

// quiesceMargin pads the write-lease wait: clocks and status propagation are
// not instant, and freezing before every router's lease has truly expired
// would let a straggler write land after the barrier.
const quiesceMargin = 5 * time.Second

// reconcileReadyToCutover re-validates every identity and opens the cutover:
// persist the gate deadline (the RoutingController compiles the gate and
// pins this reshard with a finalizer), then enter CuttingOver.
func (r *PgShardReshardReconciler) reconcileReadyToCutover(
	ctx context.Context, reshard *pgshardv1alpha1.PgShardReshard,
) (ctrl.Result, error) {
	source, held, res, err := r.resolveSource(ctx, reshard)
	if err != nil {
		return ctrl.Result{}, err
	}
	if held {
		return res, nil
	}
	_ = source
	deadline := metav1.NewTime(time.Now().Add(cutoverGateWindow))
	reshard.Status.CutoverGateDeadline = &deadline
	reshard.Status.Phase = pgshardv1alpha1.ReshardCuttingOver
	setReshardCondition(reshard, reshardCutoverCondition, metav1.ConditionFalse,
		"Gating", "cutover gate requested; waiting for routers to quiesce")
	return ctrl.Result{Requeue: true}, nil
}

// reconcileCuttingOver drives gate → quiesce → freeze → barrier → switch,
// each step gated on PERSISTED status so any crash resumes exactly where it
// left off, and the deadline rolls an uncommitted cutover back.
func (r *PgShardReshardReconciler) reconcileCuttingOver(
	ctx context.Context, reshard *pgshardv1alpha1.PgShardReshard,
) (ctrl.Result, error) {
	var cluster pgshardv1alpha1.PgShardCluster
	clusterKey := client.ObjectKey{Namespace: reshard.Namespace, Name: reshard.Spec.ClusterRef}
	if err := r.Get(ctx, clusterKey, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return r.holdCutover(reshard, "ClusterNotFound",
				fmt.Sprintf("cluster %q not found", reshard.Spec.ClusterRef))
		}
		return ctrl.Result{}, err
	}
	if reshard.Status.ClusterUID != string(cluster.UID) {
		r.fail(reshard, reshardCutoverCondition, "ClusterReplaced",
			fmt.Sprintf("cluster %q is not the object this reshard was validated against", cluster.Name))
		return ctrl.Result{}, nil
	}

	var source pgshardv1alpha1.PgShardShard
	sourceKey := client.ObjectKey{Namespace: reshard.Namespace, Name: reshard.Spec.SourceShard}
	if err := r.Get(ctx, sourceKey, &source); err != nil {
		if apierrors.IsNotFound(err) {
			return r.holdCutover(reshard, "SourceShardMissing",
				fmt.Sprintf("source shard %q not found", reshard.Spec.SourceShard))
		}
		return ctrl.Result{}, err
	}
	if reshard.Status.SourceShardUID != string(source.UID) {
		r.fail(reshard, reshardCutoverCondition, "SourceReplaced",
			fmt.Sprintf("source shard %q is not the object this reshard was validated against", source.Name))
		return ctrl.Result{}, nil
	}

	if reshard.Status.SwitchCommitted {
		return r.completeSwitch(ctx, reshard, &cluster, &source)
	}

	// Pre-commit: the deadline bounds the write-refusal window. Expiry rolls
	// back to CatchingUp — clearing the gate field withdraws the gate
	// (SwitchCommitted is false, so the RoutingController allows it) and the
	// old topology resumes.
	if reshard.Status.CutoverGateDeadline != nil &&
		time.Now().After(reshard.Status.CutoverGateDeadline.Time) {
		reshard.Status.CutoverGateDeadline = nil
		reshard.Status.CutoverGateObservedAt = nil
		reshard.Status.CutoverFrozenLSN = 0
		// A retry is a NEW attempt: its freeze must emit a FRESH barrier.
		// Replaying this attempt's journal id would hand back a position the
		// workflows already acknowledged, committing the next cutover
		// without proving its own quiesce point was decoded.
		reshard.Status.CutoverAttempt++
		reshard.Status.Phase = pgshardv1alpha1.ReshardCatchingUp
		setReshardCondition(reshard, reshardCutoverCondition, metav1.ConditionFalse,
			"RolledBack", "the cutover gate deadline expired before the switch committed; retrying from CatchingUp")
		return ctrl.Result{Requeue: true}, nil
	}

	// Step 1: observe OUR gate published in routing.
	var routing pgshardv1alpha1.PgShardRouting
	if err := r.Get(ctx, clusterKey, &routing); err != nil {
		if apierrors.IsNotFound(err) {
			return r.holdCutover(reshard, "RoutingMissing", "no compiled routing exists yet")
		}
		return ctrl.Result{}, err
	}
	gateID := "reshard-" + reshard.Name
	if !slices.ContainsFunc(routing.Spec.Gates, func(g pgshardv1alpha1.RoutingGate) bool {
		return g.ID == gateID
	}) {
		return r.holdCutover(reshard, "GateUnpublished",
			"waiting for the routing compiler to publish the cutover gate")
	}
	if reshard.Status.CutoverGateObservedAt == nil {
		now := metav1.Now()
		reshard.Status.CutoverGateObservedAt = &now
		setReshardCondition(reshard, reshardCutoverCondition, metav1.ConditionFalse,
			"Quiescing", "gate published; waiting for router write leases to expire")
		return ctrl.Result{Requeue: true}, nil
	}

	// Step 2: quiesce by lease expiry. Routers refuse the gated epoch and
	// stop renewing their write leases (the #94/#96 fail-safe); after
	// writeLeaseSeconds plus margin no router accepts writes.
	quiesceAt := reshard.Status.CutoverGateObservedAt.Add(
		time.Duration(routing.Spec.WriteLeaseSeconds)*time.Second + quiesceMargin)
	if now := time.Now(); now.Before(quiesceAt) {
		setReshardCondition(reshard, reshardCutoverCondition, metav1.ConditionFalse,
			"Quiescing", fmt.Sprintf("router write leases expire at %s", quiesceAt.Format(time.RFC3339)))
		return ctrl.Result{RequeueAfter: quiesceAt.Sub(now) + time.Second}, nil
	}

	// Step 3: freeze — emit the journal barrier in the source database.
	// EmitJournal is idempotent by id with an exact-payload match, so the
	// spec built here must be DETERMINISTIC.
	if reshard.Status.CutoverFrozenLSN == 0 {
		return r.freezeSource(ctx, reshard, &source)
	}

	// Step 4: every target workflow must acknowledge the barrier.
	acked, held, res, err := r.barrierAcknowledged(ctx, reshard)
	if err != nil {
		return ctrl.Result{}, err
	}
	if held {
		return res, nil
	}
	if !acked {
		return r.holdCutover(reshard, "AwaitingBarrier",
			fmt.Sprintf("waiting for every target workflow to acknowledge the barrier %#x",
				uint64(reshard.Status.CutoverFrozenLSN)))
	}

	// Step 5: the point of no return, PERSISTED before any flip. The
	// RoutingController refuses ungated routing from here until the source
	// stops serving, so no crash ordering can re-admit source writes.
	reshard.Status.SwitchCommitted = true
	setReshardCondition(reshard, reshardCutoverCondition, metav1.ConditionFalse,
		"SwitchCommitted", "barrier acknowledged by every target; committing the switch")
	return ctrl.Result{Requeue: true}, nil
}

func (r *PgShardReshardReconciler) holdCutover(
	reshard *pgshardv1alpha1.PgShardReshard, reason, message string,
) (ctrl.Result, error) {
	setReshardCondition(reshard, reshardCutoverCondition, metav1.ConditionFalse, reason, message)
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// freezeSource emits the journal barrier (the #104 EmitJournal contract) on
// the source's verified primary and persists the returned position.
func (r *PgShardReshardReconciler) freezeSource(
	ctx context.Context,
	reshard *pgshardv1alpha1.PgShardReshard,
	source *pgshardv1alpha1.PgShardShard,
) (ctrl.Result, error) {
	sourcePod, sourceNode, err := r.primaryEndpoint(ctx, reshard.Namespace, source.Spec.NodeRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if sourcePod == nil || !databaseVerified(source, sourceNode, sourcePod) {
		return r.holdCutover(reshard, "SourceUnverified",
			fmt.Sprintf("source shard %q has no verified primary to freeze", source.Name))
	}
	agent, err := r.agentClient(sourcePod.Status.PodIP)
	if err != nil {
		return ctrl.Result{}, err
	}
	successors := make([]*pgshardv1.JournalSuccessor, 0, len(reshard.Status.TargetShards))
	for i, name := range reshard.Status.TargetShards {
		tr := reshard.Spec.TargetRanges[i]
		targetRange, err := toRange(tr)
		if err != nil {
			r.fail(reshard, reshardCutoverCondition, "InvalidTargetRange", err.Error())
			return ctrl.Result{}, nil
		}
		wire := &pgshardv1.KeyRange{Start: targetRange.Start()}
		if end, closed := targetRange.End(); closed {
			wire.End = &end
		}
		successors = append(successors, &pgshardv1.JournalSuccessor{
			Shard:    name,
			KeyRange: wire,
		})
	}
	resp, err := agent.EmitJournal(ctx, &pgshardv1.EmitJournalRequest{
		Journal: &pgshardv1.JournalEvent{
			SourceShard: source.Name,
			Successors:  successors,
		},
		Database: shardDatabaseName(source),
		// UID + attempt: a retried freeze WITHIN one attempt replays the
		// recorded barrier (idempotent), while a new attempt after rollback
		// emits a fresh one; a poisoned or mismatched id fails loudly.
		Id:           fmt.Sprintf("%s-%d", reshard.UID, reshard.Status.CutoverAttempt),
		TargetPodUid: string(sourcePod.UID),
	})
	if err != nil {
		switch grpcstatus.Code(err) {
		case codes.InvalidArgument, codes.FailedPrecondition:
			// A poisoned journal id or a payload mismatch cannot converge.
			r.fail(reshard, reshardCutoverCondition, "FreezeRejected", err.Error())
			return ctrl.Result{}, nil
		default:
			return r.holdCutover(reshard, "FreezeFailed", err.Error())
		}
	}
	lsn := resp.GetLsn().GetValue()
	if lsn == 0 {
		return r.holdCutover(reshard, "FreezeFailed", "the agent returned no barrier position")
	}
	reshard.Status.CutoverFrozenLSN = int64(lsn)
	setReshardCondition(reshard, reshardCutoverCondition, metav1.ConditionFalse,
		"Frozen", fmt.Sprintf("barrier emitted at %#x; awaiting target acknowledgements", lsn))
	return ctrl.Result{Requeue: true}, nil
}

// barrierAcknowledged checks every target workflow decoded the barrier:
// journal_lsn >= the frozen position, read through the same verified
// placement chain as seeding.
func (r *PgShardReshardReconciler) barrierAcknowledged(
	ctx context.Context,
	reshard *pgshardv1alpha1.PgShardReshard,
) (acked, held bool, res ctrl.Result, err error) {
	frozen := uint64(reshard.Status.CutoverFrozenLSN)
	for i, targetName := range reshard.Status.TargetShards {
		var target pgshardv1alpha1.PgShardShard
		if err := r.Get(ctx,
			client.ObjectKey{Namespace: reshard.Namespace, Name: targetName}, &target); err != nil {
			if apierrors.IsNotFound(err) {
				res, _ := r.holdCutover(reshard, "TargetShardMissing",
					fmt.Sprintf("target shard %q not found", targetName))
				return false, true, res, nil
			}
			return false, false, ctrl.Result{}, err
		}
		targetPod, targetNode, err := r.primaryEndpoint(ctx, reshard.Namespace, target.Spec.NodeRef)
		if err != nil {
			return false, false, ctrl.Result{}, err
		}
		if targetPod == nil || !databaseVerified(&target, targetNode, targetPod) {
			res, _ := r.holdCutover(reshard, "TargetUnready",
				fmt.Sprintf("target shard %q has no verified primary", targetName))
			return false, true, res, nil
		}
		agent, err := r.agentClient(targetPod.Status.PodIP)
		if err != nil {
			return false, false, ctrl.Result{}, err
		}
		id := seedWorkflowID(reshard, i)
		status, err := r.workflowStatus(ctx, agent, id)
		if err != nil {
			res, _ := r.holdCutover(reshard, "WorkflowStatusUnavailable", err.Error())
			return false, true, res, nil
		}
		if status.GetPhase() != pgshardv1.WorkflowPhase_WORKFLOW_PHASE_STREAMING {
			res, _ := r.holdCutover(reshard, "WorkflowNotStreaming",
				fmt.Sprintf("workflow %s is not streaming; the barrier cannot converge", id))
			return false, true, res, nil
		}
		if status.GetJournalLsn().GetValue() < frozen {
			return false, false, ctrl.Result{}, nil
		}
	}
	return true, false, ctrl.Result{}, nil
}

// completeSwitch runs AFTER the persisted point of no return: flip the
// serving set, observe it compiled, then withdraw the gate and land in
// SwitchedForward. Every step is idempotent; the RoutingController's
// SwitchCommitted fence keeps writes off the old source throughout.
func (r *PgShardReshardReconciler) completeSwitch(
	ctx context.Context,
	reshard *pgshardv1alpha1.PgShardReshard,
	cluster *pgshardv1alpha1.PgShardCluster,
	source *pgshardv1alpha1.PgShardShard,
) (ctrl.Result, error) {
	// Flip the targets serving first, then hide the source: the compiler
	// refuses every intermediate (non-partitioning) set, so routing holds
	// the last good GATED view until the full flip lands.
	for _, targetName := range reshard.Status.TargetShards {
		var target pgshardv1alpha1.PgShardShard
		if err := r.Get(ctx,
			client.ObjectKey{Namespace: reshard.Namespace, Name: targetName}, &target); err != nil {
			return ctrl.Result{}, err
		}
		if !target.Spec.Serving {
			target.Spec.Serving = true
			if err := r.Update(ctx, &target); err != nil {
				return ctrl.Result{}, err
			}
		}
	}
	if source.Spec.Serving {
		source.Spec.Serving = false
		if err := r.Update(ctx, source); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Observe the switched serving set actually compiled before withdrawing
	// the gate: the withdrawal must never race the compiler into publishing
	// the pre-switch topology ungated.
	var routing pgshardv1alpha1.PgShardRouting
	if err := r.Get(ctx,
		client.ObjectKey{Namespace: reshard.Namespace, Name: cluster.Name}, &routing); err != nil {
		return ctrl.Result{}, err
	}
	if !switchedSetCompiled(&routing, source.Name, reshard.Status.TargetShards) {
		return r.holdCutover(reshard, "AwaitingSwitchedRouting",
			"waiting for the routing compiler to publish the switched serving set")
	}

	reshard.Status.CutoverGateDeadline = nil
	reshard.Status.Phase = pgshardv1alpha1.ReshardSwitchedForward
	setReshardCondition(reshard, reshardCutoverCondition, metav1.ConditionTrue,
		"SwitchedForward", "targets serve the key range; the source is hidden")
	return ctrl.Result{Requeue: true}, nil
}

// switchedSetCompiled reports whether routing shows every target serving and
// the source hidden.
func switchedSetCompiled(
	routing *pgshardv1alpha1.PgShardRouting, sourceName string, targets []string,
) bool {
	state := map[string]pgshardv1alpha1.RoutingShardState{}
	for _, sh := range routing.Spec.Shards {
		state[sh.Name] = sh.State
	}
	if state[sourceName] != pgshardv1alpha1.RoutingHidden {
		return false
	}
	for _, t := range targets {
		if state[t] != pgshardv1alpha1.RoutingServing {
			return false
		}
	}
	return true
}
