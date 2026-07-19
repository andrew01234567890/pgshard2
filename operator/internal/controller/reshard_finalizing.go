package controller

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	corev1 "k8s.io/api/core/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
)

const reshardFinalizedCondition = "Finalized"

// reconcileSwitchedForward re-parents the now-serving target shards (and, for
// dedicated placement, their nodes and config maps) from the reshard to the
// cluster, then advances to Finalizing. This MUST happen before the reshard is
// completed or deleted: the targets are reshard-owned so that a mid-seed
// rollback cascade-deletes them, but once they serve traffic the reshard must
// no longer own them or completing/deleting it would destroy live shards.
func (r *PgShardReshardReconciler) reconcileSwitchedForward(
	ctx context.Context, reshard *pgshardv1alpha1.PgShardReshard,
) (ctrl.Result, error) {
	// Pin the cluster identity: never re-parent a serving shard onto a
	// replacement cluster of the same name.
	var cluster pgshardv1alpha1.PgShardCluster
	clusterKey := client.ObjectKey{Namespace: reshard.Namespace, Name: reshard.Spec.ClusterRef}
	if err := r.Get(ctx, clusterKey, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return r.holdFinalize(reshard, "ClusterNotFound",
				fmt.Sprintf("cluster %q not found", reshard.Spec.ClusterRef))
		}
		return ctrl.Result{}, err
	}
	if reshard.Status.ClusterUID != string(cluster.UID) {
		r.fail(reshard, reshardFinalizedCondition, "ClusterReplaced",
			fmt.Sprintf("cluster %q is not the object this reshard was validated against", cluster.Name))
		return ctrl.Result{}, nil
	}

	changed := false
	for _, targetName := range reshard.Status.TargetShards {
		var target pgshardv1alpha1.PgShardShard
		if err := r.Get(ctx,
			client.ObjectKey{Namespace: reshard.Namespace, Name: targetName}, &target); err != nil {
			if apierrors.IsNotFound(err) {
				return r.holdFinalize(reshard, "TargetMissing",
					fmt.Sprintf("target shard %q not found", targetName))
			}
			return ctrl.Result{}, err
		}
		// completeSwitch flips targets to serving before this phase; a target
		// that is not serving means the switch did not fully land.
		if !target.Spec.Serving {
			return r.holdFinalize(reshard, "TargetNotServing",
				fmt.Sprintf("target shard %q is not serving yet", targetName))
		}
		did, err := r.reparentToCluster(ctx, reshard, &cluster, &target)
		if err != nil {
			return ctrl.Result{}, err
		}
		changed = changed || did

		// The reshard-owned config map, and (dedicated placement only) the
		// reshard-owned node, share the target's name. reparentToCluster no-ops
		// on objects this reshard does not own, so unconditional calls are safe.
		cmName, err := configMapName(targetName)
		if err != nil {
			return ctrl.Result{}, err
		}
		did, err = r.reparentNamed(ctx, reshard, &cluster, cmName, &corev1.ConfigMap{})
		if err != nil {
			return ctrl.Result{}, err
		}
		changed = changed || did
		did, err = r.reparentNamed(ctx, reshard, &cluster, targetName, &pgshardv1alpha1.PgShardNode{})
		if err != nil {
			return ctrl.Result{}, err
		}
		changed = changed || did
	}
	if changed {
		// Re-observe the re-parented set before advancing.
		return ctrl.Result{Requeue: true}, nil
	}

	reshard.Status.Phase = pgshardv1alpha1.ReshardFinalizing
	setReshardCondition(reshard, reshardFinalizedCondition, metav1.ConditionFalse,
		"Finalizing", "targets re-parented to the cluster; finalizing the source")
	return ctrl.Result{Requeue: true}, nil
}

// reparentNamed fetches the named object into obj and re-parents it; a missing
// object (e.g. no dedicated node under shared placement) is not an error.
func (r *PgShardReshardReconciler) reparentNamed(
	ctx context.Context,
	reshard *pgshardv1alpha1.PgShardReshard,
	cluster *pgshardv1alpha1.PgShardCluster,
	name string,
	obj client.Object,
) (bool, error) {
	if err := r.Get(ctx, client.ObjectKey{Namespace: reshard.Namespace, Name: name}, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return r.reparentToCluster(ctx, reshard, cluster, obj)
}

// reparentToCluster moves obj's controller ownership from the reshard to the
// cluster when the reshard still owns it. Returns changed=true when it wrote.
// Idempotent: once re-parented, the reshard is no longer an owner and it no-ops.
func (r *PgShardReshardReconciler) reparentToCluster(
	ctx context.Context,
	reshard *pgshardv1alpha1.PgShardReshard,
	cluster *pgshardv1alpha1.PgShardCluster,
	obj client.Object,
) (bool, error) {
	owned, err := controllerutil.HasOwnerReference(obj.GetOwnerReferences(), reshard, r.Scheme)
	if err != nil {
		return false, err
	}
	if !owned {
		return false, nil
	}
	if err := controllerutil.RemoveOwnerReference(reshard, obj, r.Scheme); err != nil {
		return false, err
	}
	if err := controllerutil.SetControllerReference(cluster, obj, r.Scheme); err != nil {
		return false, err
	}
	if err := r.Update(ctx, obj); err != nil {
		return false, err
	}
	return true, nil
}

// reconcileFinalizing stops every target's forward workflow (dropping its slot
// on the source so no replication slot leaks WAL), releases the source cutover
// claim, and drops the cleanup finalizer, then completes. The source shard is
// RETAINED — it stays hidden and fenced (a committed switch is never
// un-fenced); its physical decommission, and reverse-replication rollback
// within the rollback window, are a later slice.
func (r *PgShardReshardReconciler) reconcileFinalizing(
	ctx context.Context, reshard *pgshardv1alpha1.PgShardReshard,
) (ctrl.Result, error) {
	for i, targetName := range reshard.Status.TargetShards {
		var target pgshardv1alpha1.PgShardShard
		if err := r.Get(ctx,
			client.ObjectKey{Namespace: reshard.Namespace, Name: targetName}, &target); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return ctrl.Result{}, err
		}
		targetPod, targetNode, err := r.primaryEndpoint(ctx, reshard.Namespace, target.Spec.NodeRef)
		if err != nil {
			return ctrl.Result{}, err
		}
		if targetPod == nil || !databaseVerified(&target, targetNode, targetPod) {
			// Hold rather than complete with a slot left leaking WAL on the
			// retained source: the workflow's slot lives on the source and is
			// dropped through the target agent that owns it.
			return r.holdFinalize(reshard, "TargetUnverified",
				fmt.Sprintf("cannot reach target %q to stop its workflow", targetName))
		}
		agent, err := r.agentClient(targetPod.Status.PodIP)
		if err != nil {
			return ctrl.Result{}, err
		}
		if _, err := agent.StopWorkflow(ctx, &pgshardv1.StopWorkflowRequest{
			Id:       seedWorkflowID(reshard, i),
			DropSlot: true,
		}); err != nil {
			if grpcstatus.Code(err) == codes.NotFound {
				continue
			}
			return r.holdFinalize(reshard, "StopWorkflowFailed", err.Error())
		}
	}

	if source := r.sourceForCleanup(ctx, reshard); source != nil {
		if err := r.releaseSourceClaim(ctx, reshard, source); err != nil {
			return ctrl.Result{}, err
		}
	}
	if controllerutil.RemoveFinalizer(reshard, cutoverClaimFinalizer) {
		if err := r.Update(ctx, reshard); err != nil {
			return ctrl.Result{}, err
		}
	}

	reshard.Status.Phase = pgshardv1alpha1.ReshardCompleted
	setReshardCondition(reshard, reshardFinalizedCondition, metav1.ConditionTrue,
		"Completed", "targets serve and are owned by the cluster; the source is retained fenced")
	return ctrl.Result{}, nil
}

func (r *PgShardReshardReconciler) holdFinalize(
	reshard *pgshardv1alpha1.PgShardReshard, reason, message string,
) (ctrl.Result, error) {
	setReshardCondition(reshard, reshardFinalizedCondition, metav1.ConditionFalse, reason, message)
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}
