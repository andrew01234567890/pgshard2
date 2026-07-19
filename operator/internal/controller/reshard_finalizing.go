package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
)

const reshardFinalizedCondition = "Finalized"

// reconcileSwitchedForward re-parents the now-serving target shards from the
// reshard to the cluster, then advances to Finalizing. Targets are created
// reshard-owned so a mid-seed rollback cascade-deletes them; but once they
// serve traffic the reshard must no longer own them, or completing OR deleting
// it would let Kubernetes GC destroy live shards.
func (r *PgShardReshardReconciler) reconcileSwitchedForward(
	ctx context.Context, reshard *pgshardv1alpha1.PgShardReshard,
) (ctrl.Result, error) {
	cluster, res, ok, err := r.finalizeCluster(ctx, reshard)
	if err != nil || !ok {
		return res, err
	}
	if res, done, err := r.reparentTargets(ctx, reshard, cluster); err != nil || !done {
		return res, err
	}
	reshard.Status.Phase = pgshardv1alpha1.ReshardFinalizing
	setReshardCondition(reshard, reshardFinalizedCondition, metav1.ConditionFalse,
		"Finalizing", "targets re-parented to the cluster; finalizing the source")
	return ctrl.Result{Requeue: true}, nil
}

// finalizeCluster fetches the cluster and pins its identity: a re-parent must
// never move a serving shard onto a same-named replacement cluster. ok=false
// with a result means the caller returns; a UID mismatch is terminal.
func (r *PgShardReshardReconciler) finalizeCluster(
	ctx context.Context, reshard *pgshardv1alpha1.PgShardReshard,
) (*pgshardv1alpha1.PgShardCluster, ctrl.Result, bool, error) {
	var cluster pgshardv1alpha1.PgShardCluster
	if err := r.Get(ctx,
		client.ObjectKey{Namespace: reshard.Namespace, Name: reshard.Spec.ClusterRef}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			res, _ := r.holdFinalize(reshard, "ClusterNotFound",
				fmt.Sprintf("cluster %q not found", reshard.Spec.ClusterRef))
			return nil, res, false, nil
		}
		return nil, ctrl.Result{}, false, err
	}
	if reshard.Status.ClusterUID != string(cluster.UID) {
		r.fail(reshard, reshardFinalizedCondition, "ClusterReplaced",
			fmt.Sprintf("cluster %q is not the object this reshard was validated against", cluster.Name))
		return nil, ctrl.Result{}, false, nil
	}
	return &cluster, ctrl.Result{}, true, nil
}

// reparentTargets moves every target shard (and, for dedicated placement, its
// node and config map) from the reshard to the cluster. Targets are re-derived
// from the IMMUTABLE spec ranges, not mutable status, and each is validated
// against the cluster, its range, and role before its ownership is touched.
// done=false with a result means not finished (hold or requeue); the source
// shard is never re-parented (it is retained, hidden, and fenced).
func (r *PgShardReshardReconciler) reparentTargets(
	ctx context.Context,
	reshard *pgshardv1alpha1.PgShardReshard,
	cluster *pgshardv1alpha1.PgShardCluster,
) (ctrl.Result, bool, error) {
	changed := false
	for _, tr := range reshard.Spec.TargetRanges {
		name := shardName(cluster.Name, tr.Start, tr.End)
		var target pgshardv1alpha1.PgShardShard
		if err := r.Get(ctx,
			client.ObjectKey{Namespace: reshard.Namespace, Name: name}, &target); err != nil {
			if apierrors.IsNotFound(err) {
				res, _ := r.holdFinalize(reshard, "TargetMissing",
					fmt.Sprintf("target shard %q not found", name))
				return res, false, nil
			}
			return ctrl.Result{}, false, err
		}
		if target.Spec.ClusterRef != cluster.Name ||
			target.Spec.KeyRange.Start != tr.Start || target.Spec.KeyRange.End != tr.End ||
			target.Spec.Role == pgshardv1alpha1.ShardRoleSystem {
			res, _ := r.holdFinalize(reshard, "TargetMismatch",
				fmt.Sprintf("target shard %q does not match the reshard's cluster/range/role", name))
			return res, false, nil
		}
		if !target.Spec.Serving {
			res, _ := r.holdFinalize(reshard, "TargetNotServing",
				fmt.Sprintf("target shard %q is not serving yet", name))
			return res, false, nil
		}

		did, res, ok, err := r.reparentOne(ctx, reshard, cluster, &target, name)
		if err != nil || !ok {
			return res, false, err
		}
		changed = changed || did

		// A dedicated-placement target has its own reshard-owned node whose pod
		// mounts its config map; both must survive GC. A shared-placement target
		// has no per-target node — its data lives on the cluster's shared node —
		// so its per-target config map is vestigial.
		var node pgshardv1alpha1.PgShardNode
		switch getErr := r.Get(ctx,
			client.ObjectKey{Namespace: reshard.Namespace, Name: name}, &node); {
		case apierrors.IsNotFound(getErr):
			// Shared placement: best-effort re-parent of a vestigial config map.
			did, res, ok, err = r.reparentConfigMap(ctx, reshard, cluster, name, true)
		case getErr != nil:
			return ctrl.Result{}, false, getErr
		default:
			did, res, ok, err = r.reparentOne(ctx, reshard, cluster, &node, name)
			if err != nil || !ok {
				return res, false, err
			}
			changed = changed || did
			// A dedicated node's config map is load-bearing: require it.
			did, res, ok, err = r.reparentConfigMap(ctx, reshard, cluster, name, false)
		}
		if err != nil || !ok {
			return res, false, err
		}
		changed = changed || did
	}
	if changed {
		return ctrl.Result{Requeue: true}, false, nil
	}
	return ctrl.Result{}, true, nil
}

// reparentConfigMap re-parents a target's config map. When tolerateMissing is
// false (a dedicated node depends on it) a missing map holds; when true (a
// vestigial shared-placement map) it is skipped.
func (r *PgShardReshardReconciler) reparentConfigMap(
	ctx context.Context,
	reshard *pgshardv1alpha1.PgShardReshard,
	cluster *pgshardv1alpha1.PgShardCluster,
	targetName string,
	tolerateMissing bool,
) (bool, ctrl.Result, bool, error) {
	cmName, err := configMapName(targetName)
	if err != nil {
		return false, ctrl.Result{}, false, err
	}
	var cm corev1.ConfigMap
	if getErr := r.Get(ctx,
		client.ObjectKey{Namespace: reshard.Namespace, Name: cmName}, &cm); getErr != nil {
		if apierrors.IsNotFound(getErr) {
			if tolerateMissing {
				return false, ctrl.Result{}, true, nil
			}
			res, _ := r.holdFinalize(reshard, "ConfigMapMissing",
				fmt.Sprintf("config map %q for target %q not found", cmName, targetName))
			return false, res, false, nil
		}
		return false, ctrl.Result{}, false, getErr
	}
	return r.reparentOne(ctx, reshard, cluster, &cm, targetName)
}

// reparentOne moves obj's controller ownership from the reshard to the cluster.
// It authorizes by controller UID (metav1.IsControlledBy), not GVK+name, so a
// same-named reshard replacement cannot match: already-cluster-controlled is a
// no-op; reshard-controlled is re-parented; anything else (foreign or ownerless)
// holds rather than being silently accepted as done.
func (r *PgShardReshardReconciler) reparentOne(
	ctx context.Context,
	reshard *pgshardv1alpha1.PgShardReshard,
	cluster *pgshardv1alpha1.PgShardCluster,
	obj client.Object,
	targetName string,
) (changed bool, res ctrl.Result, ok bool, err error) {
	if metav1.IsControlledBy(obj, cluster) {
		return false, ctrl.Result{}, true, nil
	}
	if !metav1.IsControlledBy(obj, reshard) {
		hold, _ := r.holdFinalize(reshard, "TargetForeign",
			fmt.Sprintf("%s for target %q is not controlled by this reshard", obj.GetObjectKind().GroupVersionKind().Kind, targetName))
		return false, hold, false, nil
	}
	if err := controllerutil.RemoveOwnerReference(reshard, obj, r.Scheme); err != nil {
		return false, ctrl.Result{}, false, err
	}
	if err := controllerutil.SetControllerReference(cluster, obj, r.Scheme); err != nil {
		return false, ctrl.Result{}, false, err
	}
	if err := r.Update(ctx, obj); err != nil {
		return false, ctrl.Result{}, false, err
	}
	return true, ctrl.Result{}, true, nil
}

// reconcileFinalizing stops each target's forward workflow, releases the source
// cutover claim, drops the cleanup finalizer, and completes. The source shard
// is RETAINED — hidden and still fenced (a committed switch is never
// un-fenced). Dropping the source-side replication slots and physically
// decommissioning the source (and reverse-replication rollback within the
// rollback window) are a later slice: the slot-drop RPC is source-owned and not
// yet implemented, so the workflow is stopped WITHOUT drop_slot.
func (r *PgShardReshardReconciler) reconcileFinalizing(
	ctx context.Context, reshard *pgshardv1alpha1.PgShardReshard,
) (ctrl.Result, error) {
	for i, tr := range reshard.Spec.TargetRanges {
		name := shardName(reshard.Spec.ClusterRef, tr.Start, tr.End)
		var target pgshardv1alpha1.PgShardShard
		if err := r.Get(ctx,
			client.ObjectKey{Namespace: reshard.Namespace, Name: name}, &target); err != nil {
			if apierrors.IsNotFound(err) {
				return r.holdFinalize(reshard, "TargetMissing",
					fmt.Sprintf("serving target %q vanished before finalization", name))
			}
			return ctrl.Result{}, err
		}
		targetPod, targetNode, err := r.primaryEndpoint(ctx, reshard.Namespace, target.Spec.NodeRef)
		if err != nil {
			return ctrl.Result{}, err
		}
		if targetPod == nil || !databaseVerified(&target, targetNode, targetPod) {
			return r.holdFinalize(reshard, "TargetUnverified",
				fmt.Sprintf("cannot reach target %q to stop its workflow", name))
		}
		agent, err := r.agentClient(targetPod.Status.PodIP)
		if err != nil {
			return ctrl.Result{}, err
		}
		if _, err := agent.StopWorkflow(ctx, &pgshardv1.StopWorkflowRequest{
			Id: seedWorkflowID(reshard, i),
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
