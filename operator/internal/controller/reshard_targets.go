/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard2/operator/internal/pgconfig"
)

// reshardCollision reports that an object the reshard would create already exists
// under a different owner. It is recoverable once the conflicting object is
// removed, so it is surfaced as a condition and retried rather than being
// treated as a terminal failure or a bare controller error.
type reshardCollision struct{ msg string }

func (e *reshardCollision) Error() string { return e.msg }

// reconcileProvisioningTargets creates the reshard's non-serving target shards —
// and, for dedicated placement, a PgShardNode per target — all owned by the
// PgShardReshard.
//
// It deliberately does not wait for the target databases to come up or advance
// to Seeding: the target set is Serving:false, so the routing compiler keeps it
// hidden and the source keeps serving until cutover; the shard and node
// controllers provision the databases asynchronously; and Seeding itself needs
// the (not-yet-built) seeding engine. The reshard therefore rests at
// ProvisioningTargets once its targets exist.
//
// Rollback scope: deleting the reshard while it rests here cascade-deletes the
// Kubernetes objects it owns (target shards, dedicated nodes, config maps, and
// the nodes' pods). It does NOT reclaim the retained PVCs (node PVCs carry no
// owner reference, by design, for data safety) or drop the target Postgres
// databases (there is no drop-on-delete yet). Reclaiming that physical storage
// is the decommission/RollingBack slice's job, not owner-reference GC.
func (r *PgShardReshardReconciler) reconcileProvisioningTargets(
	ctx context.Context, reshard *pgshardv1alpha1.PgShardReshard,
) (ctrl.Result, error) {
	var cluster pgshardv1alpha1.PgShardCluster
	clusterKey := client.ObjectKey{Namespace: reshard.Namespace, Name: reshard.Spec.ClusterRef}
	if err := r.Get(ctx, clusterKey, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			setReshardCondition(reshard, reshardTargetsProvisionedCondition, metav1.ConditionFalse,
				"ClusterNotFound", fmt.Sprintf("cluster %q not found yet", reshard.Spec.ClusterRef))
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	// The cluster was pinned at Validating; a replacement under the same
	// name would combine the validated source with an unvalidated cluster's
	// hash function and configuration.
	if reshard.Status.ClusterUID != string(cluster.UID) {
		r.fail(reshard, reshardTargetsProvisionedCondition, "ClusterReplaced",
			fmt.Sprintf("cluster %q is not the object this reshard was validated against", cluster.Name))
		return ctrl.Result{}, nil
	}
	rendered, err := pgconfig.Render(clusterRenderInputs(&cluster))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("rendering configuration: %w", err)
	}

	// Build the hidden target shards and compute their placement.
	targets := make([]pgshardv1alpha1.PgShardShard, 0, len(reshard.Spec.TargetRanges))
	names := make([]string, 0, len(reshard.Spec.TargetRanges))
	for _, tr := range reshard.Spec.TargetRanges {
		name := shardName(cluster.Name, tr.Start, tr.End)
		// A shard name is also its Postgres database name; one PostgreSQL cannot
		// truncate is permanent (derived from the immutable cluster + ranges), so
		// fail now rather than let the shard controller record InvalidName forever.
		// Dedicated placement also catches this via nodeFor's 58-char node-name
		// cap, but shared placement does not build a node, so check it here for both.
		if len(name) > maxDatabaseNameBytes {
			r.fail(reshard, reshardTargetsProvisionedCondition, "InvalidTargetName",
				fmt.Sprintf("target shard database name %q exceeds %d bytes", name, maxDatabaseNameBytes))
			return ctrl.Result{}, nil
		}
		targets = append(targets,
			shardFor(&cluster, name, tr, pgshardv1alpha1.ShardRoleData, rendered, false))
		names = append(names, name)
	}

	mode, colocateWith := placementOf(&cluster)
	assignment, owned, external, err := desiredPlacement(&cluster, rendered, targets)
	if err != nil {
		// A placement error is derived from the immutable cluster plus the target
		// ranges (e.g. a node name that is too long), so it is permanent.
		r.fail(reshard, reshardTargetsProvisionedCondition, "PlacementFailed", err.Error())
		return ctrl.Result{}, nil
	}

	// Shared placement with colocateWith points every target at another cluster's
	// shared node. Validate that node's ownership before pointing databases at it
	// — the same check the cluster controller makes — so a foreign or stale
	// same-named node can never be adopted.
	if external != "" {
		if res, ok, err := r.validateColocation(ctx, reshard, colocateWith, external); err != nil || ok {
			return res, err
		}
	}

	// Dedicated placement gives each target its own node; create those, owned by
	// the reshard. In shared placement the single shared node belongs to the
	// cluster — reference it via NodeRef but never create it here, or we would
	// fight the cluster controller over its ownership.
	if mode != pgshardv1alpha1.PlacementShared {
		for i := range owned {
			requeue, err := r.ensureReshardNode(ctx, reshard, &owned[i], rendered)
			if res, ok := r.handleTargetErr(reshard, err); ok {
				return res, nil
			} else if err != nil {
				return ctrl.Result{}, err
			}
			if requeue {
				return ctrl.Result{Requeue: true}, nil
			}
		}
	}

	for i := range targets {
		targets[i].Spec.NodeRef = assignment[targets[i].Name]
		requeue, err := r.ensureReshardShard(ctx, reshard, &targets[i], rendered)
		if res, ok := r.handleTargetErr(reshard, err); ok {
			return res, nil
		} else if err != nil {
			return ctrl.Result{}, err
		}
		if requeue {
			return ctrl.Result{Requeue: true}, nil
		}
	}

	reshard.Status.TargetShards = names
	setReshardCondition(reshard, reshardTargetsProvisionedCondition, metav1.ConditionTrue,
		"TargetsProvisioned", "target shards created (non-serving)")
	// Requeue explicitly: a status-only write does not re-enqueue under
	// GenerationChangedPredicate.
	reshard.Status.Phase = pgshardv1alpha1.ReshardSeeding
	return ctrl.Result{Requeue: true}, nil
}

// handleTargetErr maps a name collision to a surfaced condition plus a delayed
// retry (recoverable once the conflict is cleared) rather than a bare, silent
// controller error. It returns ok=true when it handled err.
func (r *PgShardReshardReconciler) handleTargetErr(
	reshard *pgshardv1alpha1.PgShardReshard, err error,
) (ctrl.Result, bool) {
	var col *reshardCollision
	if errors.As(err, &col) {
		setReshardCondition(reshard, reshardTargetsProvisionedCondition, metav1.ConditionFalse,
			"TargetCollision", col.Error())
		return ctrl.Result{RequeueAfter: 30 * time.Second}, true
	}
	return ctrl.Result{}, false
}

// validateColocation confirms the colocateWith target node exists and is
// controlled by the target cluster. A missing cluster/node is transient (hold
// and retry); a same-named node owned by someone else is a misconfiguration we
// refuse to place databases on. It returns ok=true when it produced a result the
// caller should return.
func (r *PgShardReshardReconciler) validateColocation(
	ctx context.Context, reshard *pgshardv1alpha1.PgShardReshard, colocateWith, external string,
) (ctrl.Result, bool, error) {
	var host pgshardv1alpha1.PgShardCluster
	if err := r.Get(ctx, client.ObjectKey{Namespace: reshard.Namespace, Name: colocateWith}, &host); err != nil {
		if apierrors.IsNotFound(err) {
			setReshardCondition(reshard, reshardTargetsProvisionedCondition, metav1.ConditionFalse,
				"ColocationTargetMissing", fmt.Sprintf("colocation target cluster %q not found", colocateWith))
			return ctrl.Result{RequeueAfter: 10 * time.Second}, true, nil
		}
		return ctrl.Result{}, true, err
	}
	var node pgshardv1alpha1.PgShardNode
	if err := r.Get(ctx, client.ObjectKey{Namespace: reshard.Namespace, Name: external}, &node); err != nil {
		if apierrors.IsNotFound(err) {
			setReshardCondition(reshard, reshardTargetsProvisionedCondition, metav1.ConditionFalse,
				"ColocationNodeMissing", fmt.Sprintf("colocation node %q not found yet", external))
			return ctrl.Result{RequeueAfter: 10 * time.Second}, true, nil
		}
		return ctrl.Result{}, true, err
	}
	if !metav1.IsControlledBy(&node, &host) {
		setReshardCondition(reshard, reshardTargetsProvisionedCondition, metav1.ConditionFalse,
			"ColocationNodeForeign",
			fmt.Sprintf("colocation node %q is not controlled by cluster %q", external, colocateWith))
		return ctrl.Result{RequeueAfter: 30 * time.Second}, true, nil
	}
	return ctrl.Result{}, false, nil
}

// ensureReshardNode creates a reshard-owned PgShardNode (and its config map) if
// absent. A node with the same name that this reshard does not own — the
// cluster's own node, or a stale one — is a collision we refuse rather than
// adopt.
func (r *PgShardReshardReconciler) ensureReshardNode(
	ctx context.Context,
	reshard *pgshardv1alpha1.PgShardReshard,
	node *pgshardv1alpha1.PgShardNode,
	rendered pgconfig.Rendered,
) (requeue bool, err error) {
	existing := &pgshardv1alpha1.PgShardNode{}
	switch getErr := r.Get(ctx, client.ObjectKeyFromObject(node), existing); {
	case apierrors.IsNotFound(getErr):
		if err := controllerutil.SetControllerReference(reshard, node, r.Scheme); err != nil {
			return false, err
		}
		if err := r.Create(ctx, node); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return true, nil
			}
			return false, fmt.Errorf("creating target node %s: %w", node.Name, err)
		}
		return false, r.ensureReshardConfigMap(ctx, reshard, node.Name, rendered)
	case getErr != nil:
		return false, getErr
	default:
		if !metav1.IsControlledBy(existing, reshard) {
			return false, &reshardCollision{fmt.Sprintf(
				"target node %s exists but is not controlled by reshard %s", node.Name, reshard.Name)}
		}
		return false, r.ensureReshardConfigMap(ctx, reshard, node.Name, rendered)
	}
}

// ensureReshardShard creates a reshard-owned target PgShardShard (and its config
// map) if absent, refusing to adopt a same-named shard this reshard does not own.
func (r *PgShardReshardReconciler) ensureReshardShard(
	ctx context.Context,
	reshard *pgshardv1alpha1.PgShardReshard,
	shard *pgshardv1alpha1.PgShardShard,
	rendered pgconfig.Rendered,
) (requeue bool, err error) {
	existing := &pgshardv1alpha1.PgShardShard{}
	switch getErr := r.Get(ctx, client.ObjectKeyFromObject(shard), existing); {
	case apierrors.IsNotFound(getErr):
		if err := controllerutil.SetControllerReference(reshard, shard, r.Scheme); err != nil {
			return false, err
		}
		if err := r.Create(ctx, shard); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return true, nil
			}
			return false, fmt.Errorf("creating target shard %s: %w", shard.Name, err)
		}
		return false, r.ensureReshardConfigMap(ctx, reshard, shard.Name, rendered)
	case getErr != nil:
		return false, getErr
	default:
		if !metav1.IsControlledBy(existing, reshard) {
			return false, &reshardCollision{fmt.Sprintf(
				"target shard %s exists but is not controlled by reshard %s", shard.Name, reshard.Name)}
		}
		return false, r.ensureReshardConfigMap(ctx, reshard, shard.Name, rendered)
	}
}

// ensureReshardConfigMap materializes the rendered PostgreSQL parameters for a
// reshard-owned node or shard, mirroring the cluster controller's config map but
// owned by the reshard for cascade cleanup.
func (r *PgShardReshardReconciler) ensureReshardConfigMap(
	ctx context.Context,
	reshard *pgshardv1alpha1.PgShardReshard,
	objectName string,
	rendered pgconfig.Rendered,
) error {
	name, err := configMapName(objectName)
	if err != nil {
		return err
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: reshard.Namespace},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Data = map[string]string{"config-hash": rendered.ConfigHash}
		for k, v := range rendered.Parameters {
			cm.Data["param."+k] = v
		}
		return controllerutil.SetControllerReference(reshard, cm, r.Scheme)
	})
	return err
}
