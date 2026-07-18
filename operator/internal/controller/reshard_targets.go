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

// reconcileProvisioningTargets creates the reshard's non-serving target shards —
// and, for dedicated placement, a PgShardNode per target — all owned by the
// PgShardReshard so a rollback that deletes the reshard cascades them away.
//
// It deliberately does not wait for the target databases to come up or advance
// to Seeding: the target set is Serving:false, so the routing compiler keeps it
// hidden and the source keeps serving until cutover; the shard and node
// controllers provision the databases asynchronously; and Seeding itself needs
// the (not-yet-built) seeding engine. The reshard therefore rests at
// ProvisioningTargets once its targets exist.
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

	rendered, err := pgconfig.Render(clusterRenderInputs(&cluster))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("rendering configuration: %w", err)
	}

	// Build the hidden target shards and compute their placement.
	targets := make([]pgshardv1alpha1.PgShardShard, 0, len(reshard.Spec.TargetRanges))
	names := make([]string, 0, len(reshard.Spec.TargetRanges))
	for _, tr := range reshard.Spec.TargetRanges {
		name := shardName(cluster.Name, tr.Start, tr.End)
		targets = append(targets,
			shardFor(&cluster, name, tr, pgshardv1alpha1.ShardRoleData, rendered, false))
		names = append(names, name)
	}

	mode, _ := placementOf(&cluster)
	assignment, owned, _, err := desiredPlacement(&cluster, rendered, targets)
	if err != nil {
		// A placement error is derived from the immutable cluster plus the target
		// ranges (e.g. a node name that is too long), so it is permanent.
		r.fail(reshard, reshardTargetsProvisionedCondition, "PlacementFailed", err.Error())
		return ctrl.Result{}, nil
	}

	// Dedicated placement gives each target its own node; create those, owned by
	// the reshard. In shared placement the single shared node belongs to the
	// cluster — reference it via NodeRef but never create it here, or we would
	// fight the cluster controller over its ownership.
	if mode != pgshardv1alpha1.PlacementShared {
		for i := range owned {
			requeue, err := r.ensureReshardNode(ctx, reshard, &owned[i], rendered)
			if err != nil {
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
		if err != nil {
			return ctrl.Result{}, err
		}
		if requeue {
			return ctrl.Result{Requeue: true}, nil
		}
	}

	reshard.Status.TargetShards = names
	setReshardCondition(reshard, reshardTargetsProvisionedCondition, metav1.ConditionTrue,
		"TargetsProvisioned",
		"target shards created (non-serving); seeding and cutover are later slices")
	return ctrl.Result{}, nil
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
			return false, fmt.Errorf(
				"target node %s exists but is not controlled by reshard %s", node.Name, reshard.Name)
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
		// Validate the derived config-map name before Create so an impossible name
		// fails fast instead of leaving a shard whose config map can never exist.
		if _, err := configMapName(shard.Name); err != nil {
			return false, err
		}
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
			return false, fmt.Errorf(
				"target shard %s exists but is not controlled by reshard %s", shard.Name, reshard.Name)
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
