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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard2/operator/internal/pgconfig"
	"github.com/andrew01234567890/pgshard2/operator/internal/topology"
)

// PgShardClusterReconciler expands a PgShardCluster into its child objects:
// the initial equal-range data shards, the system shard (sequences,
// migration state), and one rendered-configuration ConfigMap per shard.
// Pod/PVC/service lifecycle belongs to the shard controller.
type PgShardClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardshards,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update

func (r *PgShardClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var cluster pgshardv1alpha1.PgShardCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if cluster.Spec.Pause {
		if cluster.Status.Phase != pgshardv1alpha1.ClusterPaused {
			cluster.Status.Phase = pgshardv1alpha1.ClusterPaused
			if err := r.Status().Update(ctx, &cluster); err != nil {
				return ctrl.Result{}, client.IgnoreNotFound(err)
			}
		}
		log.Info("cluster paused; skipping reconcile")
		return ctrl.Result{}, nil
	}

	inputs := pgconfig.Inputs{
		UserParameters: cluster.Spec.Postgres.Parameters,
		SlotHeadroom:   16,
	}
	if cluster.Spec.Size != nil {
		inputs.Class = cluster.Spec.Size.Class
		inputs.Overrides = cluster.Spec.Size.Overrides
	}
	rendered, err := pgconfig.Render(inputs)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("rendering configuration: %w", err)
	}

	desired, err := desiredShards(&cluster, rendered)
	if err != nil {
		return ctrl.Result{}, err
	}

	ready, degraded := int32(0), int32(0)
	for i := range desired {
		shard := &desired[i]
		existing := &pgshardv1alpha1.PgShardShard{}
		err := r.Get(ctx, client.ObjectKeyFromObject(shard), existing)
		switch {
		case apierrors.IsNotFound(err):
			// Validate the derived child name up front (cheap, no side effect),
			// but only materialize the ConfigMap AFTER Create establishes our
			// ownership — otherwise a stale cached NotFound over a foreign shard
			// would let us write config for a shard we do not own. A concurrent
			// create (AlreadyExists) means it exists after all: requeue so the
			// next reconcile Gets and ownership-checks it.
			if _, err := configMapName(shard.Name); err != nil {
				return ctrl.Result{}, err
			}
			if err := controllerutil.SetControllerReference(&cluster, shard, r.Scheme); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.Create(ctx, shard); err != nil {
				if apierrors.IsAlreadyExists(err) {
					return ctrl.Result{Requeue: true}, nil
				}
				return ctrl.Result{}, fmt.Errorf("creating shard %s: %w", shard.Name, err)
			}
			if err := r.ensureConfigMap(ctx, &cluster, shard.Name, rendered); err != nil {
				return ctrl.Result{}, err
			}
			log.Info("created shard", "shard", shard.Name, "range", shard.Spec.KeyRange)
		case err != nil:
			return ctrl.Result{}, err
		default:
			// A shard with the desired name that this cluster does not own is a
			// foreign object (or a stale one from a deleted cluster of the same
			// name); mutating it or its ConfigMap would corrupt the partition, so
			// verify ownership before touching either.
			if !metav1.IsControlledBy(existing, &cluster) {
				return ctrl.Result{}, fmt.Errorf(
					"shard %s exists but is not controlled by cluster %s", shard.Name, cluster.Name)
			}
			if err := r.ensureConfigMap(ctx, &cluster, shard.Name, rendered); err != nil {
				return ctrl.Result{}, err
			}
			// Key range and clusterRef are immutable; converge the fields the
			// cluster owns (config hash, sizing) without touching the rest.
			if existing.Spec.PostgresConfigHash != shard.Spec.PostgresConfigHash ||
				existing.Spec.Replicas != shard.Spec.Replicas ||
				existing.Spec.Image != shard.Spec.Image {
				existing.Spec.PostgresConfigHash = shard.Spec.PostgresConfigHash
				existing.Spec.Replicas = shard.Spec.Replicas
				existing.Spec.Image = shard.Spec.Image
				existing.Spec.Resources = shard.Spec.Resources
				if err := r.Update(ctx, existing); err != nil {
					return ctrl.Result{}, err
				}
			}
			switch existing.Status.Phase {
			case pgshardv1alpha1.ShardReady:
				ready++
			case pgshardv1alpha1.ShardDegraded:
				degraded++
			}
		}
	}

	total := int32(len(desired))
	newStatus := cluster.Status.DeepCopy()
	newStatus.Shards = pgshardv1alpha1.ShardCounts{Total: total, Ready: ready, Degraded: degraded}
	newStatus.Phase = clusterPhase(ready, degraded, total)
	// Write status only when it changed: the cluster watches its own status
	// updates, so an unconditional write would spin the reconcile loop.
	if !equality.Semantic.DeepEqual(&cluster.Status, newStatus) {
		cluster.Status = *newStatus
		if err := r.Status().Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
	}
	return ctrl.Result{}, nil
}

// clusterPhase is recomputed from the shard counts every reconcile so the phase
// tracks health in both directions (a degraded shard demotes a Ready cluster).
func clusterPhase(ready, degraded, total int32) pgshardv1alpha1.ClusterPhase {
	switch {
	case total == 0 || ready < total && degraded == 0:
		return pgshardv1alpha1.ClusterProvisioning
	case ready == total:
		return pgshardv1alpha1.ClusterReady
	default:
		return pgshardv1alpha1.ClusterDegraded
	}
}

// desiredShards is the initial expansion: equal ranges for data shards plus
// the unsharded system shard. Reshards never pass through here — the shard
// set only changes via PgShardReshard, so this function must stay a pure
// function of the immutable parts of the spec plus per-shard sizing.
func desiredShards(
	cluster *pgshardv1alpha1.PgShardCluster,
	rendered pgconfig.Rendered,
) ([]pgshardv1alpha1.PgShardShard, error) {
	ranges, err := topology.FullRange.SplitEvenly(uint32(cluster.Spec.Shards.InitialCount))
	if err != nil {
		return nil, fmt.Errorf("splitting keyspace: %w", err)
	}
	shards := make([]pgshardv1alpha1.PgShardShard, 0, len(ranges)+1)
	for _, kr := range ranges {
		start := topology.FormatBound(kr.Start())
		end := ""
		if e, closed := kr.End(); closed {
			end = topology.FormatBound(e)
		}
		name := shardName(cluster.Name, start, end)
		shards = append(shards, shardFor(cluster, name,
			pgshardv1alpha1.KeyRange{Start: start, End: end}, pgshardv1alpha1.ShardRoleData, rendered))
	}
	// The system shard is unsharded: its Role — not its key range — is what
	// excludes it from data routing, so the zero (full-range) KeyRange is never
	// consulted for placement. Partition/routing logic keys on Role==system.
	systemName := fmt.Sprintf("%s-system", cluster.Name)
	shards = append(shards, shardFor(cluster, systemName,
		pgshardv1alpha1.KeyRange{}, pgshardv1alpha1.ShardRoleSystem, rendered))
	return shards, nil
}

// shardFor builds a PgShardShard with the fields the cluster controller owns.
func shardFor(
	cluster *pgshardv1alpha1.PgShardCluster,
	name string,
	kr pgshardv1alpha1.KeyRange,
	role pgshardv1alpha1.ShardRole,
	rendered pgconfig.Rendered,
) pgshardv1alpha1.PgShardShard {
	return pgshardv1alpha1.PgShardShard{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace},
		Spec: pgshardv1alpha1.PgShardShardSpec{
			ClusterRef:         cluster.Name,
			KeyRange:           kr,
			Role:               role,
			Replicas:           rendered.ReplicasPerShard,
			Serving:            true,
			PostgresConfigHash: rendered.ConfigHash,
			Image:              cluster.Spec.Postgres.Image,
			Resources:          rendered.Resources.DeepCopy(),
			Stanza:             fmt.Sprintf("%s-g1", name),
		},
	}
}

func shardName(cluster, start, end string) string {
	if start == "" {
		start = "min"
	}
	if end == "" {
		end = "max"
	}
	return fmt.Sprintf("%s-%s-%s", cluster, start, end)
}

// ensureConfigMap materializes the rendered postgresql parameters for one
// shard; the content hash in the shard spec is what agents/rollouts compare.
// configMapName derives (and length-validates) a shard's config-map name. It
// is pure so callers can validate a name before creating the shard without the
// side effect of writing the config map.
func configMapName(shardName string) (string, error) {
	name := fmt.Sprintf("%s-postgres-config", shardName)
	if len(name) > validation.DNS1123SubdomainMaxLength {
		return "", fmt.Errorf("config map name %q exceeds %d characters; shorten the cluster name",
			name, validation.DNS1123SubdomainMaxLength)
	}
	return name, nil
}

func (r *PgShardClusterReconciler) ensureConfigMap(
	ctx context.Context,
	cluster *pgshardv1alpha1.PgShardCluster,
	shardName string,
	rendered pgconfig.Rendered,
) error {
	name, err := configMapName(shardName)
	if err != nil {
		return err
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Data = map[string]string{
			"config-hash": rendered.ConfigHash,
		}
		for k, v := range rendered.Parameters {
			cm.Data["param."+k] = v
		}
		return controllerutil.SetControllerReference(cluster, cm, r.Scheme)
	})
	return err
}

// SetupWithManager sets up the controller with the Manager.
func (r *PgShardClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgshardv1alpha1.PgShardCluster{}).
		Owns(&pgshardv1alpha1.PgShardShard{}).
		Owns(&corev1.ConfigMap{}).
		Named("pgshardcluster").
		Complete(r)
}
