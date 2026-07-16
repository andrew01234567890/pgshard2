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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete

func (r *PgShardClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var cluster pgshardv1alpha1.PgShardCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if cluster.Spec.Pause {
		log.Info("cluster paused; skipping reconcile")
		return ctrl.Result{}, nil
	}

	rendered, err := pgconfig.Render(pgconfig.Inputs{
		Class:          cluster.Spec.Size.Class,
		Overrides:      cluster.Spec.Size.Overrides,
		UserParameters: cluster.Spec.Postgres.Parameters,
		SlotHeadroom:   16,
	})
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
		if err := r.ensureConfigMap(ctx, &cluster, shard.Name, rendered); err != nil {
			return ctrl.Result{}, err
		}
		existing := &pgshardv1alpha1.PgShardShard{}
		err := r.Get(ctx, client.ObjectKeyFromObject(shard), existing)
		switch {
		case apierrors.IsNotFound(err):
			if err := controllerutil.SetControllerReference(&cluster, shard, r.Scheme); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.Create(ctx, shard); err != nil && !apierrors.IsAlreadyExists(err) {
				return ctrl.Result{}, fmt.Errorf("creating shard %s: %w", shard.Name, err)
			}
			log.Info("created shard", "shard", shard.Name, "range", shard.Spec.KeyRange)
		case err != nil:
			return ctrl.Result{}, err
		default:
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

	cluster.Status.Shards = pgshardv1alpha1.ShardCounts{
		Total:    int32(len(desired)),
		Ready:    ready,
		Degraded: degraded,
	}
	if cluster.Status.Phase == "" {
		cluster.Status.Phase = pgshardv1alpha1.ClusterProvisioning
	}
	if ready == int32(len(desired)) && ready > 0 {
		cluster.Status.Phase = pgshardv1alpha1.ClusterReady
	}
	if err := r.Status().Update(ctx, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	return ctrl.Result{}, nil
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
	image := cluster.Spec.Postgres.Image

	shards := make([]pgshardv1alpha1.PgShardShard, 0, len(ranges)+1)
	for _, kr := range ranges {
		start := topology.FormatBound(kr.Start())
		end := ""
		if e, closed := kr.End(); closed {
			end = topology.FormatBound(e)
		}
		name := shardName(cluster.Name, start, end)
		shards = append(shards, pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef:         cluster.Name,
				KeyRange:           pgshardv1alpha1.KeyRange{Start: start, End: end},
				Role:               pgshardv1alpha1.ShardRoleData,
				Replicas:           rendered.ReplicasPerShard,
				Serving:            true,
				PostgresConfigHash: rendered.ConfigHash,
				Image:              image,
				Resources:          rendered.Resources.DeepCopy(),
				Stanza:             fmt.Sprintf("%s-g1", name),
			},
		})
	}
	systemName := fmt.Sprintf("%s-system", cluster.Name)
	shards = append(shards, pgshardv1alpha1.PgShardShard{
		ObjectMeta: metav1.ObjectMeta{Name: systemName, Namespace: cluster.Namespace},
		Spec: pgshardv1alpha1.PgShardShardSpec{
			ClusterRef:         cluster.Name,
			KeyRange:           pgshardv1alpha1.KeyRange{},
			Role:               pgshardv1alpha1.ShardRoleSystem,
			Replicas:           rendered.ReplicasPerShard,
			Serving:            true,
			PostgresConfigHash: rendered.ConfigHash,
			Image:              image,
			Resources:          rendered.Resources.DeepCopy(),
			Stanza:             fmt.Sprintf("%s-g1", systemName),
		},
	})
	return shards, nil
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
func (r *PgShardClusterReconciler) ensureConfigMap(
	ctx context.Context,
	cluster *pgshardv1alpha1.PgShardCluster,
	shard string,
	rendered pgconfig.Rendered,
) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-postgres-config", shard),
			Namespace: cluster.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
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
