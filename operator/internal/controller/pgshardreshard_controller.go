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
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard2/operator/internal/topology"
)

// PgShardReshardReconciler drives an online key-range split.
//
// This slice reconciles a reshard through Validating: it confirms the target
// ranges partition the source shard's range, then advances to
// ProvisioningTargets. Creating the (non-serving) target shards, seeding them,
// and the gated cutover are later slices — so a reshard parked at
// ProvisioningTargets here is validated but not yet acted on.
type PgShardReshardReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// The condition that records whether the requested split is well-formed.
const reshardValidatedCondition = "Validated"

// The condition that records whether the target shards have been created.
const reshardTargetsProvisionedCondition = "TargetsProvisioned"

// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardreshards,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardreshards/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardshards,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardnodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update

func (r *PgShardReshardReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var reshard pgshardv1alpha1.PgShardReshard
	if err := r.Get(ctx, req.NamespacedName, &reshard); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	before := reshard.Status.DeepCopy()

	// Dispatch by phase. Terminal states and the later (not-yet-built) workflow
	// phases (Seeding onward) are left untouched, so a controller restart hitting
	// an already-advanced reshard never regresses it.
	var (
		result ctrl.Result
		err    error
	)
	switch reshard.Status.Phase {
	case "", pgshardv1alpha1.ReshardPending, pgshardv1alpha1.ReshardValidating:
		result, err = r.reconcileValidating(ctx, &reshard)
	case pgshardv1alpha1.ReshardProvisioningTargets:
		result, err = r.reconcileProvisioningTargets(ctx, &reshard)
	default:
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	reshard.Status.ObservedGeneration = reshard.Generation
	if !apiequality.Semantic.DeepEqual(before, &reshard.Status) {
		if err := r.Status().Update(ctx, &reshard); err != nil {
			return ctrl.Result{}, err
		}
	}
	return result, nil
}

// reconcileValidating checks the target ranges partition the source range and
// sets the phase accordingly. An invalid partition is terminal (the spec is
// immutable); a missing source shard is a transient race, so it holds and
// retries rather than failing.
func (r *PgShardReshardReconciler) reconcileValidating(
	ctx context.Context,
	reshard *pgshardv1alpha1.PgShardReshard,
) (ctrl.Result, error) {
	var source pgshardv1alpha1.PgShardShard
	sourceKey := client.ObjectKey{Namespace: reshard.Namespace, Name: reshard.Spec.SourceShard}
	if err := r.Get(ctx, sourceKey, &source); err != nil {
		if apierrors.IsNotFound(err) {
			reshard.Status.Phase = pgshardv1alpha1.ReshardValidating
			setReshardCondition(reshard, reshardValidatedCondition, metav1.ConditionFalse, "SourceNotFound",
				fmt.Sprintf("source shard %q not found yet", reshard.Spec.SourceShard))
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	// The source must actually belong to this reshard's cluster. Both the
	// reshard's clusterRef/sourceShard and the shard's clusterRef are immutable,
	// so a mismatch is a permanent misconfiguration — fail rather than validate a
	// split that would later seed and cut over another cluster's shard.
	if source.Spec.ClusterRef != reshard.Spec.ClusterRef {
		r.fail(reshard, reshardValidatedCondition, "SourceClusterMismatch",
			fmt.Sprintf("source shard %q belongs to cluster %q, not this reshard's cluster %q",
				source.Name, source.Spec.ClusterRef, reshard.Spec.ClusterRef))
		return ctrl.Result{}, nil
	}

	// The system shard holds control-plane state (sequences, migrations) and is
	// never a data-routing partition; resharding it is never valid.
	if source.Spec.Role == pgshardv1alpha1.ShardRoleSystem {
		r.fail(reshard, reshardValidatedCondition, "SourceNotReshardable",
			fmt.Sprintf("source shard %q is the system shard and cannot be resharded", source.Name))
		return ctrl.Result{}, nil
	}

	sourceRange, err := toRange(source.Spec.KeyRange)
	if err != nil {
		r.fail(reshard, reshardValidatedCondition, "InvalidSourceRange",
			fmt.Sprintf("source shard %q has an invalid key range: %v", source.Name, err))
		return ctrl.Result{}, nil
	}

	targets := make([]topology.KeyRange, 0, len(reshard.Spec.TargetRanges))
	for _, tr := range reshard.Spec.TargetRanges {
		t, err := toRange(tr)
		if err != nil {
			r.fail(reshard, reshardValidatedCondition, "InvalidPartition",
				fmt.Sprintf("target range %q-%q is invalid: %v", tr.Start, tr.End, err))
			return ctrl.Result{}, nil
		}
		targets = append(targets, t)
	}

	if err := validateReshardPartition(sourceRange, targets); err != nil {
		r.fail(reshard, reshardValidatedCondition, "InvalidPartition", err.Error())
		return ctrl.Result{}, nil
	}

	// Validated. Advance to ProvisioningTargets; the next reconcile creates the
	// target shards.
	reshard.Status.Phase = pgshardv1alpha1.ReshardProvisioningTargets
	setReshardCondition(reshard, reshardValidatedCondition, metav1.ConditionTrue, "PartitionValid",
		"target ranges partition the source shard's key range")
	return ctrl.Result{}, nil
}

// fail moves the reshard to the terminal Failed phase, recording the reason on
// the condition that owns the current phase.
func (r *PgShardReshardReconciler) fail(
	reshard *pgshardv1alpha1.PgShardReshard,
	condType, reason, message string,
) {
	reshard.Status.Phase = pgshardv1alpha1.ReshardFailed
	setReshardCondition(reshard, condType, metav1.ConditionFalse, reason, message)
}

func setReshardCondition(
	reshard *pgshardv1alpha1.PgShardReshard,
	condType string,
	status metav1.ConditionStatus,
	reason, message string,
) {
	apimeta.SetStatusCondition(&reshard.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: reshard.Generation,
	})
}

func (r *PgShardReshardReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgshardv1alpha1.PgShardReshard{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&pgshardv1alpha1.PgShardShard{}).
		Owns(&pgshardv1alpha1.PgShardNode{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&corev1.ConfigMap{}).
		Named("pgshardreshard").
		Complete(r)
}
