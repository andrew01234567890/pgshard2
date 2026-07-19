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
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard2/operator/internal/agentclient"
	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
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
	// Agents dials seeding RPCs on source and target agents. Nil until the
	// Seeding phase needs it (tests inject dialAgent instead).
	Agents *agentclient.Pool
	// dialAgent overrides agent resolution in tests.
	dialAgent func(host string, port int32) (pgshardv1.AgentServiceClient, error)
}

func (r *PgShardReshardReconciler) agentClient(host string) (pgshardv1.AgentServiceClient, error) {
	if r.dialAgent != nil {
		return r.dialAgent(host, agentPort)
	}
	if r.Agents == nil {
		return nil, fmt.Errorf("no agent pool configured")
	}
	return r.Agents.Get(host, agentPort)
}

// The condition that records whether the requested split is well-formed.
const reshardValidatedCondition = "Validated"

// The condition that records whether the target shards have been created.
const reshardTargetsProvisionedCondition = "TargetsProvisioned"

// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardreshards,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardroutings,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardreshards/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardshards,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardnodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardtableconfigs,verbs=get;list;watch

func (r *PgShardReshardReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var reshard pgshardv1alpha1.PgShardReshard
	if err := r.Get(ctx, req.NamespacedName, &reshard); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !reshard.DeletionTimestamp.IsZero() {
		return r.cleanupCutoverClaim(ctx, &reshard)
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
	case pgshardv1alpha1.ReshardSeeding:
		result, err = r.reconcileSeeding(ctx, &reshard)
	case pgshardv1alpha1.ReshardCatchingUp:
		result, err = r.reconcileCatchingUp(ctx, &reshard)
	case pgshardv1alpha1.ReshardReadyToCutover:
		result, err = r.reconcileReadyToCutover(ctx, &reshard)
	case pgshardv1alpha1.ReshardCuttingOver:
		result, err = r.reconcileCuttingOver(ctx, &reshard)
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

	// Only a SERVING shard owns authoritative data: a hidden shard (another
	// reshard's still-seeding target) holds an incomplete copy, and seeding
	// from it would replicate that incompleteness into the new targets.
	if !source.Spec.Serving {
		r.fail(reshard, reshardValidatedCondition, "SourceNotServing",
			fmt.Sprintf("source shard %q is not serving; its data is not authoritative", source.Name))
		return ctrl.Result{}, nil
	}

	var cluster pgshardv1alpha1.PgShardCluster
	clusterKey := client.ObjectKey{Namespace: reshard.Namespace, Name: reshard.Spec.ClusterRef}
	if err := r.Get(ctx, clusterKey, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			reshard.Status.Phase = pgshardv1alpha1.ReshardValidating
			setReshardCondition(reshard, reshardValidatedCondition, metav1.ConditionFalse,
				"ClusterNotFound", fmt.Sprintf("cluster %q not found yet", reshard.Spec.ClusterRef))
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		return ctrl.Result{}, err
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

	// Validated. Advance to ProvisioningTargets and requeue so the next reconcile
	// creates the target shards. The requeue is required: this reconcile writes
	// status only, which does not bump the generation, so the watch's
	// GenerationChangedPredicate would otherwise drop the self-update event and
	// the reshard would stall here until the operator restarts.
	// Pin the exact objects this validation ran against — ONCE, here: a
	// same-named replacement of either is a different placement whose data
	// and configuration were never validated. Later phases only verify.
	reshard.Status.SourceShardUID = string(source.UID)
	reshard.Status.ClusterUID = string(cluster.UID)
	reshard.Status.Phase = pgshardv1alpha1.ReshardProvisioningTargets
	setReshardCondition(reshard, reshardValidatedCondition, metav1.ConditionTrue, "PartitionValid",
		"target ranges partition the source shard's key range")
	return ctrl.Result{Requeue: true}, nil
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

// deletionPredicate passes any event whose object carries a deletion
// timestamp, so a finalizer-holding object still reconciles its own delete.
func deletionPredicate() predicate.Predicate {
	has := func(o client.Object) bool { return !o.GetDeletionTimestamp().IsZero() }
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return has(e.Object) },
		UpdateFunc: func(e event.UpdateEvent) bool { return has(e.ObjectNew) },
		DeleteFunc: func(event.DeleteEvent) bool { return true },
	}
}

func (r *PgShardReshardReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		// GenerationChangedPredicate alone would drop the deletionTimestamp-only
		// update (metadata, no generation bump), so a reshard holding the
		// cutover-cleanup finalizer would never reconcile its own deletion.
		// OR in a deletion predicate.
		For(&pgshardv1alpha1.PgShardReshard{}, builder.WithPredicates(
			predicate.Or(predicate.GenerationChangedPredicate{}, deletionPredicate()))).
		// The reshard reads no shard/node status, so watch only structural
		// (generation) changes: a target's status heartbeat must not re-enqueue it.
		Owns(&pgshardv1alpha1.PgShardShard{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&pgshardv1alpha1.PgShardNode{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&corev1.ConfigMap{}).
		Named("pgshardreshard").
		Complete(r)
}
