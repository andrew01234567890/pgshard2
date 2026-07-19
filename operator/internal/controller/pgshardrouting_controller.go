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
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard2/operator/internal/routing"
)

const routingCompiledCondition = "RoutingCompiled"

// PgShardRoutingReconciler is the SINGLE WRITER of PgShardRouting: it folds
// the cluster, its shards (whose status the shard controller keeps mirrored
// from their nodes), the table configs, resolved pod endpoints, and any
// in-flight reshard's cutover gate into one compiled spec, and lets
// routing.Write assign the monotonic epoch / topology generation.
type PgShardRoutingReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardroutings,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardshards,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardtableconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardreshards,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

func (r *PgShardRoutingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	var cluster pgshardv1alpha1.PgShardCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		// The routing object is owned by the cluster; deletion cascades.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	before := cluster.Status.DeepCopy()
	result, err := r.compileAndWrite(ctx, &cluster)
	if !apiequality.Semantic.DeepEqual(before.Conditions, cluster.Status.Conditions) {
		if updateErr := r.Status().Update(ctx, &cluster); updateErr != nil {
			// Surface it: a lost condition update would leave a stale
			// RoutingCompiled verdict standing with no retry.
			if err == nil {
				return result, updateErr
			}
			log.Error(updateErr, "updating cluster routing condition")
		}
	}
	return result, err
}

func (r *PgShardRoutingReconciler) compileAndWrite(
	ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster,
) (ctrl.Result, error) {
	shards, nodes, err := r.clusterShards(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	var configs pgshardv1alpha1.PgShardTableConfigList
	if err := r.List(ctx, &configs, client.InNamespace(cluster.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	tableConfigs := make([]pgshardv1alpha1.PgShardTableConfig, 0, len(configs.Items))
	for _, cfg := range configs.Items {
		if cfg.Spec.ClusterRef == cluster.Name {
			tableConfigs = append(tableConfigs, cfg)
		}
	}
	endpoints, err := r.resolveEndpoints(ctx, cluster.Namespace, shards, nodes)
	if err != nil {
		return ctrl.Result{}, err
	}
	gates, err := r.cutoverGates(ctx, cluster, shards)
	if err != nil {
		return ctrl.Result{}, err
	}

	desired, err := routing.Compile(routing.CompileInputs{
		Cluster:      cluster,
		Shards:       shards,
		TableConfigs: tableConfigs,
		Endpoints:    endpoints,
		Gates:        gates,
	})
	if err != nil {
		// Compile refusals are configuration problems (duplicate tables,
		// serving shards not partitioning the keyspace, two system shards):
		// surface them and KEEP the last good routing — routers must never
		// see a half-correct view.
		apimeta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type: routingCompiledCondition, Status: metav1.ConditionFalse,
			Reason: "CompileFailed", Message: err.Error(),
			ObservedGeneration: cluster.Generation,
		})
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	epoch, _, err := routing.Write(ctx, r.Client, key, desired)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Converge ownership EVERY reconcile: Write creates the object bare, and
	// an ownership update lost to a crash or conflict must not stay lost
	// behind an unchanged-spec short-circuit.
	if err := r.ensureRoutingOwned(ctx, key, cluster); err != nil {
		return ctrl.Result{}, err
	}
	apimeta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type: routingCompiledCondition, Status: metav1.ConditionTrue,
		Reason: "Compiled", Message: fmt.Sprintf("routing epoch %d", epoch),
		ObservedGeneration: cluster.Generation,
	})
	return ctrl.Result{}, nil
}

// clusterShards lists the cluster's shards and OVERLAYS each placed shard's
// instance view from its node — the authoritative source (the shard mirror
// carries only phase and CurrentPrimary). The overlay is in-memory input for
// the compiler, never written back.
func (r *PgShardRoutingReconciler) clusterShards(
	ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster,
) ([]pgshardv1alpha1.PgShardShard, map[string]*pgshardv1alpha1.PgShardNode, error) {
	var list pgshardv1alpha1.PgShardShardList
	if err := r.List(ctx, &list, client.InNamespace(cluster.Namespace)); err != nil {
		return nil, nil, err
	}
	nodes := map[string]*pgshardv1alpha1.PgShardNode{}
	var shards []pgshardv1alpha1.PgShardShard
	for _, s := range list.Items {
		if s.Spec.ClusterRef != cluster.Name {
			continue
		}
		if ref := s.Spec.NodeRef; ref != "" {
			node, ok := nodes[ref]
			if !ok {
				var n pgshardv1alpha1.PgShardNode
				err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: ref}, &n)
				switch {
				case apierrors.IsNotFound(err):
					nodes[ref] = nil
				case err != nil:
					return nil, nil, err
				default:
					nodes[ref] = &n
				}
				node = nodes[ref]
			}
			if node != nil {
				s.Status.Instances = node.Status.Instances
				s.Status.CurrentPrimary = node.Status.CurrentPrimary
			} else {
				// No node: publish the shard entry with NO endpoints rather
				// than trust the stale mirrored status.
				s.Status.Instances = nil
				s.Status.CurrentPrimary = ""
			}
		}
		shards = append(shards, s)
	}
	return shards, nodes, nil
}

// resolveEndpoints maps every instance pod to its directly-dialable address —
// but ONLY when the live pod is still controlled by the node incarnation the
// instance status describes. Names are reusable: a same-named replacement pod
// under a recreated node must never inherit stale role/readiness evidence.
// Anything unverifiable is simply absent; the compiler then omits that
// endpoint (routers never dial a guess).
func (r *PgShardRoutingReconciler) resolveEndpoints(
	ctx context.Context,
	namespace string,
	shards []pgshardv1alpha1.PgShardShard,
	nodes map[string]*pgshardv1alpha1.PgShardNode,
) (map[string]pgshardv1alpha1.RoutingEndpoint, error) {
	endpoints := map[string]pgshardv1alpha1.RoutingEndpoint{}
	for _, shard := range shards {
		node := nodes[shard.Spec.NodeRef]
		if node == nil {
			continue
		}
		for _, inst := range shard.Status.Instances {
			if _, done := endpoints[inst.Pod]; done {
				continue
			}
			var pod corev1.Pod
			if err := r.Get(ctx,
				client.ObjectKey{Namespace: namespace, Name: inst.Pod}, &pod); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return nil, err
			}
			if !metav1.IsControlledBy(&pod, node) || pod.Status.PodIP == "" {
				continue
			}
			endpoints[inst.Pod] = pgshardv1alpha1.RoutingEndpoint{
				Pod:  inst.Pod,
				Host: pod.Status.PodIP,
				Port: postgresPort,
			}
		}
	}
	return endpoints, nil
}

// cutoverGates emits one bufferWrites gate per reshard that is CUTTING OVER
// with a declared gate deadline: the gate covers the SOURCE shard's key
// range, and routers that cannot apply a gated epoch stop renewing their
// write lease — writes quiesce by lease expiry.
func (r *PgShardRoutingReconciler) cutoverGates(
	ctx context.Context,
	cluster *pgshardv1alpha1.PgShardCluster,
	shards []pgshardv1alpha1.PgShardShard,
) ([]pgshardv1alpha1.RoutingGate, error) {
	var reshards pgshardv1alpha1.PgShardReshardList
	if err := r.List(ctx, &reshards, client.InNamespace(cluster.Namespace)); err != nil {
		return nil, err
	}
	byName := map[string]*pgshardv1alpha1.PgShardShard{}
	for i := range shards {
		byName[shards[i].Name] = &shards[i]
	}
	var gates []pgshardv1alpha1.RoutingGate
	for _, rs := range reshards.Items {
		// The gate follows the FIELD, never the phase: the cutover machine
		// clears CutoverGateDeadline only after it has OBSERVED the switched
		// serving set compiled into routing. A reordered phase transition can
		// therefore never publish a fresh ungated epoch that still carries
		// the pre-switch topology and re-admits writes to the old source.
		if rs.Spec.ClusterRef != cluster.Name || rs.Status.CutoverGateDeadline == nil {
			continue
		}
		source, ok := byName[rs.Spec.SourceShard]
		if !ok {
			// The cutover controller validates the source exists before
			// requesting a gate; a missing source here is transient reading
			// order — skip this compile, the next event retries.
			continue
		}
		gates = append(gates, pgshardv1alpha1.RoutingGate{
			ID:   "reshard-" + rs.Name,
			Mode: "bufferWrites",
			Match: pgshardv1alpha1.GateMatch{
				KeyRanges: []pgshardv1alpha1.KeyRange{source.Spec.KeyRange},
			},
			Deadline: *rs.Status.CutoverGateDeadline,
		})
	}
	return gates, nil
}

// ensureRoutingOwned parents the routing object to its cluster so deletion
// cascades (routing.Write creates it bare).
func (r *PgShardRoutingReconciler) ensureRoutingOwned(
	ctx context.Context, key types.NamespacedName, cluster *pgshardv1alpha1.PgShardCluster,
) error {
	var rt pgshardv1alpha1.PgShardRouting
	if err := r.Get(ctx, key, &rt); err != nil {
		return err
	}
	if metav1.IsControlledBy(&rt, cluster) {
		return nil
	}
	if err := controllerutil.SetControllerReference(cluster, &rt, r.Scheme); err != nil {
		return err
	}
	return r.Update(ctx, &rt)
}

func (r *PgShardRoutingReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mapShard := handler.EnqueueRequestsFromMapFunc(
		func(_ context.Context, obj client.Object) []ctrl.Request {
			s, ok := obj.(*pgshardv1alpha1.PgShardShard)
			if !ok || s.Spec.ClusterRef == "" {
				return nil
			}
			return []ctrl.Request{{NamespacedName: types.NamespacedName{
				Namespace: s.Namespace, Name: s.Spec.ClusterRef,
			}}}
		})
	mapConfig := handler.EnqueueRequestsFromMapFunc(
		func(_ context.Context, obj client.Object) []ctrl.Request {
			c, ok := obj.(*pgshardv1alpha1.PgShardTableConfig)
			if !ok || c.Spec.ClusterRef == "" {
				return nil
			}
			return []ctrl.Request{{NamespacedName: types.NamespacedName{
				Namespace: c.Namespace, Name: c.Spec.ClusterRef,
			}}}
		})
	mapReshard := handler.EnqueueRequestsFromMapFunc(
		func(_ context.Context, obj client.Object) []ctrl.Request {
			rs, ok := obj.(*pgshardv1alpha1.PgShardReshard)
			if !ok || rs.Spec.ClusterRef == "" {
				return nil
			}
			return []ctrl.Request{{NamespacedName: types.NamespacedName{
				Namespace: rs.Namespace, Name: rs.Spec.ClusterRef,
			}}}
		})
	mapPod := handler.EnqueueRequestsFromMapFunc(
		func(ctx context.Context, obj client.Object) []ctrl.Request {
			ref := metav1.GetControllerOf(obj)
			if ref == nil || ref.Kind != "PgShardNode" {
				return nil
			}
			var node pgshardv1alpha1.PgShardNode
			if err := r.Get(ctx,
				types.NamespacedName{Namespace: obj.GetNamespace(), Name: ref.Name}, &node); err != nil {
				return nil
			}
			owner := metav1.GetControllerOf(&node)
			if owner == nil {
				return nil
			}
			switch owner.Kind {
			case "PgShardCluster":
				return []ctrl.Request{{NamespacedName: types.NamespacedName{
					Namespace: node.Namespace, Name: owner.Name,
				}}}
			case "PgShardReshard":
				var rs pgshardv1alpha1.PgShardReshard
				if err := r.Get(ctx,
					types.NamespacedName{Namespace: node.Namespace, Name: owner.Name}, &rs); err != nil {
					return nil
				}
				return []ctrl.Request{{NamespacedName: types.NamespacedName{
					Namespace: node.Namespace, Name: rs.Spec.ClusterRef,
				}}}
			}
			return nil
		})
	return ctrl.NewControllerManagedBy(mgr).
		Named("pgshardrouting").
		For(&pgshardv1alpha1.PgShardCluster{}).
		Watches(&pgshardv1alpha1.PgShardShard{}, mapShard).
		Watches(&pgshardv1alpha1.PgShardTableConfig{}, mapConfig).
		Watches(&pgshardv1alpha1.PgShardReshard{}, mapReshard).
		Watches(&corev1.Pod{}, mapPod).
		Complete(r)
}
