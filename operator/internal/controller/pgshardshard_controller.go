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
	"maps"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard2/operator/internal/agentclient"
	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
)

const (
	labelCluster = "pgshard.dev/cluster"
	labelShard   = "pgshard.dev/shard"
	labelRole    = "pgshard.dev/role"

	roleLabelPrimary = "primary"
	roleLabelReplica = "replica"

	agentPort = 9090

	// CNPG-style binary injection: the init container copies the agent
	// binary from its own image into this shared volume; the postgres
	// container runs it as PID 1.
	agentVolumePath = "/pgshard"

	portNamePostgres  = "postgres"
	agentVolumeName   = "agent"
	headlessSvcSuffix = "-pods"

	// Service suffixes and volume names shared by the shard and node
	// controllers (physical conventions); they consolidate onto the node
	// controller when the shard's physical half is removed.
	svcSuffixRW   = "-rw"
	svcSuffixRO   = "-ro"
	volNameData   = "data"
	volNameConfig = "config"
	volNameWAL    = "wal"
)

// ShardImages are the images shard pods run; wired from manager flags.
type ShardImages struct {
	Postgres string
	Agent    string
}

// PgShardShardReconciler owns the node-level objects of one shard: pods,
// PVCs, services, and the aggregated status (the operator polls agents;
// agents never write CRD status).
type PgShardShardReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Agents *agentclient.Pool
	Images ShardImages
	// StatusPollInterval is the requeue cadence for status aggregation.
	StatusPollInterval time.Duration
	// dialAgent resolves a pod address to an AgentService client. Defaults
	// to the connection pool; tests inject per-pod fakes.
	dialAgent func(host string, port int32) (pgshardv1.AgentServiceClient, error)
}

func (r *PgShardShardReconciler) agentClient(host string, port int32) (pgshardv1.AgentServiceClient, error) {
	if r.dialAgent != nil {
		return r.dialAgent(host, port)
	}
	return r.Agents.Get(host, port)
}

// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardshards,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardshards/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardshards/finalizers,verbs=update
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardnodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete

func (r *PgShardShardReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var shard pgshardv1alpha1.PgShardShard
	if err := r.Get(ctx, req.NamespacedName, &shard); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// A placed shard is logical: its PgShardNode owns the pods, services, status,
	// and failover — including fencing, which is a node-level action. This
	// controller creates no physical objects for it and only mirrors the node's
	// health so the cluster's shard counts keep working. Checked before fencing
	// so a (meaningless) shard-level fence flag cannot wedge the status mirror.
	if shard.Spec.NodeRef != "" {
		return r.reconcileLogicalShard(ctx, &shard)
	}

	if shard.Spec.Fenced {
		// Fencing is executed by agents (they keep PostgreSQL down); the
		// controller stops mutating pods while frozen.
		log.Info("shard fenced; skipping pod reconcile")
		return ctrl.Result{}, nil
	}

	if r.Images.Agent == "" {
		return ctrl.Result{}, fmt.Errorf("shard controller agent image is not configured")
	}
	if shard.Spec.Image == "" && r.Images.Postgres == "" {
		return ctrl.Result{}, fmt.Errorf("no postgres image: set shard.spec.image or the controller default")
	}

	if err := r.ensureServices(ctx, &shard); err != nil {
		return ctrl.Result{}, err
	}
	for ordinal := int32(0); ordinal < shard.Spec.Replicas; ordinal++ {
		if err := r.ensureInstance(ctx, &shard, ordinal); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Role labels are synced even when aggregation errored mid-way (a failed
	// Promote or status write): the assessment may have just fenced an
	// instance, and leaving its -rw/-ro label standing until a lucky retry
	// would keep routing to data we already know is wrong.
	readyReplicas, fenced, aggErr := r.aggregateStatus(ctx, &shard)
	if err := r.syncRoleLabels(ctx, &shard, fenced); err != nil {
		return ctrl.Result{}, err
	}
	if aggErr != nil {
		return ctrl.Result{}, aggErr
	}
	// Prune only the pods this reconcile confirmed are ready replicas — never a
	// just-promoted, unpollable, or foreign pod.
	if err := r.pruneExcessInstances(ctx, &shard, readyReplicas); err != nil {
		return ctrl.Result{}, err
	}

	interval := r.pollInterval()
	return ctrl.Result{RequeueAfter: interval}, nil
}

func instanceName(shard *pgshardv1alpha1.PgShardShard, ordinal int32) string {
	return fmt.Sprintf("%s-%d", shard.Name, ordinal)
}

// reconcileLogicalShard mirrors the placed shard's health from its node. The
// node is the physical unit that owns the pods and runs failover; the shard's
// status merely reflects it so the cluster's Ready/Degraded shard counts keep
// working. A node that does not exist yet reads as provisioning, not degraded.
func (r *PgShardShardReconciler) reconcileLogicalShard(
	ctx context.Context, shard *pgshardv1alpha1.PgShardShard,
) (ctrl.Result, error) {
	before := shard.Status.DeepCopy()
	var node pgshardv1alpha1.PgShardNode
	switch err := r.Get(ctx,
		client.ObjectKey{Namespace: shard.Namespace, Name: shard.Spec.NodeRef}, &node); {
	case apierrors.IsNotFound(err):
		shard.Status.Phase = pgshardv1alpha1.ShardProvisioning
		shard.Status.CurrentPrimary = ""
	case err != nil:
		return ctrl.Result{}, err
	default:
		shard.Status.Phase = shardPhaseForNode(node.Status.Phase)
		shard.Status.CurrentPrimary = node.Status.CurrentPrimary
		// Best-effort and never fatal: a database-provisioning problem must not
		// stop the shard from mirroring its node's health.
		r.reconcileShardDatabase(ctx, shard, &node)
	}

	interval := r.pollInterval()
	if apiequality.Semantic.DeepEqual(before, &shard.Status) {
		return ctrl.Result{RequeueAfter: interval}, nil
	}
	return ctrl.Result{RequeueAfter: interval}, client.IgnoreNotFound(r.Status().Update(ctx, shard))
}

// shardDatabaseReadyCondition marks that the shard's Postgres database exists on
// its node.
const shardDatabaseReadyCondition = "DatabaseReady"

// maxDatabaseNameBytes mirrors PostgreSQL's NAMEDATALEN-1 (the agent rejects a
// longer name). Validating here turns an otherwise permanent gRPC error into a
// terminal condition instead of an endless retry.
const maxDatabaseNameBytes = 63

// adoptDatabaseAnnotation ("true") authorizes taking over an existing
// same-named database whose provenance marker does not match this shard — a
// deliberate restore/adopt action. Without it a foreign or unmarked database
// is fenced (DatabaseReady False/ForeignDatabase) and never served.
const adoptDatabaseAnnotation = "pgshard.dev/adopt-database"

// shardDatabaseName is the Postgres DATABASE a placed shard lives in. Shard
// names are namespace-unique, and a node hosts many shards' databases, so the
// shard name is a natural per-node-unique database name.
func shardDatabaseName(shard *pgshardv1alpha1.PgShardShard) string {
	return shard.Name
}

func (r *PgShardShardReconciler) setDatabaseCondition(
	shard *pgshardv1alpha1.PgShardShard, status metav1.ConditionStatus, reason, msg string,
) {
	apimeta.SetStatusCondition(&shard.Status.Conditions, metav1.Condition{
		Type:               shardDatabaseReadyCondition,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: shard.Generation,
	})
}

// reconcileShardDatabase ensures the shard's Postgres database exists on its
// node by asking the node's primary agent to create it. It is best-effort,
// idempotent, and never returns an error: it waits until the node has a ready
// primary with a reachable address, and once it has provisioned the database on
// a given node it records both the DatabaseReady condition and the node identity
// so the round trip is skipped until the shard moves to a different node.
func (r *PgShardShardReconciler) reconcileShardDatabase(
	ctx context.Context, shard *pgshardv1alpha1.PgShardShard, node *pgshardv1alpha1.PgShardNode,
) {
	log := logf.FromContext(ctx)
	name := shardDatabaseName(shard)
	if len(name) > maxDatabaseNameBytes {
		// A name PostgreSQL would truncate is terminal, not retriable.
		r.setDatabaseCondition(shard, metav1.ConditionFalse, "InvalidName",
			fmt.Sprintf("database name %q exceeds %d bytes", name, maxDatabaseNameBytes))
		return
	}
	// Already provisioned on this node; a move to another node re-provisions.
	if shard.Status.DatabaseNode == node.Name &&
		apimeta.IsStatusConditionTrue(shard.Status.Conditions, shardDatabaseReadyCondition) {
		return
	}
	primary := node.Status.CurrentPrimary
	if node.Status.Phase != pgshardv1alpha1.NodeReady || primary == "" {
		return
	}
	var pod corev1.Pod
	if err := r.Get(ctx,
		client.ObjectKey{Namespace: shard.Namespace, Name: primary}, &pod); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "fetching primary pod for database provisioning", "pod", primary)
		}
		return
	}
	if pod.Status.PodIP == "" {
		return
	}
	agent, err := r.agentClient(pod.Status.PodIP, agentPort)
	if err != nil {
		log.Error(err, "dialing agent for database provisioning")
		return
	}
	if _, err := agent.CreateDatabase(ctx, &pgshardv1.CreateDatabaseRequest{
		Name: name,
		// The shard UID binds the database to this placement: deterministic
		// names plus retained volumes mean a same-named database can hold a
		// prior placement's stale, partially-seeded data, and the agent fences
		// it (FAILED_PRECONDITION) instead of adopting it silently.
		Provenance: string(shard.UID),
		Adopt:      shard.Annotations[adoptDatabaseAnnotation] == "true",
	}); err != nil {
		switch grpcstatus.Code(err) {
		// InvalidArgument is a permanent contract violation; record it terminally
		// rather than retrying forever. Other errors are transient — the poll
		// interval retries.
		case codes.InvalidArgument:
			r.setDatabaseCondition(shard, metav1.ConditionFalse, "Rejected", err.Error())
		case codes.FailedPrecondition:
			r.setDatabaseCondition(shard, metav1.ConditionFalse, "ForeignDatabase",
				err.Error()+fmt.Sprintf(
					" (annotate the shard with %s=true to adopt it deliberately)",
					adoptDatabaseAnnotation))
		default:
			log.Error(err, "creating shard database", "database", name)
		}
		return
	}
	shard.Status.DatabaseNode = node.Name
	r.setDatabaseCondition(shard, metav1.ConditionTrue, "Provisioned",
		fmt.Sprintf("database %s created on node %s", name, node.Name))
}

func (r *PgShardShardReconciler) pollInterval() time.Duration {
	if r.StatusPollInterval == 0 {
		return 10 * time.Second
	}
	return r.StatusPollInterval
}

func shardPhaseForNode(phase pgshardv1alpha1.NodePhase) pgshardv1alpha1.ShardPhase {
	switch phase {
	case pgshardv1alpha1.NodeReady:
		return pgshardv1alpha1.ShardReady
	case pgshardv1alpha1.NodeFailingOver:
		return pgshardv1alpha1.ShardFailingOver
	case pgshardv1alpha1.NodeDegraded:
		return pgshardv1alpha1.ShardDegraded
	default:
		return pgshardv1alpha1.ShardProvisioning
	}
}

func shardSelector(shard *pgshardv1alpha1.PgShardShard) map[string]string {
	return map[string]string{
		labelCluster: shard.Spec.ClusterRef,
		labelShard:   shard.Name,
	}
}

// ensureServices maintains the CNPG-style trio plus stable per-pod DNS:
// -rw (primary only), -ro (replicas), -r (all instances), and a headless
// service so native logical-replication subscribers can pin a standby.
func (r *PgShardShardReconciler) ensureServices(
	ctx context.Context, shard *pgshardv1alpha1.PgShardShard,
) error {
	services := []struct {
		suffix   string
		selector map[string]string
		headless bool
	}{
		{svcSuffixRW, withRole(shardSelector(shard), roleLabelPrimary), false},
		{svcSuffixRO, withRole(shardSelector(shard), roleLabelReplica), false},
		{"-r", shardSelector(shard), false},
		{headlessSvcSuffix, shardSelector(shard), true},
	}
	for _, spec := range services {
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Name: shard.Name + spec.suffix, Namespace: shard.Namespace,
		}}
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
			svc.Labels = shardSelector(shard)
			svc.Spec.Selector = spec.selector
			svc.Spec.Ports = []corev1.ServicePort{{
				Name: portNamePostgres, Port: 5432, TargetPort: intstr.FromString(portNamePostgres),
			}}
			if spec.headless {
				svc.Spec.ClusterIP = corev1.ClusterIPNone
				// Publish pods before readiness so replication can bootstrap.
				svc.Spec.PublishNotReadyAddresses = true
			}
			return controllerutil.SetControllerReference(shard, svc, r.Scheme)
		})
		if err != nil {
			return fmt.Errorf("service %s%s: %w", shard.Name, spec.suffix, err)
		}
	}
	return nil
}

func withRole(selector map[string]string, role string) map[string]string {
	out := map[string]string{labelRole: role}
	maps.Copy(out, selector)
	return out
}

// ensureInstance creates the PVC and pod for one ordinal. Pods are never
// mutated in place here: config/image changes go through the rollout flow
// (replicas first, primary last via switchover), not blind recreation.
func (r *PgShardShardReconciler) ensureInstance(
	ctx context.Context, shard *pgshardv1alpha1.PgShardShard, ordinal int32,
) error {
	name := instanceName(shard, ordinal)

	if err := r.ensurePVC(ctx, shard, name+"-data"); err != nil {
		return err
	}
	if shard.Spec.Storage != nil && shard.Spec.Storage.WalSeparate {
		if err := r.ensurePVC(ctx, shard, name+"-wal"); err != nil {
			return err
		}
	}

	var pod corev1.Pod
	err := r.Get(ctx, client.ObjectKey{Namespace: shard.Namespace, Name: name}, &pod)
	if err == nil {
		if !metav1.IsControlledBy(&pod, shard) {
			return fmt.Errorf("pod %s exists but is not controlled by shard %s", name, shard.Name)
		}
		// Pods are not mutated in place here: a config/image/storage change
		// (including toggling walSeparate on an existing instance) is rolled out
		// separately — replicas first, primary last via switchover.
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	desired := r.instancePod(shard, ordinal)
	if err := controllerutil.SetControllerReference(shard, desired, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, desired); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("pod %s: %w", name, err)
	}
	return nil
}

// ensurePVC creates a shard PVC if absent. PVCs deliberately carry no owner
// reference: data outlives pod churn, and deletion is an explicit decommission
// step — never on scale-down or pod recreation.
func (r *PgShardShardReconciler) ensurePVC(
	ctx context.Context, shard *pgshardv1alpha1.PgShardShard, pvcName string,
) error {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: pvcName, Namespace: shard.Namespace,
	}}
	err := r.Get(ctx, client.ObjectKeyFromObject(pvc), pvc)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	size := resource.MustParse("1Gi")
	var storageClass *string
	if shard.Spec.Storage != nil {
		size = shard.Spec.Storage.Size
		if shard.Spec.Storage.StorageClass != "" {
			storageClass = &shard.Spec.Storage.StorageClass
		}
	}
	pvc.Labels = shardSelector(shard)
	pvc.Spec = corev1.PersistentVolumeClaimSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		Resources: corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceStorage: size},
		},
		StorageClassName: storageClass,
	}
	if err := r.Create(ctx, pvc); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("pvc %s: %w", pvcName, err)
	}
	return nil
}

// pruneExcessInstances deletes pods for ordinals at or above the desired
// replica count (a scale-down). Their PVCs are retained for data safety and
// reused if the shard scales back up.
func (r *PgShardShardReconciler) pruneExcessInstances(
	ctx context.Context, shard *pgshardv1alpha1.PgShardShard, candidates []corev1.Pod,
) error {
	// Only scale down a fully-healthy shard. A promotion always drives the shard
	// through a non-Ready phase (Degraded/Provisioning), so gating on Ready keeps
	// pruning out of the window in which a candidate replica could be promoted
	// after polling. A planned switchover's coordinated decommission is the
	// failover controller's responsibility, not raced here.
	if shard.Status.Phase != pgshardv1alpha1.ShardReady {
		return nil
	}
	// candidates are exactly the pods aggregateStatus confirmed are ready
	// replicas this reconcile, so a just-promoted, unpollable, foreign, or
	// same-name-replacement pod is never deleted. PVCs are retained for data
	// safety and reused on scale-up.
	prefix := shard.Name + "-"
	for i := range candidates {
		pod := &candidates[i]
		ord, ok := ordinalOf(pod.Name, prefix)
		if !ok || ord < shard.Spec.Replicas {
			continue
		}
		// Delete the exact pod we confirmed (UID precondition); if it was
		// replaced or already removed since, skip it.
		uid := pod.UID
		if err := r.Delete(ctx, pod, client.Preconditions{UID: &uid}); err != nil {
			if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
				continue
			}
			return fmt.Errorf("pruning pod %s: %w", pod.Name, err)
		}
	}
	return nil
}

func ordinalOf(podName, prefix string) (int32, bool) {
	if !strings.HasPrefix(podName, prefix) {
		return 0, false
	}
	// ParseInt with bitSize 32 rejects values that would truncate; a canonical
	// ordinal round-trips (no leading zeros / sign).
	suffix := strings.TrimPrefix(podName, prefix)
	n, err := strconv.ParseInt(suffix, 10, 32)
	if err != nil || n < 0 || strconv.FormatInt(n, 10) != suffix {
		return 0, false
	}
	return int32(n), true
}

func (r *PgShardShardReconciler) instancePod(
	shard *pgshardv1alpha1.PgShardShard, ordinal int32,
) *corev1.Pod {
	name := instanceName(shard, ordinal)
	postgresImage := shard.Spec.Image
	if postgresImage == "" {
		postgresImage = r.Images.Postgres
	}
	labels := shardSelector(shard)
	labels[labelRole] = roleLabelReplica // promoted after status polls

	var resources corev1.ResourceRequirements
	if shard.Spec.Resources != nil {
		resources = *shard.Spec.Resources
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: shard.Namespace,
			Labels:    labels,
			Annotations: map[string]string{
				"pgshard.dev/config-hash": shard.Spec.PostgresConfigHash,
			},
		},
		Spec: corev1.PodSpec{
			Hostname:  name,
			Subdomain: shard.Name + headlessSvcSuffix,
			InitContainers: []corev1.Container{{
				Name:    "inject-agent",
				Image:   r.Images.Agent,
				Command: []string{"cp", "/pgshard-agent", agentVolumePath + "/pgshard-agent"},
				VolumeMounts: []corev1.VolumeMount{{
					Name: agentVolumeName, MountPath: agentVolumePath,
				}},
			}},
			Containers: []corev1.Container{{
				Name:    portNamePostgres,
				Image:   postgresImage,
				Command: []string{agentVolumePath + "/pgshard-agent", "run"},
				Env: []corev1.EnvVar{
					{Name: "PGSHARD_CLUSTER", Value: shard.Spec.ClusterRef},
					{Name: "PGSHARD_SHARD", Value: shard.Name},
					{Name: "PGSHARD_POD", ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
					}},
				},
				Ports: []corev1.ContainerPort{
					{Name: portNamePostgres, ContainerPort: 5432},
					{Name: "agent-grpc", ContainerPort: agentPort},
					{Name: "probes", ContainerPort: 8080},
				},
				Resources: resources,
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/readyz", Port: intstr.FromString("probes"),
						},
					},
					PeriodSeconds: 5,
				},
				// Liveness carries the primary isolation self-fence: the
				// agent reports unhealthy only when it is a primary that
				// can reach neither the API server nor its replicas.
				LivenessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/healthz", Port: intstr.FromString("probes"),
						},
					},
					PeriodSeconds:    10,
					FailureThreshold: 3,
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "agent", MountPath: agentVolumePath},
					{Name: volNameData, MountPath: "/var/lib/postgresql/data"},
					{Name: volNameConfig, MountPath: "/etc/pgshard/config"},
				},
			}},
			Volumes: []corev1.Volume{
				{Name: agentVolumeName, VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				}},
				{Name: volNameData, VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: name + "-data",
					},
				}},
				{Name: volNameConfig, VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: shard.Name + "-postgres-config",
						},
					},
				}},
			},
		},
	}

	// storage.walSeparate mounts a dedicated WAL volume so WAL I/O does not
	// contend with the data volume.
	if shard.Spec.Storage != nil && shard.Spec.Storage.WalSeparate {
		c := &pod.Spec.Containers[0]
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
			Name: volNameWAL, MountPath: "/var/lib/postgresql/wal",
		})
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: volNameWAL, VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: name + "-wal",
				},
			},
		})
	}
	return pod
}

// aggregateStatus polls every controlled instance's agent, writes the shard
// status (single status writer), and returns the controlled pods it confirmed
// are ready replicas this reconcile — the only pods a scale-down may prune,
// bound to their polled UID.
func (r *PgShardShardReconciler) aggregateStatus(
	ctx context.Context, shard *pgshardv1alpha1.PgShardShard,
) ([]corev1.Pod, map[string]bool, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(shardSelector(shard))); err != nil {
		return nil, nil, err
	}

	before := shard.Status.DeepCopy()

	instances := make([]pgshardv1alpha1.InstanceState, 0, len(pods.Items))
	polled := make([]corev1.Pod, 0, len(pods.Items))
	views := make([]instanceView, 0, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, shard) {
			// A foreign pod that merely matches our labels must not be polled
			// into status (it could otherwise be preserved as CurrentPrimary).
			continue
		}
		// Role is left unset until the agent explicitly confirms one. An
		// unconfirmed role (unpolled, or reported UNSPECIFIED) is not silently
		// treated as a replica: doing so would let a role-unknown ready pod be
		// elected, counted toward readiness, or pruned.
		state := pgshardv1alpha1.InstanceState{Pod: pod.Name}
		view := instanceView{pod: pod.Name, host: pod.Status.PodIP}
		if pod.Status.PodIP != "" {
			pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			status, err := r.pollAgent(pollCtx, pod.Status.PodIP)
			cancel()
			if err == nil {
				view.observed = true
				state.Ready = status.Ready
				view.ready = status.Ready
				view.receivedLSN = lsnValue(status.WalReceiveLsn)
				view.systemID = status.SystemId
				view.timeline = int32(status.Timeline)
				view.walReceiver = status.WalReceiverActive
				switch status.Role {
				case pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY:
					state.Role = roleLabelPrimary
					view.isPrimary = true
				case pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY:
					state.Role = roleLabelReplica
					view.isStandby = true
				}
				state.WalWriteLSN = lsnString(status.WalWriteLsn)
				state.WalReplayLSN = lsnString(status.WalReplayLsn)
			}
		}
		instances = append(instances, state)
		polled = append(polled, *pod)
		views = append(views, view)
	}

	// Same identity guard as the node controller (this is the legacy physical
	// path for un-placed shards).
	var expectedID uint64
	var parseErr error
	shard.Status.SystemID, expectedID, shard.Status.Timeline, parseErr = latchIdentity(
		views, before.CurrentPrimary, before.TargetPrimary,
		shard.Status.SystemID, shard.Status.Timeline)
	assessment := assessIdentity(views, identityInputs{
		systemID:  expectedID,
		timeline:  shard.Status.Timeline,
		current:   before.CurrentPrimary,
		committed: before.TargetPrimary,
		malformed: parseErr != nil,
	})
	// An unrecognized instance's role is never believed: no CurrentPrimary
	// (-rw writes) for a rogue claimant, no replica role (-ro reads) for a
	// fenced standby.
	for i := range views {
		if assessment.rogue(views[i].pod) {
			instances[i].Role = ""
		}
	}
	if len(assessment.fenced) > 0 {
		logf.FromContext(ctx).Info("instances fenced from election or recognition",
			"shard", shard.Name, "instances", assessment.fenced)
	}

	shard.Status.Instances = instances
	// CurrentPrimary is assigned unconditionally, so a demoted/unreachable
	// primary with no confirmed replacement is CLEARED (else -rw keeps routing
	// writes to a stale primary).
	shard.Status.CurrentPrimary, shard.Status.Phase =
		deriveShardStatus(instances, shard.Spec.Replicas, before.CurrentPrimary != "", shard.Name+"-")
	// A same-lineage claimant dispute (or a pre-latch identity conflict)
	// means writes may be landing somewhere we cannot vouch for: publish no
	// primary at all until it is resolved. An identity blocker parking the
	// election is likewise not a Ready shard, whatever the ordinal health.
	if assessment.suppressPrimary {
		shard.Status.CurrentPrimary = ""
	}
	if assessment.suppressPrimary || assessment.blocked {
		shard.Status.Phase = pgshardv1alpha1.ShardDegraded
	}

	if cond := identityConsistentCondition(
		&assessment, shard.Status.SystemID != "", parseErr, shard.Generation); cond != nil {
		apimeta.SetStatusCondition(&shard.Status.Conditions, *cond)
	}

	// Drive the target/current-primary handshake: track the healthy primary,
	// or elect and promote a replacement when it is gone. A pre-latch identity
	// conflict or an unparseable latch disables the election entirely (fail
	// closed) until a human resolves it; the condition above says why.
	fencedPods := make(map[string]bool, len(assessment.unrecognized))
	for _, pod := range assessment.unrecognized {
		fencedPods[pod] = true
	}
	// A failover error (failed Promote, conflicting status write) must not
	// abort the poll's safety output: the caller still needs the fenced set
	// and the status this poll computed, so the write below is attempted and
	// the error propagated afterwards.
	var failoverErr error
	if parseErr == nil && !assessment.conflict {
		failoverErr = r.reconcileFailover(ctx, shard, assessment.kept)
	}

	// Ready replicas, bound to their polled pod objects: the only pods a
	// scale-down may prune (never the primary, never an unpollable pod).
	var readyReplicas []corev1.Pod
	for i := range instances {
		if s := instances[i]; s.Ready && s.Role == roleLabelReplica && s.Pod != shard.Status.CurrentPrimary {
			readyReplicas = append(readyReplicas, polled[i])
		}
	}

	// Only write when the status actually changed. The controller watches its
	// own resource, so an unconditional write on every poll (WAL LSNs advance
	// with every commit) would re-enqueue immediately and spin a hot loop
	// under write traffic.
	if !apiequality.Semantic.DeepEqual(before, &shard.Status) {
		if err := client.IgnoreNotFound(r.Status().Update(ctx, shard)); err != nil && failoverErr == nil {
			failoverErr = err
		}
	}
	return readyReplicas, fencedPods, failoverErr
}

// deriveShardStatus computes the current primary and phase from polled instance
// states. More than one instance reporting primary is split-brain: no primary
// is published (withholding -rw write routing) and the shard is Degraded. A
// shard that had a primary and now has none is Degraded (not Provisioning,
// which is the initial bring-up only).
func deriveShardStatus(
	instances []pgshardv1alpha1.InstanceState, replicas int32, hadPrimary bool, namePrefix string,
) (string, pgshardv1alpha1.ShardPhase) {
	currentPrimary, primaries, anyReady := "", 0, 0
	desiredReady := map[int32]bool{}
	for _, s := range instances {
		// A pod counts toward readiness only with a confirmed role: a ready pod
		// whose role the agent has not confirmed must not mask a failed desired
		// instance or drive the shard to Ready (which would enable pruning).
		if s.Ready && (s.Role == roleLabelPrimary || s.Role == roleLabelReplica) {
			anyReady++
			// Only desired ordinals (< replicas) count toward readiness, so a
			// failed desired instance is never masked by extra ready pods that
			// are pending a scale-down prune.
			if ord, ok := ordinalOf(s.Pod, namePrefix); ok && ord < replicas {
				desiredReady[ord] = true
			}
		}
		if s.Role == roleLabelPrimary {
			currentPrimary = s.Pod
			primaries++
		}
	}
	switch {
	case primaries > 1:
		return "", pgshardv1alpha1.ShardDegraded
	case int32(len(desiredReady)) == replicas && currentPrimary != "":
		// Every desired ordinal is ready; extra ready pods (awaiting prune) are
		// fine — an over-provisioned but healthy shard must reach Ready so that
		// pruning runs, else it would be stuck Degraded forever.
		return currentPrimary, pgshardv1alpha1.ShardReady
	case anyReady == 0 && !hadPrimary:
		// Initial bring-up: keep a uniquely-confirmed (but not-yet-ready)
		// primary so its label is set; the Service's readiness gate withholds
		// traffic until it is actually ready.
		return currentPrimary, pgshardv1alpha1.ShardProvisioning
	default:
		return currentPrimary, pgshardv1alpha1.ShardDegraded
	}
}

func lsnValue(lsn *pgshardv1.Lsn) uint64 {
	if lsn == nil {
		return 0
	}
	return lsn.Value
}

func (r *PgShardShardReconciler) pollAgent(
	ctx context.Context, host string,
) (*pgshardv1.InstanceStatus, error) {
	agent, err := r.agentClient(host, agentPort)
	if err != nil {
		return nil, err
	}
	resp, err := agent.GetStatus(ctx, &pgshardv1.GetStatusRequest{})
	if err != nil {
		return nil, err
	}
	if resp.GetStatus() == nil {
		return nil, fmt.Errorf("agent %s returned an empty status", host)
	}
	return resp.GetStatus(), nil
}

func lsnString(lsn *pgshardv1.Lsn) string {
	if lsn == nil {
		return ""
	}
	return fmt.Sprintf("%X/%X", lsn.Value>>32, uint32(lsn.Value))
}

// syncRoleLabels moves the primary/replica labels to match polled reality,
// which is what points the -rw/-ro services. Only the confirmed primary is
// labeled primary and only a confirmed standby is labeled replica; a pod whose
// role is unconfirmed this cycle is left unlabeled so it is in neither -rw nor
// -ro. A possible writer (an unreachable or not-yet-classified ex-primary) must
// not receive read traffic on -ro.
func (r *PgShardShardReconciler) syncRoleLabels(
	ctx context.Context, shard *pgshardv1alpha1.PgShardShard, fenced map[string]bool,
) error {
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(shardSelector(shard))); err != nil {
		return err
	}
	confirmedStandby := map[string]bool{}
	for _, s := range shard.Status.Instances {
		if s.Role == roleLabelReplica {
			confirmedStandby[s.Pod] = true
		}
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, shard) {
			continue
		}
		want := ""
		switch {
		case pod.Name == shard.Status.CurrentPrimary:
			want = roleLabelPrimary
		case confirmedStandby[pod.Name]:
			want = roleLabelReplica
		case pod.Labels[labelRole] == roleLabelReplica && !fenced[pod.Name]:
			// Role unconfirmed this cycle (a transient poll blip), but the pod was
			// a confirmed standby: keep it in -ro rather than flap a healthy replica
			// out of read routing on a single hiccup. A possible writer is never
			// kept sticky — a demoted or unreachable ex-primary still carries the
			// primary label here (not replica), so it falls through to unlabeled.
			want = roleLabelReplica
		}
		if pod.Labels[labelRole] == want {
			continue
		}
		patched := pod.DeepCopy()
		if want == "" {
			delete(patched.Labels, labelRole)
		} else {
			patched.Labels[labelRole] = want
		}
		if err := r.Patch(ctx, patched, client.MergeFrom(pod)); err != nil {
			return err
		}
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PgShardShardReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Never silently fall back to the plaintext pool: production must wire a
	// credentialed agentclient.Pool (tests inject dialAgent). An insecure
	// default would poll agents unauthenticated.
	if r.Agents == nil && r.dialAgent == nil {
		return fmt.Errorf("shard controller requires an agent client Pool or an injected dialer")
	}
	return ctrl.NewControllerManagedBy(mgr).
		// GenerationChangedPredicate on the shard itself: the controller writes
		// the shard's status every poll (advancing WAL LSNs), which does not bump
		// generation. Without this filter each self-write re-enqueues immediately
		// and hot-loops under write traffic; periodic polling comes from
		// RequeueAfter, pod/service reactions from the Owns watches.
		For(&pgshardv1alpha1.PgShardShard{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&corev1.Pod{}).
		Owns(&corev1.Service{}).
		Named("pgshardshard").
		Complete(r)
}
