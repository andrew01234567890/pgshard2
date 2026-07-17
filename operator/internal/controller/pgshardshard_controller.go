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

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	// to the connection pool; tests inject fakes.
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
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete

func (r *PgShardShardReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var shard pgshardv1alpha1.PgShardShard
	if err := r.Get(ctx, req.NamespacedName, &shard); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
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

	if err := r.aggregateStatus(ctx, &shard); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.syncRoleLabels(ctx, &shard); err != nil {
		return ctrl.Result{}, err
	}
	// Prune AFTER polling so it acts on this reconcile's fresh instance states —
	// never deleting a just-promoted or temporarily-unpollable writer.
	if err := r.pruneExcessInstances(ctx, &shard); err != nil {
		return ctrl.Result{}, err
	}

	interval := r.StatusPollInterval
	if interval == 0 {
		interval = 10 * time.Second
	}
	return ctrl.Result{RequeueAfter: interval}, nil
}

func instanceName(shard *pgshardv1alpha1.PgShardShard, ordinal int32) string {
	return fmt.Sprintf("%s-%d", shard.Name, ordinal)
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
		{"-rw", withRole(shardSelector(shard), roleLabelPrimary), false},
		{"-ro", withRole(shardSelector(shard), roleLabelReplica), false},
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
	ctx context.Context, shard *pgshardv1alpha1.PgShardShard,
) error {
	// Only prune an excess ordinal that THIS reconcile positively confirmed is a
	// ready replica (from the just-completed poll, role replica, not the
	// primary). A just-promoted or temporarily-unpollable excess pod is left in
	// place so scale-down never deletes the writer.
	confirmedReplica := map[string]bool{}
	for _, s := range shard.Status.Instances {
		if s.Ready && s.Role == roleLabelReplica && s.Pod != shard.Status.CurrentPrimary {
			confirmedReplica[s.Pod] = true
		}
	}

	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(shardSelector(shard))); err != nil {
		return err
	}
	prefix := shard.Name + "-"
	for i := range pods.Items {
		pod := &pods.Items[i]
		ord, ok := ordinalOf(pod.Name, prefix)
		if !ok || ord < shard.Spec.Replicas {
			continue
		}
		// Own it AND have confirmed it a ready replica this reconcile.
		if !metav1.IsControlledBy(pod, shard) || !confirmedReplica[pod.Name] {
			continue
		}
		// UID precondition: never delete a same-name pod recreated between the
		// list and the delete.
		uid := pod.UID
		if err := r.Delete(ctx, pod, client.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) {
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
				Name:    "postgres",
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
					{Name: "postgres", ContainerPort: 5432},
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
					{Name: "data", MountPath: "/var/lib/postgresql/data"},
					{Name: "config", MountPath: "/etc/pgshard/config"},
				},
			}},
			Volumes: []corev1.Volume{
				{Name: agentVolumeName, VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				}},
				{Name: "data", VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: name + "-data",
					},
				}},
				{Name: "config", VolumeSource: corev1.VolumeSource{
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
			Name: "wal", MountPath: "/var/lib/postgresql/wal",
		})
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: "wal", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: name + "-wal",
				},
			},
		})
	}
	return pod
}

// aggregateStatus polls every instance's agent and writes the shard status
// (single status writer). Unreachable agents mark the instance not ready.
func (r *PgShardShardReconciler) aggregateStatus(
	ctx context.Context, shard *pgshardv1alpha1.PgShardShard,
) error {
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(shardSelector(shard))); err != nil {
		return err
	}

	before := shard.Status.DeepCopy()

	instances := make([]pgshardv1alpha1.InstanceState, 0, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, shard) {
			// A foreign pod that merely matches our labels must not be polled
			// into status (it could otherwise be preserved as CurrentPrimary).
			continue
		}
		state := pgshardv1alpha1.InstanceState{Pod: pod.Name, Role: roleLabelReplica}
		if pod.Status.PodIP != "" {
			pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			status, err := r.pollAgent(pollCtx, pod.Status.PodIP)
			cancel()
			if err == nil {
				state.Ready = status.Ready
				if status.Role == pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY {
					state.Role = roleLabelPrimary
				}
				state.WalWriteLSN = lsnString(status.WalWriteLsn)
				state.WalReplayLSN = lsnString(status.WalReplayLsn)
			}
		}
		instances = append(instances, state)
	}

	shard.Status.Instances = instances
	// CurrentPrimary is assigned unconditionally, so a demoted/unreachable
	// primary with no confirmed replacement is CLEARED (else -rw keeps routing
	// writes to a stale primary).
	shard.Status.CurrentPrimary, shard.Status.Phase =
		deriveShardStatus(instances, shard.Spec.Replicas, before.CurrentPrimary != "")

	// Only write when the status actually changed. The controller watches its
	// own resource, so an unconditional write on every poll (WAL LSNs advance
	// with every commit) would re-enqueue immediately and spin a hot loop
	// under write traffic.
	if apiequality.Semantic.DeepEqual(before, &shard.Status) {
		return nil
	}
	return client.IgnoreNotFound(r.Status().Update(ctx, shard))
}

// deriveShardStatus computes the current primary and phase from polled instance
// states. More than one instance reporting primary is split-brain: no primary
// is published (withholding -rw write routing) and the shard is Degraded. A
// shard that had a primary and now has none is Degraded (not Provisioning,
// which is the initial bring-up only).
func deriveShardStatus(
	instances []pgshardv1alpha1.InstanceState, replicas int32, hadPrimary bool,
) (string, pgshardv1alpha1.ShardPhase) {
	currentPrimary, primaries, ready := "", 0, 0
	for _, s := range instances {
		if s.Ready {
			ready++
		}
		if s.Role == roleLabelPrimary {
			currentPrimary = s.Pod
			primaries++
		}
	}
	switch {
	case primaries > 1:
		return "", pgshardv1alpha1.ShardDegraded
	case ready == int(replicas) && currentPrimary != "":
		return currentPrimary, pgshardv1alpha1.ShardReady
	case ready == 0 && !hadPrimary:
		// Initial bring-up: keep a uniquely-confirmed (but not-yet-ready)
		// primary so its label is set; the Service's readiness gate withholds
		// traffic until it is actually ready.
		return currentPrimary, pgshardv1alpha1.ShardProvisioning
	default:
		return currentPrimary, pgshardv1alpha1.ShardDegraded
	}
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
// which is what points the -rw/-ro services.
func (r *PgShardShardReconciler) syncRoleLabels(
	ctx context.Context, shard *pgshardv1alpha1.PgShardShard,
) error {
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(shardSelector(shard))); err != nil {
		return err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, shard) {
			continue
		}
		want := roleLabelReplica
		if pod.Name == shard.Status.CurrentPrimary {
			want = roleLabelPrimary
		}
		if pod.Labels[labelRole] != want {
			patched := pod.DeepCopy()
			patched.Labels[labelRole] = want
			if err := r.Patch(ctx, patched, client.MergeFrom(pod)); err != nil {
				return err
			}
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
