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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

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
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
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

	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: name + "-data", Namespace: shard.Namespace,
	}}
	err := r.Get(ctx, client.ObjectKeyFromObject(pvc), pvc)
	if apierrors.IsNotFound(err) {
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
		// PVCs deliberately carry no owner reference: data outlives pod
		// churn, and deletion is an explicit decommission step.
		if err := r.Create(ctx, pvc); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("pvc %s: %w", pvc.Name, err)
		}
	} else if err != nil {
		return err
	}

	var pod corev1.Pod
	err = r.Get(ctx, client.ObjectKey{Namespace: shard.Namespace, Name: name}, &pod)
	if err == nil || !apierrors.IsNotFound(err) {
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

	return &corev1.Pod{
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
			Subdomain: shard.Name + "-pods",
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

	instances := make([]pgshardv1alpha1.InstanceState, 0, len(pods.Items))
	views := make([]instanceView, 0, len(pods.Items))
	currentPrimary := ""
	ready := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		state := pgshardv1alpha1.InstanceState{Pod: pod.Name, Role: "replica"}
		view := instanceView{pod: pod.Name, host: pod.Status.PodIP}
		if pod.Status.PodIP != "" {
			pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			status, err := r.pollAgent(pollCtx, pod.Status.PodIP)
			cancel()
			if err == nil {
				state.Ready = status.Ready
				view.ready = status.Ready
				view.receivedLSN = lsnValue(status.WalReceiveLsn)
				view.walReceiver = status.WalReceiverActive
				if status.Role == pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY {
					state.Role = "primary"
					view.isPrimary = true
					currentPrimary = pod.Name
				}
				state.WalWriteLSN = lsnString(status.WalWriteLsn)
				state.WalReplayLSN = lsnString(status.WalReplayLsn)
			}
		}
		if state.Ready {
			ready++
		}
		instances = append(instances, state)
		views = append(views, view)
	}

	shard.Status.Instances = instances
	if currentPrimary != "" {
		shard.Status.CurrentPrimary = currentPrimary
	}
	switch {
	case ready == int(shard.Spec.Replicas) && currentPrimary != "":
		shard.Status.Phase = pgshardv1alpha1.ShardReady
	case ready == 0:
		shard.Status.Phase = pgshardv1alpha1.ShardProvisioning
	default:
		shard.Status.Phase = pgshardv1alpha1.ShardDegraded
	}

	// A healthy primary clears any prior failover marker.
	if currentPrimary != "" && shard.Status.TargetPrimary == PendingFailoverMarker {
		shard.Status.TargetPrimary = currentPrimary
	}
	if err := r.reconcileFailover(ctx, shard, views); err != nil {
		return err
	}
	return client.IgnoreNotFound(r.Status().Update(ctx, shard))
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
	return resp.Status, nil
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
	if r.Agents == nil {
		r.Agents = agentclient.NewInsecurePool()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgshardv1alpha1.PgShardShard{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.Service{}).
		Named("pgshardshard").
		Complete(r)
}
