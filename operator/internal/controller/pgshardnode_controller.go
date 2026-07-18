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

// labelNode selects the pods, PVCs, and services of one physical node. A node
// is not scoped to a cluster (a shared node hosts several clusters' shard
// databases), so — unlike a shard — its objects carry no cluster label.
const labelNode = "pgshard.dev/node"

// PgShardNodeReconciler owns the physical objects of one node: pods, PVCs,
// services, and the aggregated status (the operator polls the in-pod agents;
// agents never write CRD status). Failover election is a separate controller.
type PgShardNodeReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Agents *agentclient.Pool
	Images ShardImages
	// StatusPollInterval is the requeue cadence for status aggregation.
	StatusPollInterval time.Duration
	// dialAgent resolves a pod address to an AgentService client. Defaults to
	// the connection pool; tests inject per-pod fakes.
	dialAgent func(host string, port int32) (pgshardv1.AgentServiceClient, error)
}

func (r *PgShardNodeReconciler) agentClient(host string, port int32) (pgshardv1.AgentServiceClient, error) {
	if r.dialAgent != nil {
		return r.dialAgent(host, port)
	}
	return r.Agents.Get(host, port)
}

// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardnodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardnodes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgshard.dev,resources=pgshardnodes/finalizers,verbs=update

func (r *PgShardNodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var node pgshardv1alpha1.PgShardNode
	if err := r.Get(ctx, req.NamespacedName, &node); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if node.Spec.Fenced {
		// Fencing is executed by agents (they keep PostgreSQL down); the
		// controller stops mutating pods while frozen.
		log.Info("node fenced; skipping pod reconcile")
		return ctrl.Result{}, nil
	}

	if r.Images.Agent == "" {
		return ctrl.Result{}, fmt.Errorf("node controller agent image is not configured")
	}
	if node.Spec.Image == "" && r.Images.Postgres == "" {
		return ctrl.Result{}, fmt.Errorf("no postgres image: set node.spec.image or the controller default")
	}

	if err := r.ensureServices(ctx, &node); err != nil {
		return ctrl.Result{}, err
	}
	for ordinal := int32(0); ordinal < node.Spec.Replicas; ordinal++ {
		if err := r.ensureInstance(ctx, &node, ordinal); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Role labels are synced even when aggregation errored mid-way (a failed
	// Promote or status write): the assessment may have just fenced an
	// instance, and leaving its -rw/-ro label standing until a lucky retry
	// would keep routing to data we already know is wrong.
	readyReplicas, fenced, aggErr := r.aggregateStatus(ctx, &node)
	if err := r.syncRoleLabels(ctx, &node, fenced); err != nil {
		return ctrl.Result{}, err
	}
	if aggErr != nil {
		return ctrl.Result{}, aggErr
	}
	// Prune only the pods this reconcile confirmed are ready replicas — never a
	// just-promoted, unpollable, or foreign pod.
	if err := r.pruneExcessInstances(ctx, &node, readyReplicas); err != nil {
		return ctrl.Result{}, err
	}

	interval := r.StatusPollInterval
	if interval == 0 {
		interval = 10 * time.Second
	}
	return ctrl.Result{RequeueAfter: interval}, nil
}

func nodeInstanceName(node *pgshardv1alpha1.PgShardNode, ordinal int32) string {
	return fmt.Sprintf("%s-%d", node.Name, ordinal)
}

func nodeSelector(node *pgshardv1alpha1.PgShardNode) map[string]string {
	return map[string]string{labelNode: node.Name}
}

// ensureServices maintains the CNPG-style trio plus stable per-pod DNS:
// -rw (primary only), -ro (replicas), -r (all instances), and a headless
// service so native logical-replication subscribers can pin a standby.
func (r *PgShardNodeReconciler) ensureServices(
	ctx context.Context, node *pgshardv1alpha1.PgShardNode,
) error {
	services := []struct {
		suffix   string
		selector map[string]string
		headless bool
	}{
		{svcSuffixRW, withRole(nodeSelector(node), roleLabelPrimary), false},
		{svcSuffixRO, withRole(nodeSelector(node), roleLabelReplica), false},
		{"-r", nodeSelector(node), false},
		{headlessSvcSuffix, nodeSelector(node), true},
	}
	for _, spec := range services {
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Name: node.Name + spec.suffix, Namespace: node.Namespace,
		}}
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
			svc.Labels = nodeSelector(node)
			svc.Spec.Selector = spec.selector
			svc.Spec.Ports = []corev1.ServicePort{{
				Name: portNamePostgres, Port: 5432, TargetPort: intstr.FromString(portNamePostgres),
			}}
			if spec.headless {
				svc.Spec.ClusterIP = corev1.ClusterIPNone
				// Publish pods before readiness so replication can bootstrap.
				svc.Spec.PublishNotReadyAddresses = true
			}
			return controllerutil.SetControllerReference(node, svc, r.Scheme)
		})
		if err != nil {
			return fmt.Errorf("service %s%s: %w", node.Name, spec.suffix, err)
		}
	}
	return nil
}

// ensureInstance creates the PVC and pod for one ordinal. Pods are never
// mutated in place here: config/image changes go through the rollout flow
// (replicas first, primary last via switchover), not blind recreation.
func (r *PgShardNodeReconciler) ensureInstance(
	ctx context.Context, node *pgshardv1alpha1.PgShardNode, ordinal int32,
) error {
	name := nodeInstanceName(node, ordinal)

	if err := r.ensurePVC(ctx, node, name+"-data"); err != nil {
		return err
	}
	if node.Spec.Storage != nil && node.Spec.Storage.WalSeparate {
		if err := r.ensurePVC(ctx, node, name+"-wal"); err != nil {
			return err
		}
	}

	var pod corev1.Pod
	err := r.Get(ctx, client.ObjectKey{Namespace: node.Namespace, Name: name}, &pod)
	if err == nil {
		if !metav1.IsControlledBy(&pod, node) {
			return fmt.Errorf("pod %s exists but is not controlled by node %s", name, node.Name)
		}
		// A config/image/storage change (including toggling walSeparate on an
		// existing instance) is rolled out separately — replicas first, primary
		// last via switchover — never by blind recreation here.
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	desired := r.instancePod(node, ordinal)
	if err := controllerutil.SetControllerReference(node, desired, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, desired); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("pod %s: %w", name, err)
	}
	return nil
}

// ensurePVC creates a node PVC if absent. PVCs deliberately carry no owner
// reference: data outlives pod churn, and deletion is an explicit decommission
// step — never on scale-down or pod recreation.
func (r *PgShardNodeReconciler) ensurePVC(
	ctx context.Context, node *pgshardv1alpha1.PgShardNode, pvcName string,
) error {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: pvcName, Namespace: node.Namespace,
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
	if node.Spec.Storage != nil {
		size = node.Spec.Storage.Size
		if node.Spec.Storage.StorageClass != "" {
			storageClass = &node.Spec.Storage.StorageClass
		}
	}
	pvc.Labels = nodeSelector(node)
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
// reused if the node scales back up.
func (r *PgShardNodeReconciler) pruneExcessInstances(
	ctx context.Context, node *pgshardv1alpha1.PgShardNode, candidates []corev1.Pod,
) error {
	// Only scale down a fully-healthy node. A promotion always drives the node
	// through a non-Ready phase, so gating on Ready keeps pruning out of the
	// window in which a candidate replica could be promoted after polling. A
	// planned switchover's coordinated decommission is the failover controller's
	// responsibility, not raced here.
	if node.Status.Phase != pgshardv1alpha1.NodeReady {
		return nil
	}
	// candidates are exactly the pods aggregateStatus confirmed are ready
	// replicas this reconcile, so a just-promoted, unpollable, foreign, or
	// same-name-replacement pod is never deleted. PVCs are retained for data
	// safety and reused on scale-up.
	prefix := node.Name + "-"
	for i := range candidates {
		pod := &candidates[i]
		ord, ok := ordinalOf(pod.Name, prefix)
		if !ok || ord < node.Spec.Replicas {
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

func (r *PgShardNodeReconciler) instancePod(
	node *pgshardv1alpha1.PgShardNode, ordinal int32,
) *corev1.Pod {
	name := nodeInstanceName(node, ordinal)
	postgresImage := node.Spec.Image
	if postgresImage == "" {
		postgresImage = r.Images.Postgres
	}
	labels := nodeSelector(node)
	labels[labelRole] = roleLabelReplica // promoted after status polls

	var resources corev1.ResourceRequirements
	if node.Spec.Resources != nil {
		resources = *node.Spec.Resources
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: node.Namespace,
			Labels:    labels,
			Annotations: map[string]string{
				"pgshard.dev/config-hash": node.Spec.PostgresConfigHash,
			},
		},
		Spec: corev1.PodSpec{
			Hostname:  name,
			Subdomain: node.Name + headlessSvcSuffix,
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
					// A node hosts many shard databases, so the agent's identity
					// is the node, not one cluster/shard; per-database placement
					// is delivered separately.
					{Name: "PGSHARD_NODE", Value: node.Name},
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
				// Liveness carries the primary isolation self-fence: the agent
				// reports unhealthy only when it is a primary that can reach
				// neither the API server nor its replicas.
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
							Name: node.Name + "-postgres-config",
						},
					},
				}},
			},
		},
	}

	// storage.walSeparate mounts a dedicated WAL volume so WAL I/O does not
	// contend with the data volume.
	if node.Spec.Storage != nil && node.Spec.Storage.WalSeparate {
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

// aggregateStatus polls every controlled instance's agent, writes the node
// status (single status writer), and returns the controlled pods it confirmed
// are ready replicas this reconcile — the only pods a scale-down may prune,
// bound to their polled UID.
func (r *PgShardNodeReconciler) aggregateStatus(
	ctx context.Context, node *pgshardv1alpha1.PgShardNode,
) ([]corev1.Pod, map[string]bool, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(node.Namespace),
		client.MatchingLabels(nodeSelector(node))); err != nil {
		return nil, nil, err
	}

	before := node.Status.DeepCopy()

	instances := make([]pgshardv1alpha1.InstanceState, 0, len(pods.Items))
	polled := make([]corev1.Pod, 0, len(pods.Items))
	views := make([]instanceView, 0, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, node) {
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
				view.walReceiver = status.WalReceiverActive
				view.systemID = status.SystemId
				view.timeline = int32(status.Timeline)
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

	var expectedID uint64
	var parseErr error
	node.Status.SystemID, expectedID, node.Status.Timeline, parseErr = latchIdentity(
		views, before.CurrentPrimary, before.TargetPrimary,
		node.Status.SystemID, node.Status.Timeline)
	assessment := assessIdentity(views, identityInputs{
		systemID:  expectedID,
		timeline:  node.Status.Timeline,
		current:   before.CurrentPrimary,
		committed: before.TargetPrimary,
	})
	// An unrecognized instance's role is never believed: publishing a rogue
	// claimant would put CurrentPrimary — and the -rw write service — on data
	// this lineage does not own, and a fenced standby's replica role would
	// serve wrong reads via -ro.
	for i := range views {
		if assessment.rogue(views[i].pod) {
			instances[i].Role = ""
		}
	}
	if len(assessment.fenced) > 0 {
		logf.FromContext(ctx).Info("instances fenced from election or recognition",
			"node", node.Name, "instances", assessment.fenced)
	}

	node.Status.Instances = instances
	// CurrentPrimary is assigned unconditionally, so a demoted/unreachable
	// primary with no confirmed replacement is CLEARED (else -rw keeps routing
	// writes to a stale primary).
	node.Status.CurrentPrimary, node.Status.Phase =
		deriveNodeStatus(instances, node.Spec.Replicas, before.CurrentPrimary != "", node.Name+"-")
	// A same-lineage claimant dispute (or a pre-latch identity conflict)
	// means writes may be landing somewhere we cannot vouch for: publish no
	// primary at all until it is resolved. An identity blocker parking the
	// election is likewise not a Ready node, whatever the ordinal health.
	if assessment.suppressPrimary {
		node.Status.CurrentPrimary = ""
	}
	if assessment.suppressPrimary || assessment.blocked {
		node.Status.Phase = pgshardv1alpha1.NodeDegraded
	}

	if cond := identityConsistentCondition(
		&assessment, node.Status.SystemID != "", parseErr, node.Generation); cond != nil {
		apimeta.SetStatusCondition(&node.Status.Conditions, *cond)
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
		failoverErr = r.reconcileFailover(ctx, node, assessment.kept)
	}

	// Ready replicas, bound to their polled pod objects: the only pods a
	// scale-down may prune (never the primary, never an unpollable pod).
	var readyReplicas []corev1.Pod
	for i := range instances {
		if s := instances[i]; s.Ready && s.Role == roleLabelReplica && s.Pod != node.Status.CurrentPrimary {
			readyReplicas = append(readyReplicas, polled[i])
		}
	}

	// Only write when the status actually changed. The controller watches its
	// own resource, so an unconditional write on every poll (WAL LSNs advance
	// with every commit) would re-enqueue immediately and spin a hot loop under
	// write traffic.
	if !apiequality.Semantic.DeepEqual(before, &node.Status) {
		if err := client.IgnoreNotFound(r.Status().Update(ctx, node)); err != nil && failoverErr == nil {
			failoverErr = err
		}
	}
	return readyReplicas, fencedPods, failoverErr
}

// deriveNodeStatus computes the current primary and phase from polled instance
// states. More than one instance reporting primary is split-brain: no primary
// is published (withholding -rw write routing) and the node is Degraded. A node
// that had a primary and now has none is Degraded (not Provisioning, which is
// the initial bring-up only).
func deriveNodeStatus(
	instances []pgshardv1alpha1.InstanceState, replicas int32, hadPrimary bool, namePrefix string,
) (string, pgshardv1alpha1.NodePhase) {
	currentPrimary, primaries, anyReady := "", 0, 0
	desiredReady := map[int32]bool{}
	for _, s := range instances {
		// A pod counts toward readiness only with a confirmed role: a ready pod
		// whose role the agent has not confirmed must not mask a failed desired
		// instance or drive the node to Ready (which would enable pruning).
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
		return "", pgshardv1alpha1.NodeDegraded
	case int32(len(desiredReady)) == replicas && currentPrimary != "":
		// Every desired ordinal is ready; extra ready pods (awaiting prune) are
		// fine — an over-provisioned but healthy node must reach Ready so that
		// pruning runs, else it would be stuck Degraded forever.
		return currentPrimary, pgshardv1alpha1.NodeReady
	case anyReady == 0 && !hadPrimary:
		// Initial bring-up: keep a uniquely-confirmed (but not-yet-ready)
		// primary so its label is set; the Service's readiness gate withholds
		// traffic until it is actually ready.
		return currentPrimary, pgshardv1alpha1.NodeProvisioning
	default:
		return currentPrimary, pgshardv1alpha1.NodeDegraded
	}
}

func (r *PgShardNodeReconciler) pollAgent(
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

// syncRoleLabels moves the primary/replica labels to match polled reality,
// which is what points the -rw/-ro services. Only the confirmed primary is
// labeled primary and only a confirmed standby is labeled replica; a pod whose
// role is unconfirmed this cycle is left unlabeled so it is in neither -rw nor
// -ro. A possible writer (an unreachable or not-yet-classified ex-primary) must
// not receive read traffic on -ro.
func (r *PgShardNodeReconciler) syncRoleLabels(
	ctx context.Context, node *pgshardv1alpha1.PgShardNode, fenced map[string]bool,
) error {
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(node.Namespace),
		client.MatchingLabels(nodeSelector(node))); err != nil {
		return err
	}
	confirmedStandby := map[string]bool{}
	for _, s := range node.Status.Instances {
		if s.Role == roleLabelReplica {
			confirmedStandby[s.Pod] = true
		}
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, node) {
			continue
		}
		want := ""
		switch {
		case pod.Name == node.Status.CurrentPrimary:
			want = roleLabelPrimary
		case confirmedStandby[pod.Name]:
			want = roleLabelReplica
		case pod.Labels[labelRole] == roleLabelReplica && !fenced[pod.Name]:
			// Role unconfirmed this cycle (a transient poll blip), but the pod was
			// a confirmed standby: keep it in -ro rather than flap a healthy replica
			// out of read routing on a single hiccup. A possible writer is never
			// kept sticky — a demoted or unreachable ex-primary still carries the
			// primary label here (not replica), so it falls through to unlabeled —
			// and neither is an identity-fenced pod, whose reads are wrong data.
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
func (r *PgShardNodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Never silently fall back to the plaintext pool: production must wire a
	// credentialed agentclient.Pool (tests inject dialAgent). An insecure
	// default would poll agents unauthenticated.
	if r.Agents == nil && r.dialAgent == nil {
		return fmt.Errorf("node controller requires an agent client Pool or an injected dialer")
	}
	return ctrl.NewControllerManagedBy(mgr).
		// GenerationChangedPredicate on the node itself: the controller writes
		// the node's status every poll (advancing WAL LSNs), which does not bump
		// generation. Without this filter each self-write re-enqueues immediately
		// and hot-loops under write traffic; periodic polling comes from
		// RequeueAfter, pod/service reactions from the Owns watches.
		For(&pgshardv1alpha1.PgShardNode{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&corev1.Pod{}).
		Owns(&corev1.Service{}).
		Named("pgshardnode").
		Complete(r)
}
