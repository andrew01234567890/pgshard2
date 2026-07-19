package controller

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
)

var _ = Describe("PgShardRouting compilation", func() {
	const ns = "default"
	const (
		rc3Split = "rc3-split"
		rc7Pod   = "rc7-p0"
		rc8Split = "rc8-split"
	)

	reconcile := func(cluster string) {
		r := &PgShardRoutingReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := r.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Name: cluster, Namespace: ns},
		})
		Expect(err).NotTo(HaveOccurred())
	}
	getRouting := func(cluster string) pgshardv1alpha1.PgShardRouting {
		var rt pgshardv1alpha1.PgShardRouting
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: cluster, Namespace: ns}, &rt)).To(Succeed())
		return rt
	}

	// makeShard models the REAL placed-shard shape: the shard carries only
	// NodeRef (its status mirror holds no instances); the NODE owns the pod
	// and carries the instance view; the pod is controlled by the node.
	makeShard := func(cluster, name, start, end, nodeName, podName, ip string) {
		node := &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName, Namespace: ns},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 1},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: ns},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "pg", Image: "pg"}},
			},
		}
		Expect(controllerutil.SetControllerReference(node, pod, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = ip
		pod.Status.Phase = corev1.PodRunning
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		node.Status.Phase = pgshardv1alpha1.NodeReady
		node.Status.CurrentPrimary = podName
		node.Status.Instances = []pgshardv1alpha1.InstanceState{
			{Pod: podName, Ready: true, Role: roleLabelPrimary},
		}
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())

		s := &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef: cluster,
				KeyRange:   pgshardv1alpha1.KeyRange{Start: start, End: end},
				Replicas:   1,
				Serving:    true,
				Role:       pgshardv1alpha1.ShardRoleData,
				NodeRef:    nodeName,
			},
		}
		Expect(k8sClient.Create(ctx, s)).To(Succeed())
		// The shard controller publishes CurrentPrimary only after the full
		// database verification chain passes; mirror the whole chain here as
		// production records it (instances stay on the node).
		s.Status.CurrentPrimary = podName
		s.Status.DatabaseNode = nodeName
		s.Status.DatabaseNodeUID = string(node.UID)
		s.Status.DatabasePodUID = string(pod.UID)
		apimeta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
			Type: shardDatabaseReadyCondition, Status: metav1.ConditionTrue,
			Reason: testProvisionedReason, Message: testProvisionedReason,
		})
		Expect(k8sClient.Status().Update(ctx, s)).To(Succeed())
	}

	newCluster := func(name string) {
		cl := &pgshardv1alpha1.PgShardCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardClusterSpec{
				Postgres: pgshardv1alpha1.PostgresSpec{Version: "18"},
				Shards:   pgshardv1alpha1.ShardsSpec{InitialCount: 2},
			},
		}
		Expect(k8sClient.Create(ctx, cl)).To(Succeed())
	}

	It("compiles routing with endpoints and bumps epochs monotonically", func() {
		newCluster("rc1")
		makeShard("rc1", "rc1-a", "", "80", "rc1-n0", "rc1-p0", "127.0.1.1")
		makeShard("rc1", "rc1-b", "80", "", "rc1-n1", "rc1-p1", "127.0.1.2")

		reconcile("rc1")
		rt := getRouting("rc1")
		Expect(rt.Spec.Epoch).To(Equal(int64(1)))
		Expect(rt.Spec.TopologyGeneration).To(Equal(int64(1)))
		Expect(rt.Spec.Shards).To(HaveLen(2))
		Expect(rt.Spec.Shards[0].Primary).NotTo(BeNil())
		Expect(rt.Spec.Shards[0].Primary.Host).To(Equal("127.0.1.1"))

		// The routing object must belong to the cluster for cascade cleanup.
		var cl pgshardv1alpha1.PgShardCluster
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rc1", Namespace: ns}, &cl)).To(Succeed())
		Expect(metav1.IsControlledBy(&rt, &cl)).To(BeTrue())
		Expect(apimeta.IsStatusConditionTrue(cl.Status.Conditions, "RoutingCompiled")).To(BeTrue())

		// An identical recompile must NOT bump the epoch.
		reconcile("rc1")
		Expect(getRouting("rc1").Spec.Epoch).To(Equal(int64(1)))

		// An endpoint move bumps the epoch but NOT the topology generation.
		var pod corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rc1-p0", Namespace: ns}, &pod)).To(Succeed())
		pod.Status.PodIP = "127.0.1.9"
		Expect(k8sClient.Status().Update(ctx, &pod)).To(Succeed())
		reconcile("rc1")
		rt = getRouting("rc1")
		Expect(rt.Spec.Epoch).To(Equal(int64(2)))
		Expect(rt.Spec.TopologyGeneration).To(Equal(int64(1)))
	})

	It("keeps the last good routing when a compile fails", func() {
		newCluster("rc2")
		makeShard("rc2", "rc2-a", "", "80", "rc2-n0", "rc2-p0", "127.0.2.1")
		makeShard("rc2", "rc2-b", "80", "", "rc2-n1", "rc2-p1", "127.0.2.2")
		reconcile("rc2")
		Expect(getRouting("rc2").Spec.Epoch).To(Equal(int64(1)))

		// A serving flip that breaks the keyspace partition must refuse to
		// compile and leave the published routing untouched.
		var b pgshardv1alpha1.PgShardShard
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rc2-b", Namespace: ns}, &b)).To(Succeed())
		b.Spec.Serving = false
		Expect(k8sClient.Update(ctx, &b)).To(Succeed())
		reconcile("rc2")

		rt := getRouting("rc2")
		Expect(rt.Spec.Epoch).To(Equal(int64(1)), "a refused compile must not publish")
		var cl pgshardv1alpha1.PgShardCluster
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rc2", Namespace: ns}, &cl)).To(Succeed())
		cond := apimeta.FindStatusCondition(cl.Status.Conditions, "RoutingCompiled")
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("CompileFailed"))
	})

	It("emits and withdraws a cutover gate from a CuttingOver reshard", func() {
		newCluster("rc3")
		makeShard("rc3", "rc3-a", "", "80", "rc3-n0", "rc3-p0", "127.0.3.1")
		makeShard("rc3", "rc3-b", "80", "", "rc3-n1", "rc3-p1", "127.0.3.2")
		reconcile("rc3")

		rs := &pgshardv1alpha1.PgShardReshard{
			ObjectMeta: metav1.ObjectMeta{Name: rc3Split, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardReshardSpec{
				ClusterRef:  "rc3",
				SourceShard: "rc3-a",
				TargetRanges: []pgshardv1alpha1.KeyRange{
					{Start: "", End: "40"}, {Start: "40", End: "80"},
				},
			},
		}
		Expect(k8sClient.Create(ctx, rs)).To(Succeed())
		deadline := metav1.NewTime(time.Now().Add(time.Minute).Truncate(time.Second))
		rs.Status.Phase = pgshardv1alpha1.ReshardCuttingOver
		rs.Status.CutoverGateDeadline = &deadline
		Expect(k8sClient.Status().Update(ctx, rs)).To(Succeed())

		reconcile("rc3")
		rt := getRouting("rc3")
		Expect(rt.Spec.Gates).To(HaveLen(1))
		Expect(rt.Spec.Gates[0].ID).To(Equal("reshard-rc3-split"))
		Expect(rt.Spec.Gates[0].Mode).To(Equal("bufferWrites"))
		Expect(rt.Spec.Gates[0].Match.KeyRanges).To(ConsistOf(
			pgshardv1alpha1.KeyRange{Start: "", End: "80"}))
		gatedEpoch := rt.Spec.Epoch

		// A phase transition alone must NOT withdraw the gate: a reordered
		// status event could otherwise publish a fresh ungated epoch still
		// carrying the pre-switch topology and re-admit writes.
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rc3Split, Namespace: ns}, rs)).To(Succeed())
		rs.Status.Phase = pgshardv1alpha1.ReshardSwitchedForward
		Expect(k8sClient.Status().Update(ctx, rs)).To(Succeed())
		reconcile("rc3")
		Expect(getRouting("rc3").Spec.Gates).To(HaveLen(1),
			"the gate follows the deadline field, never the phase")

		// Clearing the FIELD (the cutover machine does this only after
		// observing the switched serving set) withdraws the gate.
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rc3Split, Namespace: ns}, rs)).To(Succeed())
		rs.Status.CutoverGateDeadline = nil
		Expect(k8sClient.Status().Update(ctx, rs)).To(Succeed())
		reconcile("rc3")
		rt = getRouting("rc3")
		Expect(rt.Spec.Gates).To(BeEmpty())
		Expect(rt.Spec.Epoch).To(BeNumerically(">", gatedEpoch))
	})

	It("refuses ungated routing while a committed switch's source still serves", func() {
		newCluster("rc5")
		makeShard("rc5", "rc5-a", "", "80", "rc5-n0", "rc5-p0", "127.0.5.1")
		makeShard("rc5", "rc5-b", "80", "", "rc5-n1", "rc5-p1", "127.0.5.2")
		reconcile("rc5")

		rs := &pgshardv1alpha1.PgShardReshard{
			ObjectMeta: metav1.ObjectMeta{Name: "rc5-split", Namespace: ns},
			Spec: pgshardv1alpha1.PgShardReshardSpec{
				ClusterRef:  "rc5",
				SourceShard: "rc5-a",
				TargetRanges: []pgshardv1alpha1.KeyRange{
					{Start: "", End: "40"}, {Start: "40", End: "80"},
				},
			},
		}
		Expect(k8sClient.Create(ctx, rs)).To(Succeed())
		deadline := metav1.NewTime(time.Now().Add(time.Minute).Truncate(time.Second))
		rs.Status.Phase = pgshardv1alpha1.ReshardCuttingOver
		rs.Status.CutoverGateDeadline = &deadline
		Expect(k8sClient.Status().Update(ctx, rs)).To(Succeed())
		reconcile("rc5")
		gatedEpoch := getRouting("rc5").Spec.Epoch
		Expect(getRouting("rc5").Spec.Gates).To(HaveLen(1))

		// The crash window: switch committed, gate field cleared, but the
		// serving flip never landed. Publishing ungated routing would
		// re-admit writes to a source the targets snapshotted past.
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rc5-split", Namespace: ns}, rs)).To(Succeed())
		rs.Status.SwitchCommitted = true
		rs.Status.CutoverGateDeadline = nil
		Expect(k8sClient.Status().Update(ctx, rs)).To(Succeed())
		reconcile("rc5")

		rt := getRouting("rc5")
		Expect(rt.Spec.Epoch).To(Equal(gatedEpoch), "the last good gated routing must stand")
		Expect(rt.Spec.Gates).To(HaveLen(1))
		var cl pgshardv1alpha1.PgShardCluster
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rc5", Namespace: ns}, &cl)).To(Succeed())
		cond := apimeta.FindStatusCondition(cl.Status.Conditions, "RoutingCompiled")
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal("GateInconsistent"))
	})

	It("refuses to compile when an active gate's source shard is gone", func() {
		newCluster("rc6")
		makeShard("rc6", "rc6-a", "", "80", "rc6-n0", "rc6-p0", "127.0.6.1")
		makeShard("rc6", "rc6-b", "80", "", "rc6-n1", "rc6-p1", "127.0.6.2")
		reconcile("rc6")
		epoch := getRouting("rc6").Spec.Epoch

		rs := &pgshardv1alpha1.PgShardReshard{
			ObjectMeta: metav1.ObjectMeta{Name: "rc6-split", Namespace: ns},
			Spec: pgshardv1alpha1.PgShardReshardSpec{
				ClusterRef:  "rc6",
				SourceShard: "rc6-vanished",
				TargetRanges: []pgshardv1alpha1.KeyRange{
					{Start: "", End: "40"}, {Start: "40", End: "80"},
				},
			},
		}
		Expect(k8sClient.Create(ctx, rs)).To(Succeed())
		deadline := metav1.NewTime(time.Now().Add(time.Minute).Truncate(time.Second))
		rs.Status.CutoverGateDeadline = &deadline
		Expect(k8sClient.Status().Update(ctx, rs)).To(Succeed())
		reconcile("rc6")

		Expect(getRouting("rc6").Spec.Epoch).To(Equal(epoch),
			"an active gate with no source must never compile away silently")
	})

	It("drops the primary when a same-named node incarnation replaces the attested one", func() {
		newCluster("rc7")
		makeShard("rc7", "rc7-a", "", "80", "rc7-n0", rc7Pod, "127.0.7.1")
		makeShard("rc7", "rc7-b", "80", "", "rc7-n1", "rc7-p1", "127.0.7.2")
		reconcile("rc7")
		Expect(getRouting("rc7").Spec.Shards[0].Primary).NotTo(BeNil())

		// Recreate node AND pod under the same names: new UIDs, ready
		// instance view — but the shard's attestation names the OLD
		// incarnation. No primary may be published until re-verification.
		var node pgshardv1alpha1.PgShardNode
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rc7-n0", Namespace: ns}, &node)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &node)).To(Succeed())
		var pod corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rc7Pod, Namespace: ns}, &pod)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &pod)).To(Succeed())

		fresh := &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: "rc7-n0", Namespace: ns},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 1},
		}
		Expect(k8sClient.Create(ctx, fresh)).To(Succeed())
		freshPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: rc7Pod, Namespace: ns},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "pg", Image: "pg"}},
			},
		}
		Expect(controllerutil.SetControllerReference(fresh, freshPod, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, freshPod)).To(Succeed())
		freshPod.Status.PodIP = "127.0.7.9"
		Expect(k8sClient.Status().Update(ctx, freshPod)).To(Succeed())
		fresh.Status.Phase = pgshardv1alpha1.NodeReady
		fresh.Status.CurrentPrimary = rc7Pod
		fresh.Status.Instances = []pgshardv1alpha1.InstanceState{
			{Pod: rc7Pod, Ready: true, Role: "primary"},
		}
		Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())

		reconcile("rc7")
		Expect(getRouting("rc7").Spec.Shards[0].Primary).To(BeNil(),
			"stale attestation must not bind to a same-named replacement incarnation")
	})

	It("pins a gating reshard with a finalizer until nothing load-bearing remains", func() {
		newCluster("rc8")
		makeShard("rc8", "rc8-a", "", "80", "rc8-n0", "rc8-p0", "127.0.8.1")
		makeShard("rc8", "rc8-b", "80", "", "rc8-n1", "rc8-p1", "127.0.8.2")
		reconcile("rc8")

		rs := &pgshardv1alpha1.PgShardReshard{
			ObjectMeta: metav1.ObjectMeta{Name: rc8Split, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardReshardSpec{
				ClusterRef:  "rc8",
				SourceShard: "rc8-a",
				TargetRanges: []pgshardv1alpha1.KeyRange{
					{Start: "", End: "40"}, {Start: "40", End: "80"},
				},
			},
		}
		Expect(k8sClient.Create(ctx, rs)).To(Succeed())
		deadline := metav1.NewTime(time.Now().Add(time.Minute).Truncate(time.Second))
		rs.Status.CutoverGateDeadline = &deadline
		Expect(k8sClient.Status().Update(ctx, rs)).To(Succeed())
		reconcile("rc8")
		Expect(getRouting("rc8").Spec.Gates).To(HaveLen(1))

		// Deleting the reshard must NOT delete the only durable gate record:
		// the finalizer keeps it, and the gate stays published.
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rc8Split, Namespace: ns}, rs)).To(Succeed())
		Expect(k8sClient.Delete(ctx, rs)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rc8Split, Namespace: ns}, rs)).To(Succeed())
		Expect(rs.DeletionTimestamp).NotTo(BeNil())
		reconcile("rc8")
		Expect(getRouting("rc8").Spec.Gates).To(HaveLen(1),
			"a deleted-but-finalized reshard's gate must stand")

		// Clearing the gate (rollback semantics: SwitchCommitted false)
		// releases the finalizer and the object goes away; the gate follows.
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rc8Split, Namespace: ns}, rs)).To(Succeed())
		rs.Status.CutoverGateDeadline = nil
		Expect(k8sClient.Status().Update(ctx, rs)).To(Succeed())
		reconcile("rc8")
		// The SAME reconcile that releases the finalizer publishes the
		// withdrawal — no follow-up event required.
		Expect(getRouting("rc8").Spec.Gates).To(BeEmpty())
		Expect(apierrors.IsNotFound(
			k8sClient.Get(ctx, types.NamespacedName{Name: rc8Split, Namespace: ns}, rs))).To(BeTrue())
	})

	It("never publishes an endpoint for a pod another node incarnation owns", func() {
		newCluster("rc4")
		makeShard("rc4", "rc4-a", "", "80", "rc4-n0", "rc4-p0", "127.0.4.1")
		makeShard("rc4", "rc4-b", "80", "", "rc4-n1", "rc4-p1", "127.0.4.2")
		reconcile("rc4")
		rt := getRouting("rc4")
		Expect(rt.Spec.Shards[0].Primary).NotTo(BeNil())

		// Replace the pod under a DIFFERENT owner while the node status
		// still names it: stale evidence must not bind to the new pod.
		var pod corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rc4-p0", Namespace: ns}, &pod)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &pod)).To(Succeed())
		var otherNode pgshardv1alpha1.PgShardNode
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rc4-n1", Namespace: ns}, &otherNode)).To(Succeed())
		replacement := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "rc4-p0", Namespace: ns},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "pg", Image: "pg"}},
			},
		}
		Expect(controllerutil.SetControllerReference(&otherNode, replacement, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, replacement)).To(Succeed())
		replacement.Status.PodIP = "127.0.4.9"
		Expect(k8sClient.Status().Update(ctx, replacement)).To(Succeed())

		reconcile("rc4")
		rt = getRouting("rc4")
		Expect(rt.Spec.Shards[0].Primary).To(BeNil(),
			"a foreign pod's address must never be published as this shard's primary")
	})
})
