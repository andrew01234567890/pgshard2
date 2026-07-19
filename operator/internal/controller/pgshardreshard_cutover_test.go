package controller

import (
	"context"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
	"github.com/andrew01234567890/pgshard2/operator/test/fakes"
)

// foreignClaimReader is an uncached reader stub: it reports one named shard's
// cutover claim as held by a foreign reshard, simulating an API-server truth
// that a lagging informer cache has not yet caught up to. Every other read
// delegates to the wrapped client.
type foreignClaimReader struct {
	client.Reader
	shardName string
	holder    string
}

func (f foreignClaimReader) Get(
	ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption,
) error {
	if err := f.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if s, ok := obj.(*pgshardv1alpha1.PgShardShard); ok && s.Name == f.shardName {
		if s.Annotations == nil {
			s.Annotations = map[string]string{}
		}
		s.Annotations[cutoverClaimAnnotation] = f.holder
	}
	return nil
}

// committedSwitchReader is an uncached reader stub: it reports one named
// reshard as SwitchCommitted, simulating the API-server truth after a commit
// the informer cache has not yet observed. Every other read delegates.
type committedSwitchReader struct {
	client.Reader
	reshardName string
}

func (c committedSwitchReader) Get(
	ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption,
) error {
	if err := c.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if r, ok := obj.(*pgshardv1alpha1.PgShardReshard); ok && r.Name == c.reshardName {
		r.Status.SwitchCommitted = true
	}
	return nil
}

var _ = Describe("PgShardReshard cutover", func() {
	const (
		ns       = "default"
		srcIP    = "127.0.0.41"
		tgtIP    = "127.0.0.42"
		barrier  = uint64(0x5000)
		leaseSec = int32(1)
		staleUID = "stale-cluster-uid"
	)

	get := func(name string) pgshardv1alpha1.PgShardReshard {
		var got pgshardv1alpha1.PgShardReshard
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &got)).To(Succeed())
		return got
	}
	getShard := func(name string) pgshardv1alpha1.PgShardShard {
		var got pgshardv1alpha1.PgShardShard
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &got)).To(Succeed())
		return got
	}

	// cutoverSetup drives a reshard to ReadyToCutover on a shared-placement
	// cluster with a 1s write lease, returning the fakes and both reconcilers.
	cutoverSetup := func(base string) (*fakes.FakeAgent, *fakes.FakeAgent,
		func() (ctrl.Result, error), func(), []string, *PgShardReshardReconciler) {
		clusterName := "c" + base
		reshardName := "rco-" + base

		sourceAgent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(sourceAgent.Stop)
		sourceAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY)
		sourceAgent.SetWalWriteLsn(0x4000)
		sourceAgent.SetJournalLsn(barrier)
		targetAgent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(targetAgent.Stop)

		rr := &PgShardReshardReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			dialAgent: func(host string, _ int32) (pgshardv1.AgentServiceClient, error) {
				if host == srcIP {
					return sourceAgent.Client()
				}
				return targetAgent.Client()
			},
		}
		reconcile := func() (ctrl.Result, error) {
			return rr.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: reshardName, Namespace: ns},
			})
		}
		routingReconcile := func() {
			rt := &PgShardRoutingReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := rt.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: clusterName, Namespace: ns},
			})
			Expect(err).NotTo(HaveOccurred())
		}

		cl := &pgshardv1alpha1.PgShardCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardClusterSpec{
				Postgres:  pgshardv1alpha1.PostgresSpec{Version: "18"},
				Shards:    pgshardv1alpha1.ShardsSpec{InitialCount: 2},
				Placement: &pgshardv1alpha1.PlacementSpec{Mode: pgshardv1alpha1.PlacementShared},
				Router:    &pgshardv1alpha1.RouterSpec{WriteLeaseSeconds: leaseSec},
			},
		}
		Expect(k8sClient.Create(ctx, cl)).To(Succeed())
		cfg := &pgshardv1alpha1.PgShardTableConfig{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName + "-tables", Namespace: ns},
			Spec: pgshardv1alpha1.PgShardTableConfigSpec{
				ClusterRef: clusterName,
				Tables: []pgshardv1alpha1.TableEntry{
					{Name: ordersTable, Type: pgshardv1alpha1.TableSharded,
						ShardKeyColumn: customerIDCol, ShardKeyType: pgshardv1alpha1.ShardKeyInt},
				},
			},
		}
		Expect(k8sClient.Create(ctx, cfg)).To(Succeed())

		makeNode := func(nodeName, ip string) *corev1.Pod {
			node := &pgshardv1alpha1.PgShardNode{
				ObjectMeta: metav1.ObjectMeta{Name: nodeName, Namespace: ns},
				Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 1},
			}
			Expect(k8sClient.Create(ctx, node)).To(Succeed())
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: nodeName + "-0", Namespace: ns},
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
			node.Status.CurrentPrimary = pod.Name
			node.Status.Instances = []pgshardv1alpha1.InstanceState{
				{Pod: pod.Name, Ready: true, Role: roleLabelPrimary},
			}
			Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())
			return pod
		}
		verify := func(shardName, nodeName string, pod *corev1.Pod) {
			var shard pgshardv1alpha1.PgShardShard
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: shardName, Namespace: ns}, &shard)).To(Succeed())
			var node pgshardv1alpha1.PgShardNode
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName, Namespace: ns}, &node)).To(Succeed())
			shard.Status.CurrentPrimary = pod.Name
			shard.Status.DatabaseNode = nodeName
			shard.Status.DatabaseNodeUID = string(node.UID)
			shard.Status.DatabasePodUID = string(pod.UID)
			apimeta.SetStatusCondition(&shard.Status.Conditions, metav1.Condition{
				Type: shardDatabaseReadyCondition, Status: metav1.ConditionTrue,
				Reason: testProvisionedReason, Message: testProvisionedReason,
			})
			Expect(k8sClient.Status().Update(ctx, &shard)).To(Succeed())
		}

		// Flanking serving shards so the cluster's serving set partitions the
		// FULL keyspace before, during, and after the 40-80 split.
		for _, f := range []struct{ name, start, end string }{
			{clusterName + "-lo", "", "40"}, {clusterName + "-hi", "80", ""},
		} {
			flank := &pgshardv1alpha1.PgShardShard{
				ObjectMeta: metav1.ObjectMeta{Name: f.name, Namespace: ns},
				Spec: pgshardv1alpha1.PgShardShardSpec{
					ClusterRef: clusterName,
					KeyRange:   pgshardv1alpha1.KeyRange{Start: f.start, End: f.end},
					Replicas:   1,
					Serving:    true,
				},
			}
			Expect(k8sClient.Create(ctx, flank)).To(Succeed())
		}

		srcPod := makeNode(clusterName+"-srcnode", srcIP)
		src := &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName + "-src", Namespace: ns},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef: clusterName,
				KeyRange:   pgshardv1alpha1.KeyRange{Start: "40", End: "80"},
				Replicas:   1,
				Serving:    true,
				NodeRef:    clusterName + "-srcnode",
			},
		}
		Expect(k8sClient.Create(ctx, src)).To(Succeed())
		verify(src.Name, clusterName+"-srcnode", srcPod)

		reshard := &pgshardv1alpha1.PgShardReshard{
			ObjectMeta: metav1.ObjectMeta{Name: reshardName, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardReshardSpec{
				ClusterRef:  clusterName,
				SourceShard: src.Name,
				TargetRanges: []pgshardv1alpha1.KeyRange{
					{Start: "40", End: "60"}, {Start: "60", End: "80"},
				},
			},
		}
		Expect(k8sClient.Create(ctx, reshard)).To(Succeed())

		// Validating -> ProvisioningTargets -> Seeding -> pin -> streaming.
		for range 3 {
			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
		}
		tgtPod := makeNode(clusterName+"-shared", tgtIP)
		rs := get(reshardName)
		for _, name := range rs.Status.TargetShards {
			verify(name, clusterName+"-shared", tgtPod)
		}
		// Seeding -> CatchingUp (workflows stream by fake default), then
		// script convergent lag -> ReadyToCutover.
		_, err = reconcile()
		Expect(err).NotTo(HaveOccurred())
		Expect(get(reshardName).Status.Phase).To(Equal(pgshardv1alpha1.ReshardCatchingUp))
		uid := strings.ReplaceAll(string(rs.UID), "-", "_")
		ids := []string{
			"pgshard_rco_" + base + "_" + uid + "_t0",
			"pgshard_rco_" + base + "_" + uid + "_t1",
		}
		for _, id := range ids {
			targetAgent.SetWorkflowLsn(id, 0x4000)
		}
		_, err = reconcile()
		Expect(err).NotTo(HaveOccurred())
		Expect(get(reshardName).Status.Phase).To(Equal(pgshardv1alpha1.ReshardReadyToCutover))
		return sourceAgent, targetAgent, reconcile, routingReconcile, ids, rr
	}

	// drive reconciles both controllers until the predicate holds or times out.
	drive := func(reconcile func() (ctrl.Result, error), routingReconcile func(),
		what string, pred func() bool) {
		deadline := time.Now().Add(30 * time.Second)
		for {
			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			routingReconcile()
			if pred() {
				return
			}
			if time.Now().After(deadline) {
				Fail("timed out driving cutover to: " + what)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	It("gates, quiesces, freezes, commits, and switches the serving set", func() {
		sourceAgent, targetAgent, reconcile, routingReconcile, ids, _ := cutoverSetup("happy")
		name := "rco-happy"

		// ReadyToCutover -> (finalizer) -> (claim) -> CuttingOver persists the
		// gate deadline; the routing compiler publishes the gate.
		for range 3 {
			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			if get(name).Status.Phase == pgshardv1alpha1.ReshardCuttingOver {
				break
			}
		}
		rs := get(name)
		Expect(rs.Status.Phase).To(Equal(pgshardv1alpha1.ReshardCuttingOver))
		Expect(rs.Status.CutoverGateDeadline).NotTo(BeNil())
		routingReconcile()
		var rt pgshardv1alpha1.PgShardRouting
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "chappy", Namespace: ns}, &rt)).To(Succeed())
		Expect(rt.Spec.Gates).To(HaveLen(1))

		// Quiesce (1s lease + margin), then the freeze lands on the source
		// with the reshard UID as the idempotency id.
		drive(reconcile, routingReconcile, "the frozen barrier", func() bool {
			return get(name).Status.CutoverFrozenLSN != 0
		})
		Expect(uint64(get(name).Status.CutoverFrozenLSN)).To(Equal(barrier))
		// The source database was fenced write-quiescent BEFORE the barrier.
		Expect(sourceAgent.FencedDatabases()).To(HaveKey("chappy-src"))
		journals := sourceAgent.EmittedJournals()
		Expect(journals).To(HaveLen(1))
		Expect(journals[0].GetId()).To(Equal(string(get(name).UID) + "-0"))
		Expect(journals[0].GetJournal().GetSuccessors()).To(HaveLen(2))

		// The switch must NOT commit before every target acks the barrier.
		_, err := reconcile()
		Expect(err).NotTo(HaveOccurred())
		Expect(get(name).Status.SwitchCommitted).To(BeFalse())

		for _, id := range ids {
			targetAgent.SetWorkflowJournalLsn(id, barrier)
		}
		drive(reconcile, routingReconcile, "the switched-forward phase", func() bool {
			return get(name).Status.Phase == pgshardv1alpha1.ReshardSwitchedForward
		})

		rs = get(name)
		Expect(rs.Status.SwitchCommitted).To(BeTrue())
		Expect(rs.Status.CutoverGateDeadline).To(BeNil())
		Expect(getShard("chappy-src").Spec.Serving).To(BeFalse())
		for _, t := range rs.Status.TargetShards {
			Expect(getShard(t).Spec.Serving).To(BeTrue())
		}
		routingReconcile()
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "chappy", Namespace: ns}, &rt)).To(Succeed())
		Expect(rt.Spec.Gates).To(BeEmpty())
		Expect(switchedSetCompiled(&rt, "chappy-src", rs.Status.TargetShards)).To(BeTrue())
	})

	It("cleanly deletes a pre-commit cutover, withdrawing gate and un-fencing", func() {
		sourceAgent, _, reconcile, routingReconcile, _, _ := cutoverSetup("del")
		name := "rco-del"
		for range 3 {
			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			if get(name).Status.Phase == pgshardv1alpha1.ReshardCuttingOver {
				break
			}
		}
		// Reach the freeze so the source is fenced and the gate is published.
		drive(reconcile, routingReconcile, "the frozen barrier", func() bool {
			return get(name).Status.CutoverFrozenLSN != 0
		})
		Expect(sourceAgent.FencedDatabases()).To(HaveKey("cdel-src"))

		// Delete mid-cutover (delete-to-change-course). Cleanup must clear the
		// gate, un-fence the source, release the claim, and let the object go.
		rs := get(name)
		Expect(k8sClient.Delete(ctx, &rs)).To(Succeed())
		drive(reconcile, routingReconcile, "the reshard to be gone", func() bool {
			var got pgshardv1alpha1.PgShardReshard
			return apierrors.IsNotFound(
				k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &got))
		})
		Expect(sourceAgent.FencedDatabases()).NotTo(HaveKey("cdel-src"),
			"deletion must un-fence the source")
		Expect(getShard("cdel-src").Annotations).NotTo(HaveKey("pgshard.dev/cutover-claim"),
			"deletion must release the source claim")
	})

	It("keeps a committed source fenced when the object is deleted after the switch", func() {
		sourceAgent, targetAgent, reconcile, routingReconcile, ids, _ := cutoverSetup("dc")
		name := "rco-dc"
		for range 3 {
			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			if get(name).Status.Phase == pgshardv1alpha1.ReshardCuttingOver {
				break
			}
		}
		drive(reconcile, routingReconcile, "the frozen barrier", func() bool {
			return get(name).Status.CutoverFrozenLSN != 0
		})
		Expect(sourceAgent.FencedDatabases()).To(HaveKey("cdc-src"))
		for _, id := range ids {
			targetAgent.SetWorkflowJournalLsn(id, barrier)
		}
		drive(reconcile, routingReconcile, "the switched-forward phase", func() bool {
			return get(name).Status.Phase == pgshardv1alpha1.ReshardSwitchedForward
		})
		Expect(get(name).Status.SwitchCommitted).To(BeTrue())
		Expect(getShard("cdc-src").Spec.Serving).To(BeFalse())

		// Deleting a COMMITTED cutover must NOT un-fence the source: it has been
		// hidden and the targets have snapshotted past the freeze barrier, so
		// re-admitting writes would resurrect a diverged, wrong-data source.
		rs := get(name)
		Expect(k8sClient.Delete(ctx, &rs)).To(Succeed())
		drive(reconcile, routingReconcile, "the reshard to be gone", func() bool {
			var got pgshardv1alpha1.PgShardReshard
			return apierrors.IsNotFound(
				k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &got))
		})
		Expect(sourceAgent.FencedDatabases()).To(HaveKey("cdc-src"),
			"a committed switch must leave the hidden source fenced for good")
	})

	It("keeps a committed source fenced when a terminal validation fails the cutover", func() {
		sourceAgent, targetAgent, reconcile, routingReconcile, ids, _ := cutoverSetup("fc")
		name := "rco-fc"
		for range 3 {
			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			if get(name).Status.Phase == pgshardv1alpha1.ReshardCuttingOver {
				break
			}
		}
		drive(reconcile, routingReconcile, "the frozen barrier", func() bool {
			return get(name).Status.CutoverFrozenLSN != 0
		})
		for _, id := range ids {
			targetAgent.SetWorkflowJournalLsn(id, barrier)
		}
		drive(reconcile, routingReconcile, "the switched-forward phase", func() bool {
			return get(name).Status.Phase == pgshardv1alpha1.ReshardSwitchedForward
		})
		Expect(get(name).Status.SwitchCommitted).To(BeTrue())
		Expect(sourceAgent.FencedDatabases()).To(HaveKey("cfc-src"))

		// Re-enter CuttingOver with a stale ClusterUID so the next reconcile
		// takes the ClusterReplaced terminal path (failCutover) while the switch
		// is already committed. It must fail WITHOUT un-fencing the source.
		rs := get(name)
		rs.Status.Phase = pgshardv1alpha1.ReshardCuttingOver
		rs.Status.ClusterUID = staleUID
		Expect(k8sClient.Status().Update(ctx, &rs)).To(Succeed())
		_, err := reconcile()
		Expect(err).NotTo(HaveOccurred())

		Expect(get(name).Status.Phase).To(Equal(pgshardv1alpha1.ReshardFailed))
		Expect(sourceAgent.FencedDatabases()).To(HaveKey("cfc-src"),
			"a terminal failure after commit must never re-admit writes to the diverged source")
	})

	It("retains the claim on a committed-but-unflipped source when a terminal validation fails", func() {
		sourceAgent, targetAgent, reconcile, routingReconcile, ids, _ := cutoverSetup("xo")
		name := "rco-xo"
		for range 3 {
			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			if get(name).Status.Phase == pgshardv1alpha1.ReshardCuttingOver {
				break
			}
		}
		drive(reconcile, routingReconcile, "the frozen barrier", func() bool {
			return get(name).Status.CutoverFrozenLSN != 0
		})
		for _, id := range ids {
			targetAgent.SetWorkflowJournalLsn(id, barrier)
		}
		// Stop the instant the switch commits — BEFORE completeSwitch flips the
		// serving set — so the source is committed yet still Serving. Break on
		// the reconcile that sets SwitchCommitted; do not reconcile again.
		committed := false
		for range 10 {
			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			if get(name).Status.SwitchCommitted {
				committed = true
				break
			}
			routingReconcile()
		}
		Expect(committed).To(BeTrue())
		Expect(getShard("cxo-src").Spec.Serving).To(BeTrue(),
			"the source must still be serving in the committed-but-unflipped window")
		Expect(sourceAgent.FencedDatabases()).To(HaveKey("cxo-src"))
		Expect(getShard("cxo-src").Annotations).To(HaveKeyWithValue(cutoverClaimAnnotation, name))

		// A terminal validation trips after commit. failCutover must keep the
		// source fenced AND retain the claim: the committed fence lives only in
		// THIS reshard's status, so releasing the claim would let a replacement
		// reshard claim the still-serving source and later un-fence it, reopening
		// writes past this reshard's freeze barrier.
		rs := get(name)
		rs.Status.ClusterUID = staleUID
		Expect(k8sClient.Status().Update(ctx, &rs)).To(Succeed())
		_, err := reconcile()
		Expect(err).NotTo(HaveOccurred())

		Expect(get(name).Status.Phase).To(Equal(pgshardv1alpha1.ReshardFailed))
		Expect(sourceAgent.FencedDatabases()).To(HaveKey("cxo-src"),
			"a committed terminal failure must leave the source fenced")
		Expect(getShard("cxo-src").Annotations).To(HaveKeyWithValue(cutoverClaimAnnotation, name),
			"the claim is retained so no replacement reshard can claim and un-fence the committed source")
	})

	It("refuses to un-fence a source whose claim a replacement reshard now holds", func() {
		sourceAgent, _, reconcile, routingReconcile, _, _ := cutoverSetup("so")
		name := "rco-so"
		for range 3 {
			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			if get(name).Status.Phase == pgshardv1alpha1.ReshardCuttingOver {
				break
			}
		}
		// A reaches an uncommitted, fenced state holding the claim.
		drive(reconcile, routingReconcile, "the frozen barrier", func() bool {
			return get(name).Status.CutoverFrozenLSN != 0
		})
		Expect(get(name).Status.SourceFenced).To(BeTrue())
		Expect(get(name).Status.SwitchCommitted).To(BeFalse())
		Expect(sourceAgent.FencedDatabases()).To(HaveKey("cso-src"))

		// Simulate the crash window: A's claim was released and a replacement
		// reshard B has since claimed the still-fenced source (and, off-screen,
		// committed its own switch). A's PERSISTED status still says
		// SourceFenced=true / CuttingOver.
		src := getShard("cso-src")
		src.Annotations[cutoverClaimAnnotation] = "rco-replacement-b"
		Expect(k8sClient.Update(ctx, &src)).To(Succeed())

		// Trip A's terminal path (stale ClusterUID -> ClusterReplaced ->
		// failCutover, uncommitted). A must NOT un-fence B's source: the live
		// claim is no longer A's.
		rs := get(name)
		rs.Status.ClusterUID = staleUID
		Expect(k8sClient.Status().Update(ctx, &rs)).To(Succeed())
		_, err := reconcile()
		Expect(err).NotTo(HaveOccurred())

		Expect(get(name).Status.Phase).To(Equal(pgshardv1alpha1.ReshardFailed))
		Expect(sourceAgent.FencedDatabases()).To(HaveKey("cso-src"),
			"A must not un-fence a source whose claim a replacement reshard now holds")
		Expect(getShard("cso-src").Annotations).To(HaveKeyWithValue(cutoverClaimAnnotation, "rco-replacement-b"),
			"A must not disturb the replacement reshard's claim")
	})

	It("authorizes un-fence from the uncached API reader, not the informer cache", func() {
		sourceAgent, _, reconcile, routingReconcile, _, rr := cutoverSetup("ar")
		name := "rco-ar"
		for range 3 {
			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			if get(name).Status.Phase == pgshardv1alpha1.ReshardCuttingOver {
				break
			}
		}
		drive(reconcile, routingReconcile, "the frozen barrier", func() bool {
			return get(name).Status.CutoverFrozenLSN != 0
		})
		Expect(get(name).Status.SourceFenced).To(BeTrue())
		Expect(sourceAgent.FencedDatabases()).To(HaveKey("car-src"))

		// The cached client (rr.Client) still shows A as the claim holder — a
		// lagging informer. The uncached API reader reports the true, updated
		// holder: a replacement reshard. unfenceSource must trust the API
		// reader and refuse; if it read the cache it would un-fence a source a
		// replacement now owns.
		rr.APIReader = foreignClaimReader{Reader: k8sClient, shardName: "car-src", holder: "rco-replacement-b"}
		rs := get(name)
		rs.Status.ClusterUID = staleUID
		Expect(k8sClient.Status().Update(ctx, &rs)).To(Succeed())
		_, err := reconcile()
		Expect(err).NotTo(HaveOccurred())

		Expect(get(name).Status.Phase).To(Equal(pgshardv1alpha1.ReshardFailed))
		Expect(sourceAgent.FencedDatabases()).To(HaveKey("car-src"),
			"the uncached API reader showed a foreign holder, so A must not un-fence")
	})

	It("refuses to roll back and un-fence its own switch that the cache has not yet observed as committed", func() {
		sourceAgent, _, reconcile, routingReconcile, _, rr := cutoverSetup("cs")
		name := "rco-cs"
		for range 3 {
			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			if get(name).Status.Phase == pgshardv1alpha1.ReshardCuttingOver {
				break
			}
		}
		drive(reconcile, routingReconcile, "the frozen barrier", func() bool {
			return get(name).Status.CutoverFrozenLSN != 0
		})
		Expect(get(name).Status.SourceFenced).To(BeTrue())
		Expect(sourceAgent.FencedDatabases()).To(HaveKey("ccs-src"))

		// The switch has committed at the API server, but the reconcile's cached
		// view still shows SwitchCommitted=false (controller-runtime writes
		// status without refreshing the cache). The gate deadline has since
		// crossed, so the stale reconcile would roll back and un-fence its OWN
		// committed source — the claim is still legitimately this reshard's, so
		// the ownership check alone cannot catch it. unfenceSource must re-read
		// the reshard from the API server, see it committed, and refuse.
		rr.APIReader = committedSwitchReader{Reader: k8sClient, reshardName: name}
		rs := get(name)
		expired := metav1.NewTime(time.Now().Add(-time.Second))
		rs.Status.CutoverGateDeadline = &expired
		Expect(k8sClient.Status().Update(ctx, &rs)).To(Succeed())
		_, err := reconcile()
		Expect(err).NotTo(HaveOccurred())

		Expect(get(name).Status.Phase).To(Equal(pgshardv1alpha1.ReshardCuttingOver),
			"the stale rollback must bail, not regress a committed cutover")
		Expect(get(name).Status.SourceFenced).To(BeTrue())
		Expect(sourceAgent.FencedDatabases()).To(HaveKey("ccs-src"),
			"a committed switch the cache has not yet seen must never be un-fenced")
	})

	It("rolls back to CatchingUp when the gate deadline expires uncommitted", func() {
		_, _, reconcile, routingReconcile, _, _ := cutoverSetup("roll")
		name := "rco-roll"

		for range 3 {
			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			if get(name).Status.Phase == pgshardv1alpha1.ReshardCuttingOver {
				break
			}
		}
		rs := get(name)
		Expect(rs.Status.Phase).To(Equal(pgshardv1alpha1.ReshardCuttingOver))

		// Force the deadline into the past; the next reconcile must roll
		// back, clear the gate request, and resume CatchingUp — never
		// committing a switch.
		expired := metav1.NewTime(time.Now().Add(-time.Second))
		rs.Status.CutoverGateDeadline = &expired
		Expect(k8sClient.Status().Update(ctx, &rs)).To(Succeed())
		_, err := reconcile()
		Expect(err).NotTo(HaveOccurred())

		rs = get(name)
		Expect(rs.Status.Phase).To(Equal(pgshardv1alpha1.ReshardCatchingUp))
		Expect(rs.Status.CutoverGateDeadline).To(BeNil())
		Expect(rs.Status.SwitchCommitted).To(BeFalse())
		Expect(rs.Status.CutoverAttempt).To(Equal(int64(1)),
			"a rollback opens a NEW attempt so the next freeze cannot replay the old barrier")
		Expect(getShard("croll-src").Spec.Serving).To(BeTrue(),
			"an uncommitted rollback must leave the source serving")
		Expect(getShard("croll-src").Annotations).To(HaveKey("pgshard.dev/cutover-claim"),
			"the claim is kept across a rollback for the retry")
		routingReconcile()
		var rt pgshardv1alpha1.PgShardRouting
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "croll", Namespace: ns}, &rt)).To(Succeed())
		Expect(rt.Spec.Gates).To(BeEmpty())
	})
})
