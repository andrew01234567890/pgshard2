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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
)

var _ = Describe("PgShardReshard validation", func() {
	const ns = "default"

	reconcile := func(name string) (ctrl.Result, error) {
		r := &PgShardReshardReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		return r.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: ns},
		})
	}
	getReshard := func(name string) pgshardv1alpha1.PgShardReshard {
		var got pgshardv1alpha1.PgShardReshard
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &got)).To(Succeed())
		return got
	}
	createSource := func(name, start, end string) {
		src := &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef: "c",
				KeyRange:   pgshardv1alpha1.KeyRange{Start: start, End: end},
				Replicas:   1,
			},
		}
		Expect(k8sClient.Create(ctx, src)).To(Succeed())
	}
	createReshard := func(name, source string, ranges ...pgshardv1alpha1.KeyRange) {
		reshard := &pgshardv1alpha1.PgShardReshard{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardReshardSpec{
				ClusterRef:   "c",
				SourceShard:  source,
				TargetRanges: ranges,
			},
		}
		Expect(k8sClient.Create(ctx, reshard)).To(Succeed())
	}

	It("advances a valid split to ProvisioningTargets", func() {
		createSource("rs-src-ok", "40", "80")
		createReshard("rs-ok", "rs-src-ok",
			pgshardv1alpha1.KeyRange{Start: "40", End: "60"},
			pgshardv1alpha1.KeyRange{Start: "60", End: "80"})

		res, err := reconcile("rs-ok")
		Expect(err).NotTo(HaveOccurred())
		// The advance must requeue: a status-only write does not bump the
		// generation, so without this the GenerationChangedPredicate would drop
		// the follow-up event and the reshard would stall at ProvisioningTargets.
		Expect(res).NotTo(Equal(ctrl.Result{}))

		got := getReshard("rs-ok")
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ReshardProvisioningTargets))
		Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, "Validated")).To(BeTrue())
	})

	It("fails a split that does not partition the source range", func() {
		createSource("rs-src-bad", "40", "80")
		// A gap between 50 and 60.
		createReshard("rs-bad", "rs-src-bad",
			pgshardv1alpha1.KeyRange{Start: "40", End: "50"},
			pgshardv1alpha1.KeyRange{Start: "60", End: "80"})

		_, err := reconcile("rs-bad")
		Expect(err).NotTo(HaveOccurred())

		got := getReshard("rs-bad")
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ReshardFailed))
		Expect(apimeta.IsStatusConditionFalse(got.Status.Conditions, "Validated")).To(BeTrue())
	})

	It("fails when the source shard belongs to a different cluster", func() {
		src := &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: "rs-src-xc", Namespace: ns},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef: "other-cluster",
				KeyRange:   pgshardv1alpha1.KeyRange{Start: "40", End: "80"},
				Replicas:   1,
			},
		}
		Expect(k8sClient.Create(ctx, src)).To(Succeed())
		createReshard("rs-xc", "rs-src-xc",
			pgshardv1alpha1.KeyRange{Start: "40", End: "60"},
			pgshardv1alpha1.KeyRange{Start: "60", End: "80"})

		_, err := reconcile("rs-xc")
		Expect(err).NotTo(HaveOccurred())

		got := getReshard("rs-xc")
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ReshardFailed))
		Expect(apimeta.IsStatusConditionFalse(got.Status.Conditions, "Validated")).To(BeTrue())
	})

	It("fails when the source shard is the system shard", func() {
		src := &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: "rs-src-sys", Namespace: ns},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef: "c",
				Role:       pgshardv1alpha1.ShardRoleSystem,
				KeyRange:   pgshardv1alpha1.KeyRange{},
				Replicas:   1,
			},
		}
		Expect(k8sClient.Create(ctx, src)).To(Succeed())
		// A well-formed partition of the full range — so the rejection is on the
		// system role, not the ranges.
		createReshard("rs-sys", "rs-src-sys",
			pgshardv1alpha1.KeyRange{Start: "", End: "80"},
			pgshardv1alpha1.KeyRange{Start: "80", End: ""})

		_, err := reconcile("rs-sys")
		Expect(err).NotTo(HaveOccurred())

		got := getReshard("rs-sys")
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ReshardFailed))
		Expect(apimeta.IsStatusConditionFalse(got.Status.Conditions, "Validated")).To(BeTrue())
	})

	It("holds in Validating and retries when the source shard is absent", func() {
		createReshard("rs-nosrc", "does-not-exist",
			pgshardv1alpha1.KeyRange{Start: "40", End: "60"},
			pgshardv1alpha1.KeyRange{Start: "60", End: "80"})

		res, err := reconcile("rs-nosrc")
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))

		got := getReshard("rs-nosrc")
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ReshardValidating))
	})

	Context("ProvisioningTargets", func() {
		createCluster := func(name string, mode pgshardv1alpha1.PlacementMode) {
			cl := &pgshardv1alpha1.PgShardCluster{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
				Spec: pgshardv1alpha1.PgShardClusterSpec{
					Postgres: pgshardv1alpha1.PostgresSpec{Version: "18"},
					Shards:   pgshardv1alpha1.ShardsSpec{InitialCount: 2},
				},
			}
			if mode != "" {
				cl.Spec.Placement = &pgshardv1alpha1.PlacementSpec{Mode: mode}
			}
			Expect(k8sClient.Create(ctx, cl)).To(Succeed())
		}
		// All these tests split the source range 40-80.
		createSourceIn := func(name, cluster string) {
			src := &pgshardv1alpha1.PgShardShard{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
				Spec: pgshardv1alpha1.PgShardShardSpec{
					ClusterRef: cluster,
					KeyRange:   pgshardv1alpha1.KeyRange{Start: "40", End: "80"},
					Replicas:   1,
					Serving:    true,
				},
			}
			Expect(k8sClient.Create(ctx, src)).To(Succeed())
		}
		createReshardIn := func(name, cluster, source string, ranges ...pgshardv1alpha1.KeyRange) {
			reshard := &pgshardv1alpha1.PgShardReshard{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
				Spec: pgshardv1alpha1.PgShardReshardSpec{
					ClusterRef:   cluster,
					SourceShard:  source,
					TargetRanges: ranges,
				},
			}
			Expect(k8sClient.Create(ctx, reshard)).To(Succeed())
		}
		getShard := func(name string) (*pgshardv1alpha1.PgShardShard, error) {
			s := &pgshardv1alpha1.PgShardShard{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, s)
			return s, err
		}
		// Drive the reshard through Validating into ProvisioningTargets (reconcile
		// #1), then run the ProvisioningTargets reconcile that creates the targets
		// (reconcile #2).
		provision := func(name string) {
			res, err := reconcile(name)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).NotTo(Equal(ctrl.Result{}))
			Expect(getReshard(name).Status.Phase).To(Equal(pgshardv1alpha1.ReshardProvisioningTargets))
			_, err = reconcile(name)
			Expect(err).NotTo(HaveOccurred())
		}

		It("creates hidden target shards on the shared node for shared placement", func() {
			createCluster("cl-shared", pgshardv1alpha1.PlacementShared)
			createSourceIn("cl-shared-src", "cl-shared")
			createReshardIn("rs-shared", "cl-shared", "cl-shared-src",
				pgshardv1alpha1.KeyRange{Start: "40", End: "60"},
				pgshardv1alpha1.KeyRange{Start: "60", End: "80"})

			provision("rs-shared")

			got := getReshard("rs-shared")
			Expect(got.Status.TargetShards).To(ConsistOf("cl-shared-40-60", "cl-shared-60-80"))
			Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, "TargetsProvisioned")).To(BeTrue())

			for _, n := range []string{"cl-shared-40-60", "cl-shared-60-80"} {
				s, err := getShard(n)
				Expect(err).NotTo(HaveOccurred())
				Expect(s.Spec.Serving).To(BeFalse())
				Expect(s.Spec.NodeRef).To(Equal("cl-shared-shared"))
				Expect(metav1.IsControlledBy(s, &got)).To(BeTrue())
			}
			// The shared node belongs to the cluster; the reshard must not create one.
			node := &pgshardv1alpha1.PgShardNode{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: "cl-shared-40-60", Namespace: ns}, node)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("creates a reshard-owned node per target for dedicated placement", func() {
			createCluster("cl-ded", pgshardv1alpha1.PlacementDedicatedInstance)
			createSourceIn("cl-ded-src", "cl-ded")
			createReshardIn("rs-ded", "cl-ded", "cl-ded-src",
				pgshardv1alpha1.KeyRange{Start: "40", End: "60"},
				pgshardv1alpha1.KeyRange{Start: "60", End: "80"})

			provision("rs-ded")

			got := getReshard("rs-ded")
			Expect(got.Status.TargetShards).To(ConsistOf("cl-ded-40-60", "cl-ded-60-80"))

			for _, n := range []string{"cl-ded-40-60", "cl-ded-60-80"} {
				s, err := getShard(n)
				Expect(err).NotTo(HaveOccurred())
				Expect(s.Spec.Serving).To(BeFalse())
				Expect(s.Spec.NodeRef).To(Equal(n))
				Expect(metav1.IsControlledBy(s, &got)).To(BeTrue())

				node := &pgshardv1alpha1.PgShardNode{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: n, Namespace: ns}, node)).To(Succeed())
				Expect(metav1.IsControlledBy(node, &got)).To(BeTrue())
			}
		})

		It("is idempotent across repeated reconciles", func() {
			createCluster("cl-idem", pgshardv1alpha1.PlacementShared)
			createSourceIn("cl-idem-src", "cl-idem")
			createReshardIn("rs-idem", "cl-idem", "cl-idem-src",
				pgshardv1alpha1.KeyRange{Start: "40", End: "60"},
				pgshardv1alpha1.KeyRange{Start: "60", End: "80"})

			provision("rs-idem")
			for range 2 {
				_, err := reconcile("rs-idem")
				Expect(err).NotTo(HaveOccurred())
			}

			got := getReshard("rs-idem")
			Expect(got.Status.TargetShards).To(ConsistOf("cl-idem-40-60", "cl-idem-60-80"))
			for _, n := range []string{"cl-idem-40-60", "cl-idem-60-80"} {
				_, err := getShard(n)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		It("surfaces a condition and retries on a target-name collision", func() {
			createCluster("clcol", pgshardv1alpha1.PlacementShared)
			createSourceIn("clcol-src", "clcol")
			createReshardIn("rscol", "clcol", "clcol-src",
				pgshardv1alpha1.KeyRange{Start: "40", End: "60"},
				pgshardv1alpha1.KeyRange{Start: "60", End: "80"})
			// A pre-existing shard with the first target's name, owned by nobody.
			bare := &pgshardv1alpha1.PgShardShard{
				ObjectMeta: metav1.ObjectMeta{Name: "clcol-40-60", Namespace: ns},
				Spec: pgshardv1alpha1.PgShardShardSpec{
					ClusterRef: "clcol",
					KeyRange:   pgshardv1alpha1.KeyRange{Start: "40", End: "60"},
					Replicas:   1,
				},
			}
			Expect(k8sClient.Create(ctx, bare)).To(Succeed())

			_, err := reconcile("rscol") // Validating -> ProvisioningTargets
			Expect(err).NotTo(HaveOccurred())
			res, err := reconcile("rscol") // ProvisioningTargets -> hits the collision
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))

			got := getReshard("rscol")
			Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ReshardProvisioningTargets))
			cond := apimeta.FindStatusCondition(got.Status.Conditions, "TargetsProvisioned")
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal("TargetCollision"))
		})
	})
})
