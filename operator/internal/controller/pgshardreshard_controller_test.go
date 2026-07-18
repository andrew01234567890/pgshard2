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

		_, err := reconcile("rs-ok")
		Expect(err).NotTo(HaveOccurred())

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
})
