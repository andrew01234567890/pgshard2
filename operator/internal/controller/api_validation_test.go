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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
)

const valNamespace = "default"

var _ = Describe("API validation", func() {

	newCluster := func(name string) *pgshardv1alpha1.PgShardCluster {
		return &pgshardv1alpha1.PgShardCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: valNamespace},
			Spec: pgshardv1alpha1.PgShardClusterSpec{
				Postgres: pgshardv1alpha1.PostgresSpec{Version: "18"},
				Shards:   pgshardv1alpha1.ShardsSpec{InitialCount: 4},
			},
		}
	}

	It("rejects PostgreSQL versions below 18", func() {
		c := newCluster("val-version")
		c.Spec.Postgres.Version = "17"
		Expect(k8sClient.Create(ctx, c)).NotTo(Succeed())
		c.Spec.Postgres.Version = "16"
		Expect(k8sClient.Create(ctx, c)).NotTo(Succeed())
	})

	It("rejects mutation of initialCount and hashFunction", func() {
		c := newCluster("val-immutables")
		Expect(k8sClient.Create(ctx, c)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, c) }()

		c.Spec.Shards.InitialCount = 8
		Expect(k8sClient.Update(ctx, c)).NotTo(Succeed())

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(c), c)).To(Succeed())
		c.Spec.Postgres.HashFunction = "md5"
		Expect(k8sClient.Update(ctx, c)).NotTo(Succeed())
	})

	It("rejects out-of-range shard counts", func() {
		c := newCluster("val-count")
		c.Spec.Shards.InitialCount = 129
		Expect(k8sClient.Create(ctx, c)).NotTo(Succeed())
		c.Spec.Shards.InitialCount = 0
		Expect(k8sClient.Create(ctx, c)).NotTo(Succeed())
	})

	It("rejects malformed and mutated key ranges", func() {
		s := &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: "val-shard", Namespace: valNamespace},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef: "c",
				KeyRange:   pgshardv1alpha1.KeyRange{Start: "40", End: "80"},
				Replicas:   1,
			},
		}
		bad := s.DeepCopy()
		bad.Name = "val-shard-bad"
		bad.Spec.KeyRange.Start = "4"
		Expect(k8sClient.Create(ctx, bad)).NotTo(Succeed())
		bad.Spec.KeyRange.Start = "GG"
		Expect(k8sClient.Create(ctx, bad)).NotTo(Succeed())
		// Non-canonical bound (trailing zero byte aliases a shorter bound).
		bad.Spec.KeyRange = pgshardv1alpha1.KeyRange{Start: "4000"}
		Expect(k8sClient.Create(ctx, bad)).NotTo(Succeed())

		Expect(k8sClient.Create(ctx, s)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, s) }()
		s.Spec.KeyRange.End = "c0"
		Expect(k8sClient.Update(ctx, s)).NotTo(Succeed())

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(s), s)).To(Succeed())
		s.Spec.ClusterRef = "other"
		Expect(k8sClient.Update(ctx, s)).NotTo(Succeed())
	})

	It("requires exactly one of a replication link's source/target shard", func() {
		base := func(name string, link pgshardv1alpha1.ReplicationLink) *pgshardv1alpha1.PgShardShard {
			return &pgshardv1alpha1.PgShardShard{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: valNamespace},
				Spec: pgshardv1alpha1.PgShardShardSpec{
					ClusterRef:       "c",
					KeyRange:         pgshardv1alpha1.KeyRange{End: "80"},
					Replicas:         1,
					ReplicationLinks: []pgshardv1alpha1.ReplicationLink{link},
				},
			}
		}
		neither := base("val-link-neither", pgshardv1alpha1.ReplicationLink{
			Name: "l", Slot: "s", Publication: "p",
		})
		Expect(k8sClient.Create(ctx, neither)).NotTo(Succeed())

		both := base("val-link-both", pgshardv1alpha1.ReplicationLink{
			Name: "l", Slot: "s", Publication: "p",
			SourceShard: "a", TargetShard: "b",
		})
		Expect(k8sClient.Create(ctx, both)).NotTo(Succeed())

		one := base("val-link-one", pgshardv1alpha1.ReplicationLink{
			Name: "l", Slot: "s", Publication: "p", SourceShard: "a",
		})
		Expect(k8sClient.Create(ctx, one)).To(Succeed())
		_ = k8sClient.Delete(ctx, one)
	})
})
