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

const (
	valNamespace  = "default"
	customerIDCol = "customer_id"
)

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

	It("validates table config identifiers and required fields", func() {
		const ordersTable = "orders"
		tc := func(name string, tables []pgshardv1alpha1.TableEntry) *pgshardv1alpha1.PgShardTableConfig {
			return &pgshardv1alpha1.PgShardTableConfig{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: valNamespace},
				Spec:       pgshardv1alpha1.PgShardTableConfigSpec{ClusterRef: "c", Tables: tables},
			}
		}
		// Sharded table without a shard key is rejected.
		Expect(k8sClient.Create(ctx, tc("tc-nokey", []pgshardv1alpha1.TableEntry{
			{Name: ordersTable, Type: pgshardv1alpha1.TableSharded},
		}))).NotTo(Succeed())
		// Sharded table with a key column but no key type is rejected: the
		// router needs the type to hash literals correctly.
		Expect(k8sClient.Create(ctx, tc("tc-notype", []pgshardv1alpha1.TableEntry{
			{Name: ordersTable, Type: pgshardv1alpha1.TableSharded, ShardKeyColumn: customerIDCol},
		}))).NotTo(Succeed())
		// Identifier with a SQL metacharacter is rejected.
		Expect(k8sClient.Create(ctx, tc("tc-inject", []pgshardv1alpha1.TableEntry{
			{
				Name: ordersTable, Type: pgshardv1alpha1.TableSharded,
				ShardKeyColumn: `id"); DROP TABLE x; --`, ShardKeyType: pgshardv1alpha1.ShardKeyInt,
			},
		}))).NotTo(Succeed())
		// A sequence-only config (no tables) is admitted — the CEL rule must
		// not dereference an absent tables list.
		seqOnly := &pgshardv1alpha1.PgShardTableConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "tc-seq", Namespace: valNamespace},
			Spec: pgshardv1alpha1.PgShardTableConfigSpec{
				ClusterRef: "c",
				Sequences:  []pgshardv1alpha1.SequenceEntry{{Name: "orders_id", BlockSize: 1000}},
			},
		}
		Expect(k8sClient.Create(ctx, seqOnly)).To(Succeed())
		_ = k8sClient.Delete(ctx, seqOnly)
		// A valid sharded table is admitted.
		ok := tc("tc-ok", []pgshardv1alpha1.TableEntry{
			{
				Name: ordersTable, Type: pgshardv1alpha1.TableSharded,
				ShardKeyColumn: customerIDCol, ShardKeyType: pgshardv1alpha1.ShardKeyInt,
			},
		})
		Expect(k8sClient.Create(ctx, ok)).To(Succeed())
		_ = k8sClient.Delete(ctx, ok)
	})

	It("enforces monotonic epoch on routing updates", func() {
		r := &pgshardv1alpha1.PgShardRouting{
			ObjectMeta: metav1.ObjectMeta{Name: "route", Namespace: valNamespace},
			Spec:       pgshardv1alpha1.PgShardRoutingSpec{Epoch: 5, TopologyGeneration: 2},
		}
		Expect(k8sClient.Create(ctx, r)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, r) }()

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(r), r)).To(Succeed())
		r.Spec.Epoch = 4 // regression
		Expect(k8sClient.Update(ctx, r)).NotTo(Succeed())

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(r), r)).To(Succeed())
		r.Spec.Epoch = 6
		r.Spec.TopologyGeneration = 1 // generation regression
		Expect(k8sClient.Update(ctx, r)).NotTo(Succeed())

		// A spec change that reuses the current epoch is rejected — consumers
		// keyed on epoch>lastApplied would silently ignore it.
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(r), r)).To(Succeed())
		r.Spec.WriteLeaseSeconds = 20 // change without an epoch bump
		Expect(k8sClient.Update(ctx, r)).NotTo(Succeed())

		// A no-op re-apply at the same epoch is allowed.
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(r), r)).To(Succeed())
		Expect(k8sClient.Update(ctx, r)).To(Succeed())

		// A change with a strict epoch increase is allowed.
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(r), r)).To(Succeed())
		r.Spec.Epoch = 6
		r.Spec.TopologyGeneration = 3
		Expect(k8sClient.Update(ctx, r)).To(Succeed())
	})

	It("validates identifiers and host in the compiled routing view", func() {
		route := func(name string, spec pgshardv1alpha1.PgShardRoutingSpec) *pgshardv1alpha1.PgShardRouting {
			spec.Epoch, spec.TopologyGeneration = 1, 1
			return &pgshardv1alpha1.PgShardRouting{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: valNamespace},
				Spec:       spec,
			}
		}
		// A DDL-bound identifier with a SQL metacharacter is rejected even on
		// the operator-compiled object agents trust.
		Expect(k8sClient.Create(ctx, route("rt-inject", pgshardv1alpha1.PgShardRoutingSpec{
			Tables: []pgshardv1alpha1.RoutingTable{{
				Schema: "public", Name: `orders"; DROP TABLE x; --`,
				Type: pgshardv1alpha1.TableSharded, ShardKeyColumn: customerIDCol,
			}},
		}))).NotTo(Succeed())
		// A host carrying connection-string metacharacters is rejected.
		Expect(k8sClient.Create(ctx, route("rt-badhost", pgshardv1alpha1.PgShardRoutingSpec{
			Shards: []pgshardv1alpha1.RoutingShard{{
				Name: "s0", KeyRange: pgshardv1alpha1.KeyRange{End: "80"},
				State:   pgshardv1alpha1.RoutingServing,
				Primary: &pgshardv1alpha1.RoutingEndpoint{Pod: "p0", Host: "evil host' sslmode=disable"},
			}},
		}))).NotTo(Succeed())
		// A valid compiled view is admitted.
		ok := route("rt-ok", pgshardv1alpha1.PgShardRoutingSpec{
			Shards: []pgshardv1alpha1.RoutingShard{{
				Name: "s0", KeyRange: pgshardv1alpha1.KeyRange{End: "80"},
				State:   pgshardv1alpha1.RoutingServing,
				Primary: &pgshardv1alpha1.RoutingEndpoint{Pod: "p0", Host: "10.0.0.1"},
			}},
			Tables: []pgshardv1alpha1.RoutingTable{{
				Schema: "public", Name: "orders",
				Type: pgshardv1alpha1.TableSharded, ShardKeyColumn: customerIDCol,
			}},
		})
		Expect(k8sClient.Create(ctx, ok)).To(Succeed())
		_ = k8sClient.Delete(ctx, ok)
	})
})
