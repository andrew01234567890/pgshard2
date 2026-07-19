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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// PgShardReshardSpec requests splitting one source shard's key range into a new
// partition. The request is immutable once created: a reshard is a workflow, not
// a knob, and changing its target mid-flight would strand half-provisioned
// shards. To change course, delete this object (rolling back) and create a new
// one.
// +kubebuilder:validation:XValidation:rule="self.clusterRef == oldSelf.clusterRef",message="clusterRef is immutable"
// +kubebuilder:validation:XValidation:rule="self.sourceShard == oldSelf.sourceShard",message="sourceShard is immutable"
// +kubebuilder:validation:XValidation:rule="self.targetRanges == oldSelf.targetRanges",message="targetRanges is immutable"
type PgShardReshardSpec struct {
	// ClusterRef is the PgShardCluster this reshard operates on.
	// +kubebuilder:validation:MinLength=1
	ClusterRef string `json:"clusterRef"`

	// SourceShard is the PgShardShard whose key range is being split.
	// +kubebuilder:validation:MinLength=1
	SourceShard string `json:"sourceShard"`

	// TargetRanges is the desired partition of the source shard's key range —
	// contiguous, covering exactly the source range, with at least two parts (a
	// split). The controller validates the partition against the source's range
	// before creating any target shard.
	// +kubebuilder:validation:MinItems=2
	// +kubebuilder:validation:MaxItems=128
	TargetRanges []KeyRange `json:"targetRanges"`
}

// ReshardPhase is where a reshard is in its lifecycle. The later cutover phases
// exist in the state model but are not yet driven (they need the seeding engine,
// the freeze-LSN handshake, and router-fleet gating); this controller drives
// through ProvisioningTargets and back out via RollingBack.
// +kubebuilder:validation:Enum=Pending;Validating;ProvisioningTargets;Seeding;CatchingUp;ReadyToCutover;CuttingOver;SwitchedForward;Finalizing;Completed;RollingBack;Failed
type ReshardPhase string

const (
	// ReshardPending is a freshly accepted request not yet validated.
	ReshardPending ReshardPhase = "Pending"
	// ReshardValidating checks that TargetRanges partitions the source range.
	ReshardValidating ReshardPhase = "Validating"
	// ReshardProvisioningTargets creates the non-serving target shards and waits
	// for their databases to come up.
	ReshardProvisioningTargets ReshardPhase = "ProvisioningTargets"
	// ReshardSeeding copies and streams the source rows into the targets (later
	// slice; needs the seeding engine).
	ReshardSeeding ReshardPhase = "Seeding"
	// ReshardCatchingUp waits for the targets' replication lag to fall (later).
	ReshardCatchingUp ReshardPhase = "CatchingUp"
	// ReshardReadyToCutover is fully seeded and caught up, awaiting cutover (later).
	ReshardReadyToCutover ReshardPhase = "ReadyToCutover"
	// ReshardCuttingOver gates writes, freezes the source, and switches routing
	// to the targets (later; needs router-fleet gating + the freeze-LSN handshake).
	ReshardCuttingOver ReshardPhase = "CuttingOver"
	// ReshardSwitchedForward has switched; targets serve, source is retained for
	// the reverse-replication rollback window (later).
	ReshardSwitchedForward ReshardPhase = "SwitchedForward"
	// ReshardFinalizing decommissions the source shard (later).
	ReshardFinalizing ReshardPhase = "Finalizing"
	// ReshardCompleted is a finished reshard.
	ReshardCompleted ReshardPhase = "Completed"
	// ReshardRollingBack tears down the targets created so far.
	ReshardRollingBack ReshardPhase = "RollingBack"
	// ReshardFailed is a terminal, non-retryable rejection (e.g. an invalid
	// partition); see the conditions for the reason.
	ReshardFailed ReshardPhase = "Failed"
)

// PgShardReshardStatus records progress. TargetShards is the durable idempotency
// anchor: it records which target shard objects the controller has created, so a
// restart resumes rather than re-deriving and double-creating.
type PgShardReshardStatus struct {
	// +optional
	Phase ReshardPhase `json:"phase,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// TargetShards are the names of the PgShardShard objects created for the
	// target ranges, in TargetRanges order. Recording them here is a convenience;
	// resume safety does not depend on it, because the controller derives each
	// target's name deterministically from the (immutable) cluster and key range,
	// so a crash between creating a target and recording it re-derives the same
	// name and the create is idempotent (AlreadyExists).
	// +optional
	TargetShards []string `json:"targetShards,omitempty"`

	// SourceShardUID pins the exact source shard object validated at
	// Validating: seeding reads FROM this shard's database, and a shard
	// deleted and recreated under the same name is a different placement
	// whose data was never validated.
	// +optional
	SourceShardUID string `json:"sourceShardUID,omitempty"`

	// ClusterUID pins the cluster object the reshard was validated against.
	// +optional
	ClusterUID string `json:"clusterUID,omitempty"`

	// SeedTables pins the sharded-table schema captured when seeding began.
	// Workflow specs are built ONLY from this list: live PgShardTableConfig
	// edits mid-seed would otherwise change the copied table set or filter
	// identity under running workflows, leaving targets that stream a
	// different schema than they seeded.
	// +optional
	SeedTables []ReshardSeedTable `json:"seedTables,omitempty"`

	// SeedTablesPinned distinguishes "pinned an empty schema" (a cluster
	// with no sharded tables) from "not yet pinned".
	// +optional
	SeedTablesPinned bool `json:"seedTablesPinned,omitempty"`

	// CutoverGateDeadline asks the routing compiler to gate the SOURCE
	// shard's key range (bufferWrites) until this absolute time — whenever
	// it is set, regardless of phase. Routers that cannot apply a gated
	// epoch stop renewing their write lease, so writes quiesce by lease
	// expiry. The cutover machine clears it only after the switched serving
	// set has been observed compiled into routing.
	// +optional
	CutoverGateDeadline *metav1.Time `json:"cutoverGateDeadline,omitempty"`

	// CutoverGateObservedAt records when the controller first OBSERVED its
	// gate published in PgShardRouting; the quiesce wait (write-lease expiry)
	// counts from here.
	// +optional
	CutoverGateObservedAt *metav1.Time `json:"cutoverGateObservedAt,omitempty"`

	// CutoverAttempt counts cutover attempts; a rollback increments it. The
	// freeze's journal id embeds it, so a RETRIED cutover can never replay a
	// previous attempt's barrier — workflows already acknowledged past the
	// old position, and committing against it would skip proving the NEW
	// quiesce point was decoded.
	// +optional
	CutoverAttempt int64 `json:"cutoverAttempt,omitempty"`

	// CutoverFrozenLSN is the freeze barrier: the journal message's WAL
	// position emitted in the source database after quiescence. Every target
	// workflow must acknowledge (journal_lsn >=) it before the switch.
	// +optional
	CutoverFrozenLSN int64 `json:"cutoverFrozenLSN,omitempty"`

	// SwitchCommitted is the cutover's point of no return, persisted BEFORE
	// the serving flip and BEFORE the gate is withdrawn. The routing
	// compiler refuses to publish UNGATED routing while a committed switch's
	// source still serves — a crash between clearing the gate and flipping
	// the shards can then never re-admit writes to the old source.
	// +optional
	SwitchCommitted bool `json:"switchCommitted,omitempty"`

	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// ReshardSeedTable is one pinned sharded table a reshard seeds.
type ReshardSeedTable struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	// +optional
	ShardKeyColumn string `json:"shardKeyColumn,omitempty"`
	// +optional
	ShardKeyType ShardKeyType `json:"shardKeyType,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=reshard
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef`
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.sourceShard`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`

// PgShardReshard is an online key-range split of a cluster's shard.
type PgShardReshard struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PgShardReshardSpec   `json:"spec,omitempty"`
	Status PgShardReshardStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PgShardReshardList contains a list of PgShardReshard.
type PgShardReshardList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PgShardReshard `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PgShardReshard{}, &PgShardReshardList{})
		return nil
	})
}
