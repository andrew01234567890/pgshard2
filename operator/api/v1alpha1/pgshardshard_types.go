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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// KeyRange is a half-open range [start, end) over the 64-bit keyspace in
// the canonical trimmed big-endian hex syntax: "40" means
// 0x4000000000000000; an empty start is 0 and an empty end is the top of
// the keyspace. A shard's identity IS its range: reshards never mutate it,
// they create new shards and retire old ones.
//
// Bounds must be canonical: trailing zero bytes are trimmed, so a non-empty
// bound never ends in "00" (which would alias a shorter bound to the same
// value, e.g. "4000" == "40" == 0x4000000000000000, breaking range identity).
// The pattern enforces this directly — even-length lowercase hex, up to 8
// bytes, whose last byte is non-zero — so no CEL rule is needed.
type KeyRange struct {
	// +kubebuilder:validation:Pattern=`^$|^([0-9a-f]{2}){0,7}([0-9a-f][1-9a-f]|[1-9a-f][0-9a-f])$`
	// +kubebuilder:validation:MaxLength=16
	// +optional
	Start string `json:"start,omitempty"`

	// +kubebuilder:validation:Pattern=`^$|^([0-9a-f]{2}){0,7}([0-9a-f][1-9a-f]|[1-9a-f][0-9a-f])$`
	// +kubebuilder:validation:MaxLength=16
	// +optional
	End string `json:"end,omitempty"`
}

// +kubebuilder:validation:Enum=data;system
type ShardRole string

const (
	ShardRoleData   ShardRole = "data"
	ShardRoleSystem ShardRole = "system"
)

// ReplicationLinkPhase tracks a seeding/reverse link's lifecycle.
// +kubebuilder:validation:Enum=Pending;Copying;CatchingUp;Synced;Stopped
type ReplicationLinkPhase string

// ReplicationLink directs the agents to run a logical-replication workflow
// (reshard seeding forward or reverse); the data-plane engine executes it.
// +kubebuilder:validation:XValidation:rule="(has(self.sourceShard) && size(self.sourceShard) > 0) != (has(self.targetShard) && size(self.targetShard) > 0)",message="exactly one of sourceShard or targetShard must be set"
type ReplicationLink struct {
	Name string `json:"name"`

	// Exactly one of sourceShard/targetShard names the peer; the other end
	// is this shard.
	// +optional
	SourceShard string `json:"sourceShard,omitempty"`

	// +optional
	TargetShard string `json:"targetShard,omitempty"`

	Slot string `json:"slot"`

	Publication string `json:"publication"`

	// Rows outside this range are filtered agent-side.
	// +optional
	KeyRangeFilter *KeyRange `json:"keyRangeFilter,omitempty"`
}

// PgShardShardSpec defines the desired state of one shard. The operator
// writes it; the shard's agents watch it.
// +kubebuilder:validation:XValidation:rule="self.keyRange == oldSelf.keyRange",message="keyRange is immutable; reshard creates new shards"
type PgShardShardSpec struct {
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="clusterRef is immutable"
	ClusterRef string `json:"clusterRef"`

	KeyRange KeyRange `json:"keyRange"`

	// NodeRef names the PgShardNode that hosts this shard's database. The
	// cluster controller assigns it per the cluster's placement; the shard's
	// data lives as a Postgres database on that node.
	// +optional
	NodeRef string `json:"nodeRef,omitempty"`

	// +kubebuilder:default=data
	// +optional
	Role ShardRole `json:"role,omitempty"`

	// Instances including the primary.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=9
	Replicas int32 `json:"replicas"`

	// Serving is false for reshard targets before cutover and for
	// decommissioned sources after it.
	// +kubebuilder:default=false
	// +optional
	Serving bool `json:"serving,omitempty"`

	// Content hash of the shard's rendered PostgreSQL configuration; a
	// change drives the rolling-restart/reload flow.
	// +optional
	PostgresConfigHash string `json:"postgresConfigHash,omitempty"`

	// +optional
	Image string `json:"image,omitempty"`

	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// +optional
	Storage *StorageSpec `json:"storage,omitempty"`

	// pgBackRest stanza (includes the stanza generation suffix; a restore
	// starts a new generation).
	// +optional
	Stanza string `json:"stanza,omitempty"`

	// +optional
	ReplicationLinks []ReplicationLink `json:"replicationLinks,omitempty"`

	// Fenced freezes the shard: agents keep PostgreSQL down until lifted.
	// +optional
	Fenced bool `json:"fenced,omitempty"`
}

// +kubebuilder:validation:Enum=Provisioning;Ready;FailingOver;SwitchingOver;Degraded;Restoring;Decommissioning
type ShardPhase string

const (
	ShardProvisioning    ShardPhase = "Provisioning"
	ShardReady           ShardPhase = "Ready"
	ShardFailingOver     ShardPhase = "FailingOver"
	ShardSwitchingOver   ShardPhase = "SwitchingOver"
	ShardDegraded        ShardPhase = "Degraded"
	ShardRestoring       ShardPhase = "Restoring"
	ShardDecommissioning ShardPhase = "Decommissioning"
)

// InstanceRole of a PostgreSQL instance within the shard.
// +kubebuilder:validation:Enum=primary;replica
type InstanceRole string

// InstanceState is the operator's aggregated view of one instance,
// populated by polling the in-pod agent (agents do not write status).
type InstanceState struct {
	Pod string `json:"pod"`

	// +optional
	Role InstanceRole `json:"role,omitempty"`

	Ready bool `json:"ready,omitempty"`

	// Current WAL positions in PostgreSQL X/X text form.
	// +optional
	WalWriteLSN string `json:"walWriteLsn,omitempty"`

	// +optional
	WalReplayLSN string `json:"walReplayLsn,omitempty"`

	// +optional
	ReplayLagSeconds string `json:"replayLagSeconds,omitempty"`

	// +optional
	SyncState string `json:"syncState,omitempty"`
}

// BarrierRecord is the shard-local fact of the last consistency barrier.
type BarrierRecord struct {
	ID string `json:"id,omitempty"`

	// +optional
	LSN string `json:"lsn,omitempty"`

	// +optional
	RestorePoint string `json:"restorePoint,omitempty"`

	// +optional
	Time *metav1.Time `json:"time,omitempty"`
}

// ReplicationLinkStatus reports link progress.
type ReplicationLinkStatus struct {
	Name string `json:"name"`

	// +optional
	Phase ReplicationLinkPhase `json:"phase,omitempty"`

	// +optional
	LagBytes int64 `json:"lagBytes,omitempty"`

	// +optional
	LagSeconds string `json:"lagSeconds,omitempty"`
}

// PgShardShardStatus is the observed state of one shard. The
// targetPrimary/currentPrimary pair is the CNPG-style failover handshake:
// the operator sets the target; the elected agent promotes (guarded by the
// shard's Kubernetes Lease) and the operator records currentPrimary once
// the promotion is confirmed.
type PgShardShardStatus struct {
	// +optional
	Phase ShardPhase `json:"phase,omitempty"`

	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// +optional
	CurrentPrimary string `json:"currentPrimary,omitempty"`

	// +optional
	TargetPrimary string `json:"targetPrimary,omitempty"`

	// +optional
	TargetPrimaryTimestamp *metav1.Time `json:"targetPrimaryTimestamp,omitempty"`

	// Monotonic failover-decision epoch carried on agent Promote/Fence
	// calls; agents reject anything older than the highest they have seen.
	// +optional
	DecisionEpoch int64 `json:"decisionEpoch,omitempty"`

	// +optional
	Timeline int32 `json:"timeline,omitempty"`

	// +optional
	Instances []InstanceState `json:"instances,omitempty"`

	// +optional
	StanzaInitialized bool `json:"stanzaInitialized,omitempty"`

	// +optional
	LastBarrier *BarrierRecord `json:"lastBarrier,omitempty"`

	// +optional
	Links []ReplicationLinkStatus `json:"links,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef`
// +kubebuilder:printcolumn:name="Range",type=string,JSONPath=`.spec.keyRange.start`
// +kubebuilder:printcolumn:name="Serving",type=boolean,JSONPath=`.spec.serving`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Primary",type=string,JSONPath=`.status.currentPrimary`

// PgShardShard is one shard of a PgShardCluster (operator-managed).
type PgShardShard struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PgShardShardSpec   `json:"spec"`
	Status PgShardShardStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PgShardShardList contains a list of PgShardShard.
type PgShardShardList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PgShardShard `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PgShardShard{}, &PgShardShardList{})
		return nil
	})
}
