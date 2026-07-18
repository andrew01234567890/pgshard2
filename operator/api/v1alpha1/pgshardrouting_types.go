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

// RoutingShardState gates what traffic a shard entry accepts.
// +kubebuilder:validation:Enum=serving;buffered;readOnly;draining;hidden
type RoutingShardState string

const (
	RoutingServing  RoutingShardState = "serving"
	RoutingBuffered RoutingShardState = "buffered"
	RoutingReadOnly RoutingShardState = "readOnly"
	RoutingDraining RoutingShardState = "draining"
	RoutingHidden   RoutingShardState = "hidden"
)

// RoutingEndpoint is a directly addressable PostgreSQL instance. Routers
// connect to pod IPs published here (epoch-ordered) rather than Services,
// so routing changes never wait on kube-proxy propagation.
type RoutingEndpoint struct {
	// +kubebuilder:validation:MaxLength=253
	Pod string `json:"pod"`

	// Host is dialed directly by routers; the pattern keeps connection-string
	// metacharacters out of an operator-compiled value agents trust.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9._:-]+$`
	// +kubebuilder:validation:MaxLength=253
	Host string `json:"host"`

	// +kubebuilder:default=5432
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Port int32 `json:"port,omitempty"`

	// +optional
	CanRead bool `json:"canRead,omitempty"`
}

// RoutingShard is one shard's compiled routing entry.
type RoutingShard struct {
	Name string `json:"name"`

	KeyRange KeyRange `json:"keyRange"`

	State RoutingShardState `json:"state"`

	// +optional
	Primary *RoutingEndpoint `json:"primary,omitempty"`

	// +optional
	Replicas []RoutingEndpoint `json:"replicas,omitempty"`
}

// RoutingSequence binds a column to a global sequence.
type RoutingSequence struct {
	// +kubebuilder:validation:Pattern=`^[a-z_][a-z0-9_$]*$`
	// +kubebuilder:validation:MaxLength=63
	Column string `json:"column"`

	// +kubebuilder:validation:Pattern=`^[a-z_][a-z0-9_$]*$`
	// +kubebuilder:validation:MaxLength=63
	Sequence string `json:"sequence"`
}

// RoutingTable is one table's compiled routing entry. Agents build DDL from
// these identifiers, so they carry the same unquoted-identifier validation as
// the untrusted TableConfig they are projected from — the schema is the
// backstop if the compiler ever forgets to re-validate.
type RoutingTable struct {
	// +kubebuilder:validation:Pattern=`^[A-Za-z_][A-Za-z0-9_$]*$`
	// +kubebuilder:validation:MaxLength=63
	Schema string `json:"schema"`

	// +kubebuilder:validation:Pattern=`^[A-Za-z_][A-Za-z0-9_$]*$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`

	// Reuses the vschema TableType (sharded;global); the routing compiler
	// projects TableEntry.Type here, so the two must stay one enum.
	Type TableType `json:"type"`

	// +kubebuilder:validation:Pattern=`^[A-Za-z_][A-Za-z0-9_$]*$`
	// +kubebuilder:validation:MaxLength=63
	// +optional
	ShardKeyColumn string `json:"shardKeyColumn,omitempty"`

	// Type of the shard-key column, projected from TableEntry so the router can
	// coerce literals before hashing. The wire values match the Rust router's
	// `pgshard_topo::ShardKeyType`.
	// +optional
	ShardKeyType ShardKeyType `json:"shardKeyType,omitempty"`

	// +optional
	Sequences []RoutingSequence `json:"sequences,omitempty"`
}

// GateMatch selects the traffic a gate buffers.
type GateMatch struct {
	// +optional
	All bool `json:"all,omitempty"`

	// +optional
	Tables []string `json:"tables,omitempty"`

	// +optional
	KeyRanges []KeyRange `json:"keyRanges,omitempty"`
}

// RoutingGate is a buffering directive. The deadline is absolute: when it
// passes without an explicit open, routers fail-safe UNGATE and resume the
// prior routing — coordinators treat an expired gate as an aborted cutover.
type RoutingGate struct {
	ID string `json:"id"`

	Match GateMatch `json:"match"`

	// +kubebuilder:validation:Enum=bufferWrites;bufferAll
	// +kubebuilder:default=bufferWrites
	// +optional
	Mode string `json:"mode,omitempty"`

	Deadline metav1.Time `json:"deadline"`

	// On open, buffered sessions replay only once the router has applied a
	// topology at or beyond this generation.
	// +optional
	MinTopologyGeneration int64 `json:"minTopologyGeneration,omitempty"`
}

// PgShardRoutingSpec is the compiled, atomically versioned routing view —
// the single object routers and agents watch. Only the leader-elected
// operator writes it; every change is one write with a strictly monotonic
// epoch, and consumers apply an update iff epoch > lastApplied.
//
// Every spec change must strictly increase the epoch: consumers apply an
// update iff epoch > lastApplied, so a change that reused the current epoch
// would be silently ignored. An equal epoch is therefore allowed only for an
// identical (idempotent) re-apply. The topology generation may stay equal
// across non-structural epoch bumps but must never decrease.
// +kubebuilder:validation:XValidation:rule="self == oldSelf || self.epoch > oldSelf.epoch",message="any change must strictly increase epoch"
// +kubebuilder:validation:XValidation:rule="self.topologyGeneration >= oldSelf.topologyGeneration",message="topologyGeneration must not decrease"
type PgShardRoutingSpec struct {
	// +kubebuilder:validation:Minimum=1
	Epoch int64 `json:"epoch"`

	// Bumps only on structural change (shard set or table catalog).
	// +kubebuilder:validation:Minimum=1
	TopologyGeneration int64 `json:"topologyGeneration"`

	// +kubebuilder:default=10
	// +optional
	WriteLeaseSeconds int32 `json:"writeLeaseSeconds,omitempty"`

	// +kubebuilder:default="xxhash64_v1"
	// +optional
	HashFunction string `json:"hashFunction,omitempty"`

	// +kubebuilder:validation:MaxItems=256
	// +optional
	Shards []RoutingShard `json:"shards,omitempty"`

	// +kubebuilder:validation:MaxItems=2000
	// +optional
	Tables []RoutingTable `json:"tables,omitempty"`

	// +kubebuilder:validation:MaxItems=64
	// +optional
	Gates []RoutingGate `json:"gates,omitempty"`

	// System-shard primary that serves sequence blocks.
	// +optional
	SequenceEndpoint *RoutingEndpoint `json:"sequenceEndpoint,omitempty"`
}

// PgShardRoutingStatus records compilation provenance.
type PgShardRoutingStatus struct {
	// +optional
	CompiledAt *metav1.Time `json:"compiledAt,omitempty"`

	// +optional
	ObservedClusterGeneration int64 `json:"observedClusterGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Epoch",type=integer,JSONPath=`.spec.epoch`
// +kubebuilder:printcolumn:name="Generation",type=integer,JSONPath=`.spec.topologyGeneration`

// PgShardRouting is the operator-compiled routing view (one per cluster).
type PgShardRouting struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PgShardRoutingSpec   `json:"spec"`
	Status PgShardRoutingStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PgShardRoutingList contains a list of PgShardRouting.
type PgShardRoutingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PgShardRouting `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PgShardRouting{}, &PgShardRoutingList{})
		return nil
	})
}
