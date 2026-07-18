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

// TableType classifies a table in the sharding schema.
// +kubebuilder:validation:Enum=sharded;global
type TableType string

const (
	TableSharded TableType = "sharded"
	TableGlobal  TableType = "global"
)

// ShardKeyType is the type of a shard-key column. The router coerces a literal
// to it before hashing so that different spellings of one value (e.g. `id = 1`
// and `id = '1'`) route to the same shard. The string values are the wire
// contract with the Rust router's `pgshard_topo::ShardKeyType` — keep them in
// sync.
// +kubebuilder:validation:Enum=int;text;uuid;bytea
type ShardKeyType string

const (
	ShardKeyInt   ShardKeyType = "int"
	ShardKeyText  ShardKeyType = "text"
	ShardKeyUUID  ShardKeyType = "uuid"
	ShardKeyBytea ShardKeyType = "bytea"
)

// TableEntry declares one table's sharding configuration. Identifier fields
// flow into PostgreSQL DDL, so they are constrained at admission to
// unquoted-identifier syntax (≤63 bytes) to keep untrusted app-team input from
// injecting DDL — and to LOWERCASE only: PostgreSQL folds unquoted SQL
// identifiers to lowercase, and the router matches these names byte-exactly
// against the parser's folded form, so a mixed-case config name (`Orders`)
// could never match any query and would silently misconfigure routing.
// Case-sensitive quoted identifiers are not supported in M1.
type TableEntry struct {
	// +kubebuilder:default=public
	// +kubebuilder:validation:Pattern=`^[a-z_][a-z0-9_$]*$`
	// +kubebuilder:validation:MaxLength=63
	// +optional
	Schema string `json:"schema,omitempty"`

	// +kubebuilder:validation:Pattern=`^[a-z_][a-z0-9_$]*$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`

	// +kubebuilder:default=sharded
	// +optional
	Type TableType `json:"type,omitempty"`

	// Column hashed to the keyspace id. Required for sharded tables and
	// must be part of the primary key (enforced by the schema apply flow).
	// +kubebuilder:validation:Pattern=`^[a-z_][a-z0-9_$]*$`
	// +kubebuilder:validation:MaxLength=63
	// +optional
	ShardKeyColumn string `json:"shardKeyColumn,omitempty"`

	// Required for sharded tables; must match the column's actual PostgreSQL
	// type. See ShardKeyType.
	// +optional
	ShardKeyType ShardKeyType `json:"shardKeyType,omitempty"`

	// +optional
	Sequences []RoutingSequence `json:"sequences,omitempty"`
}

// SequenceEntry declares a global sequence hosted on the system shard.
type SequenceEntry struct {
	// +kubebuilder:validation:Pattern=`^[a-z_][a-z0-9_$]*$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`

	// Ids handed to a router per block grab.
	// +kubebuilder:default=1000
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000000
	// +optional
	BlockSize int64 `json:"blockSize,omitempty"`
}

// PgShardTableConfigSpec declares part of a cluster's sharding schema.
// Multiple PgShardTableConfig objects union together (app teams own their
// tables independently); the routing compiler validates that no table is
// declared twice across the union.
// +kubebuilder:validation:XValidation:rule="!has(self.tables) || self.tables.all(t, !has(t.type) || t.type != 'sharded' || (has(t.shardKeyColumn) && size(t.shardKeyColumn) > 0))",message="sharded tables must declare shardKeyColumn"
// +kubebuilder:validation:XValidation:rule="!has(self.tables) || self.tables.all(t, !has(t.type) || t.type != 'sharded' || (has(t.shardKeyType) && size(t.shardKeyType) > 0))",message="sharded tables must declare shardKeyType"
type PgShardTableConfigSpec struct {
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="clusterRef is immutable"
	ClusterRef string `json:"clusterRef"`

	// +kubebuilder:validation:MaxItems=500
	// +optional
	Tables []TableEntry `json:"tables,omitempty"`

	// +kubebuilder:validation:MaxItems=200
	// +optional
	Sequences []SequenceEntry `json:"sequences,omitempty"`
}

// PgShardTableConfigStatus reports compilation results.
type PgShardTableConfigStatus struct {
	// +optional
	Applied bool `json:"applied,omitempty"`

	// Routing epoch this config was last compiled into.
	// +optional
	CompiledIntoEpoch int64 `json:"compiledIntoEpoch,omitempty"`

	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef`
// +kubebuilder:printcolumn:name="Applied",type=boolean,JSONPath=`.status.applied`
// +kubebuilder:printcolumn:name="Epoch",type=integer,JSONPath=`.status.compiledIntoEpoch`

// PgShardTableConfig declares tables and sequences for a cluster (the
// vschema analog; app-team owned).
type PgShardTableConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PgShardTableConfigSpec   `json:"spec"`
	Status PgShardTableConfigStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PgShardTableConfigList contains a list of PgShardTableConfig.
type PgShardTableConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PgShardTableConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PgShardTableConfig{}, &PgShardTableConfigList{})
		return nil
	})
}
