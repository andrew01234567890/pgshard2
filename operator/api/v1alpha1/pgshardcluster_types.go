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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// PostgresSpec pins the PostgreSQL version and tuning inputs.
type PostgresSpec struct {
	// Major version; pgshard requires PostgreSQL 18 or newer.
	// +kubebuilder:validation:Pattern=`^(1[89]|[2-9][0-9])$`
	Version string `json:"version"`

	// Image overrides the default pgshard-postgres image for this version.
	// +optional
	Image string `json:"image,omitempty"`

	// Parameters are merged over the auto-configuration derived from the
	// size class; restart-classified parameters roll out online.
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`

	// HashFunction maps shard keys to keyspace ids. Immutable.
	// +kubebuilder:default="xxhash64_v1"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="hashFunction is immutable"
	// +optional
	HashFunction string `json:"hashFunction,omitempty"`
}

// +kubebuilder:validation:Enum=S;M;L;XL
type SizeClass string

// SynchronousSpec configures synchronous replication within a shard.
type SynchronousSpec struct {
	// +kubebuilder:validation:Enum=off;first;quorum
	// +kubebuilder:default=quorum
	// +optional
	Mode string `json:"mode,omitempty"`

	// Number of synchronous standbys required.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +optional
	Number int32 `json:"number,omitempty"`
}

// StorageSpec describes per-instance persistent storage.
type StorageSpec struct {
	// +kubebuilder:validation:Required
	Size resource.Quantity `json:"size"`

	// +optional
	StorageClass string `json:"storageClass,omitempty"`

	// WalSeparate provisions a dedicated WAL volume.
	// +optional
	WalSeparate bool `json:"walSeparate,omitempty"`
}

// SizeOverrides sets explicit values on top of (or instead of) a size class.
type SizeOverrides struct {
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// +optional
	Storage *StorageSpec `json:"storage,omitempty"`

	// Instances per shard including the primary.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=9
	// +optional
	ReplicasPerShard *int32 `json:"replicasPerShard,omitempty"`

	// +optional
	Synchronous *SynchronousSpec `json:"synchronous,omitempty"`
}

// SizeSpec chooses auto-configuration inputs.
type SizeSpec struct {
	// +optional
	Class SizeClass `json:"class,omitempty"`

	// +optional
	Overrides *SizeOverrides `json:"overrides,omitempty"`
}

// ShardsSpec fixes the initial shard layout. All later changes go through
// PgShardReshard.
type ShardsSpec struct {
	// Number of equal-range shards at cluster creation. Immutable.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=128
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="initialCount is immutable; reshard instead"
	InitialCount int32 `json:"initialCount"`
}

// RouterSpec sizes the router deployment.
type RouterSpec struct {
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// A router that cannot refresh routing within this lease stops
	// accepting writes (bounds the stale-router window during failover).
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=60
	// +optional
	WriteLeaseSeconds int32 `json:"writeLeaseSeconds,omitempty"`
}

// SystemSpec sizes the unsharded system shard (sequences, migration state).
type SystemSpec struct {
	// +optional
	Storage *StorageSpec `json:"storage,omitempty"`
}

// BackupRepoSpec locates the pgBackRest repository.
type BackupRepoSpec struct {
	// +kubebuilder:validation:Enum=s3;gcs;azure
	Type string `json:"type"`

	Bucket string `json:"bucket"`

	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// +optional
	Region string `json:"region,omitempty"`

	// Secret with repository credentials (provider-specific keys).
	CredentialsSecretRef corev1.LocalObjectReference `json:"credentialsSecretRef"`
}

// BackupSchedulesSpec are cron expressions per backup type.
type BackupSchedulesSpec struct {
	// +optional
	Full string `json:"full,omitempty"`

	// +optional
	Differential string `json:"differential,omitempty"`

	// +optional
	Incremental string `json:"incremental,omitempty"`
}

// BarrierSpec controls cross-shard consistency barriers (the coordinated
// restore points PITR resolves against).
type BarrierSpec struct {
	// +kubebuilder:default="5m"
	// +optional
	Interval metav1.Duration `json:"interval,omitempty"`

	// gated briefly buffers writes for a cross-shard consistent point;
	// loose skips the gate (documented causal caveat).
	// +kubebuilder:validation:Enum=gated;loose
	// +kubebuilder:default=gated
	// +optional
	Mode string `json:"mode,omitempty"`

	// +kubebuilder:default="2s"
	// +optional
	GateDeadline metav1.Duration `json:"gateDeadline,omitempty"`

	// Switch WAL after the barrier so it is restorable immediately.
	// +kubebuilder:default=true
	// +optional
	ForceWalSwitch *bool `json:"forceWalSwitch,omitempty"`
}

// RetentionSpec bounds repository growth.
type RetentionSpec struct {
	// +kubebuilder:default=4
	// +kubebuilder:validation:Minimum=1
	// +optional
	FullCount int32 `json:"fullCount,omitempty"`

	// +kubebuilder:default=14
	// +kubebuilder:validation:Minimum=1
	// +optional
	RecoveryWindowDays int32 `json:"recoveryWindowDays,omitempty"`
}

// BackupSpec wires a cluster to object storage.
type BackupSpec struct {
	Repo BackupRepoSpec `json:"repo"`

	// +optional
	Schedules BackupSchedulesSpec `json:"schedules,omitempty"`

	// +optional
	Barrier BarrierSpec `json:"barrier,omitempty"`

	// +optional
	Retention RetentionSpec `json:"retention,omitempty"`
}

// TLSSpec selects certificate management.
type TLSSpec struct {
	// +kubebuilder:validation:Enum=operatorCA;secretRef
	// +kubebuilder:default=operatorCA
	// +optional
	Mode string `json:"mode,omitempty"`

	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`
}

// RestoredFrom records restore provenance on a restored cluster.
type RestoredFrom struct {
	RepoPath string `json:"repoPath,omitempty"`

	BarrierID string `json:"barrierId,omitempty"`

	SourceTopologyGeneration int64 `json:"sourceTopologyGeneration,omitempty"`
}

// PgShardClusterSpec defines the desired state of PgShardCluster.
type PgShardClusterSpec struct {
	Postgres PostgresSpec `json:"postgres"`

	// +optional
	Size SizeSpec `json:"size,omitempty"`

	Shards ShardsSpec `json:"shards"`

	// +optional
	Router RouterSpec `json:"router,omitempty"`

	// +optional
	System SystemSpec `json:"system,omitempty"`

	// +optional
	Backup *BackupSpec `json:"backup,omitempty"`

	// +optional
	TLS TLSSpec `json:"tls,omitempty"`

	// Pause suspends reconciliation (hibernation).
	// +optional
	Pause bool `json:"pause,omitempty"`
}

// ShardCounts aggregates shard readiness; per-shard detail lives on
// PgShardShard so this object stays O(1) regardless of shard count.
type ShardCounts struct {
	Total int32 `json:"total,omitempty"`

	Ready int32 `json:"ready,omitempty"`

	Degraded int32 `json:"degraded,omitempty"`
}

// RouterCounts aggregates router readiness and epoch acknowledgement.
type RouterCounts struct {
	Ready int32 `json:"ready,omitempty"`

	MinAckedEpoch int64 `json:"minAckedEpoch,omitempty"`
}

// ClusterBackupStatus surfaces the latest barrier and backup facts.
type ClusterBackupStatus struct {
	LastBarrierID string `json:"lastBarrierId,omitempty"`

	// +optional
	LastBarrierTime *metav1.Time `json:"lastBarrierTime,omitempty"`

	// +optional
	LastFullBackupTime *metav1.Time `json:"lastFullBackupTime,omitempty"`

	WalArchivingHealthy bool `json:"walArchivingHealthy,omitempty"`
}

// +kubebuilder:validation:Enum=Provisioning;Ready;Degraded;Resharding;Restoring;Paused
type ClusterPhase string

const (
	ClusterProvisioning ClusterPhase = "Provisioning"
	ClusterReady        ClusterPhase = "Ready"
	ClusterDegraded     ClusterPhase = "Degraded"
	ClusterResharding   ClusterPhase = "Resharding"
	ClusterRestoring    ClusterPhase = "Restoring"
	ClusterPaused       ClusterPhase = "Paused"
)

// PgShardClusterStatus defines the observed state of PgShardCluster.
type PgShardClusterStatus struct {
	// +optional
	Phase ClusterPhase `json:"phase,omitempty"`

	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Mirror of the compiled PgShardRouting epoch.
	// +optional
	RoutingEpoch int64 `json:"routingEpoch,omitempty"`

	// Bumps only on structural change (shard set or table catalog).
	// +optional
	TopologyGeneration int64 `json:"topologyGeneration,omitempty"`

	// +optional
	Shards ShardCounts `json:"shards,omitempty"`

	// +optional
	Routers RouterCounts `json:"routers,omitempty"`

	// +optional
	Backup ClusterBackupStatus `json:"backup,omitempty"`

	// +optional
	RestoredFrom *RestoredFrom `json:"restoredFrom,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Shards",type=integer,JSONPath=`.status.shards.total`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.shards.ready`
// +kubebuilder:printcolumn:name="Epoch",type=integer,JSONPath=`.status.routingEpoch`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PgShardCluster is the root object of a sharded PostgreSQL cluster.
type PgShardCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PgShardClusterSpec   `json:"spec,omitempty"`
	Status PgShardClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PgShardClusterList contains a list of PgShardCluster.
type PgShardClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PgShardCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PgShardCluster{}, &PgShardClusterList{})
		return nil
	})
}
