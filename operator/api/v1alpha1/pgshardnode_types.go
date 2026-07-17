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

// PgShardNodeSpec is the desired state of one physical PostgreSQL instance
// group (a primary plus replicas) — the HA, storage, and backup unit. A node
// hosts one or more shard databases; which shards land on it is a placement
// decision recorded on the PgShardShards, not here, so a single node can serve
// several clusters' shards (dense/shared placement) or exactly one (dedicated).
type PgShardNodeSpec struct {
	// Instances including the primary.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=9
	Replicas int32 `json:"replicas"`

	// +optional
	Image string `json:"image,omitempty"`

	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// +optional
	Storage *StorageSpec `json:"storage,omitempty"`

	// Content hash of the node's rendered PostgreSQL configuration; a change
	// drives the rolling-restart/reload flow.
	// +optional
	PostgresConfigHash string `json:"postgresConfigHash,omitempty"`

	// Fenced freezes the node: agents keep PostgreSQL down until lifted.
	// +optional
	Fenced bool `json:"fenced,omitempty"`
}

// +kubebuilder:validation:Enum=Provisioning;Ready;FailingOver;Degraded
type NodePhase string

const (
	NodeProvisioning NodePhase = "Provisioning"
	NodeReady        NodePhase = "Ready"
	NodeFailingOver  NodePhase = "FailingOver"
	NodeDegraded     NodePhase = "Degraded"
)

// PgShardNodeStatus is the observed state of one node. The
// targetPrimary/currentPrimary pair is the CNPG-style failover handshake:
// the operator sets the target; the elected agent promotes (guarded by the
// node's Kubernetes Lease) and the operator records currentPrimary once the
// promotion is confirmed. The operator polls the in-pod agents and is the sole
// writer of this status; agents never write it.
type PgShardNodeStatus struct {
	// +optional
	Phase NodePhase `json:"phase,omitempty"`

	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// +optional
	CurrentPrimary string `json:"currentPrimary,omitempty"`

	// +optional
	TargetPrimary string `json:"targetPrimary,omitempty"`

	// +optional
	TargetPrimaryTimestamp *metav1.Time `json:"targetPrimaryTimestamp,omitempty"`

	// Monotonic failover-decision epoch carried on agent Promote/Fence calls;
	// agents reject anything older than the highest they have seen.
	// +optional
	DecisionEpoch int64 `json:"decisionEpoch,omitempty"`

	// +optional
	Timeline int32 `json:"timeline,omitempty"`

	// +optional
	Instances []InstanceState `json:"instances,omitempty"`

	// +optional
	StanzaInitialized bool `json:"stanzaInitialized,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Primary",type=string,JSONPath=`.status.currentPrimary`

// PgShardNode is one physical PostgreSQL instance group (operator-managed).
type PgShardNode struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PgShardNodeSpec   `json:"spec"`
	Status PgShardNodeStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PgShardNodeList contains a list of PgShardNode.
type PgShardNodeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PgShardNode `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PgShardNode{}, &PgShardNodeList{})
		return nil
	})
}
