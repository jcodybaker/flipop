/*

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
)

// FloatingIPPoolSpec defines the desired state of FloatingIPPool
type FloatingIPPoolSpec struct {
	IPs []string `json:"ips"`

	DesiredIPs int `json:"desiredIPs,omitempty"`

	// Provider describes the provider hosting the specified IPs. It's assumed all matching nodes
	// are associated with the specified provider.
	Provider string `json:"provider,omitempty"`

	// Region describes the region associated with the specified IPs. It's assumed all matching
	// nodes are associated with the specified region.
	Region string `json:"region,omitempty"`

	// Match describes the set of nodes
	Match Match `json:"match"`
}

// Match describes a pattern for finding resources the floating-ip should follow.
// NOTE: Currently only v1/Node and v1/Pod resources can be targeted.
type Match struct {
	// NodeLabel is used to restrict the nodes the IPs can be assigned to. Empty string matches all.
	NodeLabel string `json:"nodeLabel,omitempty"`

	// PodLabel, if specified, requires candidate nodes include at least one matching
	// pod in the "Ready" state.
	PodLabel string `json:"podLabel,omitempty"`

	// PodNamespace restricts the namespace used for pod matching.
	PodNamespace string `json:"podNamespace,omitempty"`

	// Tolerations is a list of node taints we will tolerate when deciding if the
	// node is a suitable candidate.
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
}

// Target describes a kubernetes resource.
type Target struct {
	Name       string `json:"name"`
	Namespace  string `json:"namespace,omitempty"`
	Kind       string `json:"kind"`
	APIVersion string `json:"apiVersion"`
}

// FloatingIPPoolStatus defines the observed state of FloatingIPPool.
type FloatingIPPoolStatus struct {
	IPs   map[string]IPStatus `json:"attached,omitempty"`
	Error string              `json:"error,omitempty"`
}

// IPStatus describes the mapping between IPs and the matching
// resources responsible for their attachment.
type IPStatus struct {
	IP         string   `json:"ip"`
	NodeName   string   `json:"nodeName"`
	ProviderID string   `json:"dropletID"`
	Targets    []Target `json:"targets,omitempty"`
	Error      string   `json:"error"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +resource:path=floatingippools

// FloatingIPPool is the Schema for the floatingippools API
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type FloatingIPPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FloatingIPPoolSpec   `json:"spec,omitempty"`
	Status FloatingIPPoolStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +resource:path=floatingippools

// FloatingIPPoolList contains a list of FloatingIPPool
type FloatingIPPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FloatingIPPool `json:"items"`
}
