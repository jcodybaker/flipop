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

package controllers

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	"istio.io/istio/pkg/log"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	flipopv1 "github.com/jcodybaker/flipop/api/v1"
	"github.com/jcodybaker/flipop/pkg/provider"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

// FloatingIPPoolReconciler reconciles a FloatingIPPool object
type FloatingIPPoolReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme

	Manager ctrl.Manager

	Providers map[string]provider.Provider

	poolLock sync.Mutex
	pools    map[types.NamespacedName]*floatingIPPool
}

// +kubebuilder:rbac:groups=flipop.codybaker.com,resources=floatingippools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=flipop.codybaker.com,resources=floatingippools/status,verbs=get;update;patch

// Reconcile assigns floating IPs to matching nodes.
func (r *FloatingIPPoolReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := r.Log.WithValues("floatingippool", req.NamespacedName)

	r.poolLock.Lock()
	defer r.poolLock.Unlock()

	pool := r.pools[req.NamespacedName]

	var updatedPool flipopv1.FloatingIPPool
	if err := r.Get(ctx, req.NamespacedName, &updatedPool); err != nil {
		// 1) Was the pool deleted?
		if apierrors.IsNotFound(err) {
			if pool != nil {
				log.Info("FloatingIPPool removed, shutting down child controller")
				pool.shutdown()
			} else {
				log.Info("FloatingIPPool removed, no active child controller")
			}
			return ctrl.Result{}, nil
		}
		log.Error(err, "unable to fetch FloatingIPPool")
		return ctrl.Result{}, err
	}

	// 2) Is this a new pool?
	if pool == nil {
		var nonFatalErrors []string

		pool = &floatingIPPool{}
		pool.updateK8s(&updatedPool, r.Providers)

		for ip, node := range ipToNode {
			// At this point the node is either assigned. If it doesn't match, it will be
			// unassigned, but isn't a candidate, because it doesn't match.
			delete(assignableNodes, node.Name)
			match, err := isMatch(&updatedPool.Spec.Match, nodeSelector, node)
			if err != nil {
				log.Error(err, "evaluating node", "node", node.Name)
				return ctrl.Result{}, err
			}
			if !match {
				// This node was assigned to an in cluster node, but it no-longer matches.
				// It's now a candidate for reassignment to another node, BUT we put it at the
				// end up the list in-case it recovers.
				delete(ipToNode, ip)
				assignableIPs = append(assignableIPs, ip)
			}
		}

	nodeLoop:
		for _, node := range assignableNodes {
			for _, ip := range assignableIPs {
				match, err := isMatch(&updatedPool.Spec.Match, nodeSelector, node)
				if err != nil {
					log.Error(err, "evaluating node", "node", node.Name)
					return ctrl.Result{}, err
				}
				if match {
					err := prov.AssignIP(ctx, ip, node.Spec.ProviderID)
					if err != nil {
						log.Error(err, "assigning ip to node", "node", node.Name, "ip", ip)
						return ctrl.Result{}, err
					}
					ipToNode[ip] = node
					continue nodeLoop
				}
			}
			break
		}
		if len(assignableNodes) == 0 && len(assignableIPs) > 0 {
			log.Info("no suitable nodes found for IPs", "ips", assignableIPs)
		}

		// Update status

		if err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{}, nil
	}

	// 3) Was the pool changed?
	pool.shutdown()
	pool.pool = updatedPool.DeepCopy()
	// TODO - Reevaluate current nodes.
	pool.start()
	return ctrl.Result{}, nil
}

// SetupWithManager ...
func (r *FloatingIPPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		For(&flipopv1.FloatingIPPool{}).
		Complete(r)
	if err != nil {
		return err
	}

	return err
}

func (r *FloatingIPPoolReconciler) loadCurrentProviderMapping(ips []string) (map[string]string, error) {
	return nil, nil
}

type floatingIPPool struct {
	k8s *flipopv1.FloatingIPPool
	// cache the parsed selectors
	nodeSelector labels.Selector
	podSelector  labels.Selector

	provider provider.Provider

	nodeNameToNode   map[string]*node
	providerIDToNode map[string]*node
	ipToNode         map[string]*node

	// TODO(cbaker) - Track last error time and avoid retrying for x period.
	ipErrors map[string]string

	// assignableIPs are available for reassignment
	assignableIPs   []string
	// assignableNodes are matching, but not yet assigned to an IP.
	assignableNodes map[string]*node
}

func (f *floatingIPPool) updateK8s(resource *flipopv1.FloatingIPPool, providers map[string]provider.Provider) error {
	f.k8s = resource.DeepCopy()

	var err error
	if f.k8s.Spec.Match.NodeLabel != "" {
		f.nodeSelector, err = labels.Parse(f.k8s.Spec.Match.NodeLabel)
		if err != nil {
			return fmt.Errorf("parsing match node label: %w", err)
		}
	} else {
		f.nodeSelector = labels.Everything()
	}

	if f.k8s.Spec.Match.PodLabel != "" {
		f.podSelector, err = labels.Parse(f.k8s.Spec.Match.PodLabel)
		if err != nil {
			return fmt.Errorf("parsing match pod label: %w", err)
		}
	} else {
		f.podSelector = labels.Everything()
	}

	prov := providers[f.k8s.Spec.Provider]
	if prov == nil {

		// Provided the status update is successful, err will be nil.  That's fine because
		// retries in this controller won't be successful.
		return ctrl.Result{}, err
	}
}

func (f *floatingIPPool) resync(c client.Client) error {
	f.providerIDToNode = make(map[string]*node)
	f.nodeNameToNode = make(map[string]*node)
	f.ipToNode = make(map[string]*node)

	f.ipToError = make(map[string]string)

	f.assignableNodes = make(map[string]*node)
	f.assignableIPs = []string

	var nodeList corev1.NodeList
	err = c.List(ctx, &nodeList, client.MatchingLabelsSelector{Selector: pool.nodeSelector})
	if err != nil {
		return ctrl.Result{}, err
	}
	for _, k8sNode := range nodeList.List {
		providerID := node.Spec.ProviderID
		if providerID == "" {
			log.Info("node has no provider id, ignoring", "node", node.Name)
			continue
		}
		n := newNode(k8sNode)
		n.isCandidate = f.isNodeMatch(n)

		f.providerIDToNode[providerID] = n
		f.nodeNameToNode[n.Name] = n
		f.assignableNodes[n.Name] = n
	}

	var assignedButNotMatching []string
	for _, ip := range f.k8s.Spec.IPs {
		providerID, err := f.provider.IPtoProviderID(ctx, ip)
		if 	err != nil {
			f.ipToError[ip] = fmt.Sprintf("querying status: provider error"
			log.Error(err, "querying IP status: provider error", "ip", ip)
			continue
		}
		if providerID == "" {
			f.assignableIPs = append(f.assignableIPs, ip)
			log.Info("ip available for mapping", "ip", ip)
			continue
		}
		node := f.providerIDToNode[providerID]
		if node == nil {
			f.assignableIPs = append(f.assignableIPs, ip)
			log.Info("ip associated with an unknown node", "ip", ip)
			continue
		}
		log.Info("ip associated with node", "ip", ip)
		f.ipToNode[ip] = node
		if node.isMatch {
			// This node matches and is already assigned an IP, no changes needed.
			delete(f.assignableNodes, node.getName())
		} else {
			assignedButNotMatching = append(assignedButNotMatching, ip)
		}
	}
	// assignedButNotMatching are assigned to node which matched at one point, but are no longer 
	// matching.  We consider them available for assignment, but will prioritize totally unassigned
	// IPs (because they're at the front of the list). The nodes associated with these IPs might
	// recover (become matching). If we haven't reassigned them, we'll just retain the current
	// mapping and avoid unnecessary churn.
	f.assignableIPs = append(f.assignableIPs, assignedButNotMatching...)
}

func (f *floatingIPPool) assign() error {
	for _, node := range assignableNodes {
		if len(f.assignableIPs) == 0 {
			return nil
		}
		err := prov.AssignIP(ctx, ip, node.Spec.ProviderID)
		if err != nil {
			// This error might be with the node (ex. already has an IP or a pending action)
			log.Error(err, "assigning ip to node", "node", node.Name, "ip", ip)
			return err
		}
		ipToNode[ip] = node
		f.assignableIPs = f.assignableIPs[1:]
	}
	return nil
}

func (f *floatingIPPool) 

func (f *floatingIPPool) setStatus(errors []string) error {
	status := flipopv1.FloatingIPPoolStatus{}
	for ip, node := range ipToNode {
		status.Attached = append(status.Attached, flipopv1.Attachment{
			IP:         ip,
			NodeName:   node.Name,
			ProviderID: node.Spec.ProviderID,
		})
	}
	status.Errors = errors
	updatedPool.Status = status
	return r.Status().Update(ctx, &updatedPool)
}

func (f *floatingIPPool) isNodeMatch(node *corev1.Node) (bool, error) {
	if f.nodeSelector.Matches(labels.Set(node.Labels)) {
		return false, nil
	}

taintLoop:
	for _, taint := range node.Spec.Taints {
		for _, tol := range f.k8s.Spec.Match.Tolerations {
			if tol.ToleratesTaint(&taint) {
				continue taintLoop
			}
		}
		return false, nil
	}

	return true, nil
}

type node struct {
	k8s          *corev1.Node
	isMatch      bool
	matchingPods map[string]*corev1.Pod
}

func newNode(n *corev1.Node) *node {
	return &node{
		k8s:          n.DeepCopy(),
		matchingPods: make(map[string]*corev1.Pod),
	}
}

func getName() string {
	return k8s.Name
}