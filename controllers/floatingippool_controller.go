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
	"container/list"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	"istio.io/istio/pkg/log"
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
	// ctx := context.Background()
	// log := r.Log.WithValues("floatingippool", req.NamespacedName)

	// r.poolLock.Lock()
	// defer r.poolLock.Unlock()

	// pool := r.pools[req.NamespacedName]

	// var updatedPool flipopv1.FloatingIPPool
	// if err := r.Get(ctx, req.NamespacedName, &updatedPool); err != nil {
	// 	// 1) Was the pool deleted?
	// 	if apierrors.IsNotFound(err) {
	// 		if pool != nil {
	// 			log.Info("FloatingIPPool removed, shutting down child controller")
	// 			pool.shutdown()
	// 		} else {
	// 			log.Info("FloatingIPPool removed, no active child controller")
	// 		}
	// 		return ctrl.Result{}, nil
	// 	}
	// 	log.Error(err, "unable to fetch FloatingIPPool")
	// 	return ctrl.Result{}, err
	// }

	// // 2) Is this a new pool?
	// if pool == nil {
	// 	var nonFatalErrors []string

	// 	pool = &floatingIPPool{
	//		log: r.Log.WithValues("floatingippool", req.NamespacedName),
	//	}
	// 	pool.updateK8s(&updatedPool, r.Providers)

	// 	for ip, node := range ipToNode {
	// 		// At this point the node is either assigned. If it doesn't match, it will be
	// 		// unassigned, but isn't a candidate, because it doesn't match.
	// 		delete(assignableNodes, node.Name)
	// 		match, err := isMatch(&updatedPool.Spec.Match, nodeSelector, node)
	// 		if err != nil {
	// 			log.Error(err, "evaluating node", "node", node.Name)
	// 			return ctrl.Result{}, err
	// 		}
	// 		if !match {
	// 			// This node was assigned to an in cluster node, but it no-longer matches.
	// 			// It's now a candidate for reassignment to another node, BUT we put it at the
	// 			// end up the list in-case it recovers.
	// 			delete(ipToNode, ip)
	// 			assignableIPs = append(assignableIPs, ip)
	// 		}
	// 	}

	// nodeLoop:
	// 	for _, n := range assignableNodes {
	// 		for _, ip := range assignableIPs {
	// 			match, err := f.isMatch(n)
	// 			if err != nil {
	// 				log.Error(err, "evaluating node", "node", n.getName())
	// 				return ctrl.Result{}, err
	// 			}
	// 			if match {
	// 				err := prov.AssignIP(ctx, ip, n.getProviderID())
	// 				if err != nil {
	// 					log.Error(err, "assigning ip to node", "node", n.getName(), "ip", ip)
	// 					return ctrl.Result{}, err
	// 				}
	// 				ipToNode[ip] = node
	// 				continue nodeLoop
	// 			}
	// 		}
	// 		break
	// 	}
	// 	if len(assignableNodes) == 0 && len(assignableIPs) > 0 {
	// 		log.Info("no suitable nodes found for IPs", "ips", assignableIPs)
	// 	}

	// 	// Update status

	// 	if err != nil {
	// 		return ctrl.Result{}, err
	// 	}

	// 	return ctrl.Result{}, nil
	// }

	// // 3) Was the pool changed?
	// pool.shutdown()
	// pool.pool = updatedPool.DeepCopy()
	// // TODO - Reevaluate current nodes.
	// pool.start()
	// return ctrl.Result{}, nil
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

type floatingIPPool struct {
	k8s *flipopv1.FloatingIPPool
	// cache the parsed selectors
	nodeSelector labels.Selector
	podSelector  labels.Selector

	provider provider.Provider

	nodeNameToNode map[string]*node
	ipToNode       map[string]*node

	// TODO(cbaker) - Track last error time and avoid retrying for x period.
	ipErrors map[string]string

	// assignableIPs are available for reassignment
	assignableIPs *list.List
	// assignableNodes are matching, but not yet assigned to an IP.
	assignableNodes map[string]*node

	log logr.Logger
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
	f.nodeNameToNode = make(map[string]*node)
	f.ipToNode = make(map[string]*node)
	for _, ip := range f.k8s.Spec.IPs {
		f.ipToNode[ip] = nil
	}

	f.ipToError = make(map[string]string)

	f.assignableNodes = make(map[string]*node)
	f.assignableIPs = list.New()

	var nodeList corev1.NodeList
	err = c.List(ctx, &nodeList, client.MatchingLabelsSelector{Selector: pool.nodeSelector})
	if err != nil {
		return err
	}
	for _, k8sNode := range nodeList.List {
		err = f.updateNode(&k8sNode)
		if err != nil {
			return err
		}
	}
	for _, ip := range f.k8s.Spec.IPs {
		if f.ipToNode[ip] != nil {
			continue
		}
		f.assignableIPs.PushBack(ip)
	}
	return nil
}

func (f *floatingIPPool) assign() error {
	for _, node := range assignableNodes {
		if f.assignableIPs.Len() == 0 {
			return nil
		}
		err := prov.AssignIP(ctx, ip, node.Spec.ProviderID)
		if err != nil {
			// This error might be with the node (ex. already has an IP or a pending action)
			log.Error(err, "assigning ip to node", "node", node.Name, "ip", ip)
			return err
		}
		ip := f.assignableIPs.Remove(f.assignableIPs.Front()).(string)
		ipToNode[ip] = node
	}
	return nil
}

func (f *floatingIPPool) updateNode(k8s *corev1.Node) error {
	n, ok := f.nodeNameToNode[k8s.Name]
	if !ok {
		if !pod.ObjectMeta.DeletionTimestamp.IsZero() {
			return nil
		}
		providerID := node.Spec.ProviderID
		if providerID == "" {
			log.Info("node has no provider id, ignoring", "node", node.Name)
			return nil
		}
		n = newNode(k8s)
		f.nodeNameToNode[n.getName()] = n
		ip, err := f.provider.NodeToIP(providerID)
		if err != nil {
			return err
		}
		if ip != "" {
			delete(f.ipErrors, ip)
			oldNode, isIPKnown := f.ipToNode[ip]
			if isIPKnown {
				if oldNode != nil {
					// Only should happen w/ OOB API edits, nevertheless, keep our accounting clean
					oldNode.ip = ""
				}
				f.ipToNode[ip] = n
			}
		}
	}

	if n.ip != "" {
		delete(n.ipErrors, n.ip)
	}

	if !pod.ObjectMeta.DeletionTimestamp.IsZero() {
		ip := n.ip
		f.releaseNode(n)
		delete(f.nodeNameToNode, n.getName())
		delete(f.ipToNode, ip)
		return nil
	}

	var err error
	var oldNodeMatch = n.isNodeMatch
	n.isNodeMatch, err = f.isNodeMatch(n)
	if err != nil {
		return err
	}

	if oldNodeMatch == n.isNodeMatch {
		return nil
	}

	if n.isNodeMatch {
		if n.k8s.Spec.Match.PodNamespace != "" || n.k8s.Spec.Match.PodMatch != nil {
			// TODO - query pods. We had been ignoring this node.
			// f.Client
		}
		f.setNodeAssignable(n)
	}
	return nil
}

func (f *floatingIPPool) updatePod(pod *corev1.Pod) error {
	if pod.Spec.NodeName == "" {
		// This pod hasn't been assigned to a node. Once a pod is assigned to a node, it cannot be
		// unassigned.
		return nil
	}
	n, ok := f.nodeNameToNode[pod.Spec.NodeName]
	if !ok {
		// We don't know about the node. When the node event comes in, we'll query all pods.
		f.log.Info("pod referenced unknown node", "node", pod.Spec.NodeName, "pod", pod.Name, "namespace", pod.Namespace)
		return nil
	}
	// Pods spec & metadata (labels+namespace) are immutable. If it doesn't match now it never did.
	if f.k8s.Spec.Match.PodNamespace != "" && pod.Namespace != f.k8s.Spec.Match.PodNamespace {
		return nil
	}
	if f.podSelector != nil && !f.podSelector.Matches(f.k8s.Labels) {
		return nil
	}

	podKey := podNamespacedName(pod)
	_, active := n.matchingPods[podKey]

	if !pod.ObjectMeta.DeletionTimestamp.IsZero() {
		// TODO delete finalizer
		if !active {
			return nil
		}
		delete(n.matchingPods, podKey)
		if len(n.matchingPods) == 0 {
			f.releaseNode(n)
		}
	} else {
		if pod.Status == nil {
			if active {
				delete(n.matchingPods, podKey)
			}
			return
		}
		running := pod.Status.Phase == corev1.PodPhaseRunning
		var ready bool
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodConditionReady {
				ready = true
			}
		}
		if (ready && running) == active {
			return nil // no change
		}
		if ready && running {
			n.matchingPods[podKey] = pod.DeepCopy()
			if len(n.matchingPods) == 1 {
				f.setNodeAssignable(n)
			}
		} else {
			delete(n.matchingPods, podKey)
			if len(n.matchingPods) == 0 {
				f.releaseNode(n)
			}
		}
	}
	return nil
}

func (f *floatingIPPool) setNodeAssignable(n *node) {
	if n.ip != "" {
		// The node still has an IP assigned, we just need to remove it from the assignable pool.
		for e := f.assignableIPs.Front(); e != nil; e = e.Next() {
			ip := e.Value.(string)
			if n.ip == ip {
				f.assignableIPs.Remove(e)
				f.log.Info("assignable node already has IP, removing from pool", "node", n.getName(), "ip", ip)
				return
			}
		}
		// We shouldn't get here, if we do the memory structures are corrupt.
		panic("assignable node claims IP, but IP is not assignable.")
	}
	f.assignableNodes[n.getName()] = n
}

func (f *floatingIPPool) releaseNode(n *node) {
	if n.ip == "" {
		delete(f.assignableNodes, n.getName())
		return
	}
	ip = n.ip
	n.ip = ""
	f.assignableIPs.PushBack(ip)
}

func (f *floatingIPPool) setStatus(errMsg string) error {
	status := flipopv1.FloatingIPPoolStatus{}
	for ip, n := range ipToNode {
		ipStatus = &flipopv1.IPStatus{
			Error: f.ipErrors[ip],
		}
		if n != nil {
			ipStatus.NodeName = n.getName()
			ipStatus.ProviderID = n.getProviderID()
			for _, pod := range n.pods {
				ipStatus.Targets = append(ipStatus.Targets, flipopv1.IPStatus{
					APIVersion: pod.APIVersion,
					Kind:       pod.Kind,
					Name:       pod.Name,
					Namespace:  pod.Namespace,
				})
			}
		}
		status.IPs[ip] = *ipStatus
	}
	status.error = errMsg
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
	isNodeMatch  bool
	ip           string
	matchingPods map[string]*corev1.Pod
}

func newNode(n *corev1.Node) *node {
	return &node{
		k8s:          n.DeepCopy(),
		matchingPods: make(map[string]*corev1.Pod),
	}
}

func (n *node) getName() string {
	return n.k8s.Name
}

func (n *node) getProviderID() string {
	return n.k8s.Spec.ProviderID
}

func podNamespacedName(pod *corev1.Pod) string {
	return fmt.Sprintf("%s/%s", pod.Name, pod.Namespace)
}
