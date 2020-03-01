/*
MIT License

Copyright (c) 2020 John Cody Baker

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/

package controllers

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	flipopv1alpha1 "github.com/jcodybaker/flipop/pkg/apis/flipop/v1alpha1"
	"github.com/jcodybaker/flipop/pkg/provider"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	flipopCS "github.com/jcodybaker/flipop/pkg/apis/flipop/generated/clientset/versioned"

	// k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	// metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	flipopInformers "github.com/jcodybaker/flipop/pkg/apis/flipop/generated/informers/externalversions/flipop/v1alpha1"
	corev1Informers "k8s.io/client-go/informers/core/v1"
)

const (
	floatingIPPoolResyncPeriod = 5 * time.Minute
	podResyncPeriod            = 5 * time.Minute
	nodeResyncPeriod           = 5 * time.Minute
	podNodeNameIndexerName     = "podNodeName"
)

// FloatingIPPoolController watches for FloatingIPPool and then manages reconciliation for each
// pool.
type FloatingIPPoolController struct {
	kubeCS   kubernetes.Interface
	flipopCS flipopCS.Interface

	providers map[string]provider.Provider

	pools    map[string]*floatingIPPool
	poolLock sync.Mutex

	// Fields provided at runtime.
	ll  logrus.FieldLogger
	ctx context.Context
}

// NewFloatingIPPoolController creates a new FloatingIPPoolController.
func NewFloatingIPPoolController(kubeConfig clientcmd.ClientConfig, providers map[string]provider.Provider) (*FloatingIPPoolController, error) {
	c := &FloatingIPPoolController{
		providers: providers,
	}
	var err error
	clientConfig, err := kubeConfig.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("building kubernetes client config")
	}
	c.kubeCS, err = kubernetes.NewForConfig(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("building kubernetes clientset: %w", err)
	}
	c.flipopCS, err = flipopCS.NewForConfig(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("building flipop clientset: %w", err)
	}
	return c, nil
}

// Run watches for FloatingIPPools and reconciles their state into reality.
func (c *FloatingIPPoolController) Run(ctx context.Context, ll logrus.FieldLogger) {
	c.ll = ll
	informer := flipopInformers.NewFloatingIPPoolInformer(c.flipopCS, "", floatingIPPoolResyncPeriod, cache.Indexers{})
	informer.AddEventHandler(c)
	c.ctx = ctx
	informer.Run(ctx.Done())
}

// OnAdd implements the shared informer ResourceEventHandler for FloatingIPPools.
func (c *FloatingIPPoolController) OnAdd(obj interface{}) {
	k8sPool, ok := obj.(*flipopv1alpha1.FloatingIPPool)
	if !ok {
		c.ll.WithField("unexpected_type", fmt.Sprintf("%T", obj)).Warn("unexpected type")
	}
	c.updateOrAdd(k8sPool, true)
}

// OnUpdate implements the shared informer ResourceEventHandler for FloatingIPPools.
func (c *FloatingIPPoolController) OnUpdate(_, newObj interface{}) {
	k8sPool, ok := newObj.(*flipopv1alpha1.FloatingIPPool)
	if !ok {
		c.ll.WithField("unexpected_type", fmt.Sprintf("%T", newObj)).Warn("unexpected type")
	}
	c.updateOrAdd(k8sPool, true)
}

func (c *FloatingIPPoolController) updateOrAdd(k8sPool *flipopv1alpha1.FloatingIPPool, fork bool) {
	c.poolLock.Lock()
	defer c.poolLock.Unlock()
	pool, ok := c.pools[k8sPool.GetSelfLink()]
	if !ok {
		pool = &floatingIPPool{
			ll:       c.ll.WithFields(logrus.Fields{"pool": k8sPool.GetSelfLink()}),
			kubeCS:   c.kubeCS,
			flipopCS: c.flipopCS,
		}
		pool.ll.Info("FloatingIPPool added; beginning reconciliation")
		c.pools[k8sPool.GetSelfLink()] = pool
	}
	specChange := !reflect.DeepEqual(pool.k8s, k8sPool)
	pool.k8s = k8sPool.DeepCopy()
	if !specChange {
		return // nothing to do
	}
	if ok {
		pool.ll.Info("FloatingIPPool changed")
		// This blocks while we wait for the current execution to stop. In theory that should
		// happen quickly, but it's possible that it blocks for a while and we're holding the
		// pool lock.
		pool.stop()
	}

	pool.reset(c.ctx)

	prov := c.providers[pool.k8s.Spec.Provider]
	if prov == nil {
		pool.ll.WithFields(logrus.Fields{"provider": pool.k8s.Spec.Provider}).
			Error("FloatingIPPool referenced unknown provider")
		pool.setStatus(pool.ctx, fmt.Sprintf("unknown provider %q", pool.k8s.Spec.Provider))
		return
	}
	pool.provider = prov

	var err error
	pool.nodeSelector = nil

	if pool.k8s.Spec.Match.NodeLabel != "" {
		pool.nodeSelector, err = labels.Parse(pool.k8s.Spec.Match.NodeLabel)
		if err != nil {
			pool.ll.WithError(err).Error("parsing node selector")
			pool.setStatus(pool.ctx, fmt.Sprintf("parsing node selector: %s", err))
			return
		}
	}

	pool.podSelector = nil
	if pool.k8s.Spec.Match.PodLabel != "" {
		pool.podSelector, err = labels.Parse(pool.k8s.Spec.Match.PodLabel)
		if err != nil {
			pool.ll.WithError(err).Error("parsing pod selector")
			pool.setStatus(pool.ctx, fmt.Sprintf("parsing pod selector: %s", err))
			return
		}
	}

	pool.wg.Add(1)
	if fork {
		go pool.run()
	} else { // This option really only exists for testing.
		pool.run()
	}
}

// OnDelete implements the shared informer ResourceEventHandler for FloatingIPPools.
func (c *FloatingIPPoolController) OnDelete(obj interface{}) {
	k8sPool, ok := obj.(*flipopv1alpha1.FloatingIPPool)
	if !ok {
		c.ll.WithField("unexpected_type", fmt.Sprintf("%T", obj)).Warn("unexpected type")
	}
	c.poolLock.Lock()
	defer c.poolLock.Unlock()
	pool, ok := c.pools[k8sPool.GetSelfLink()]
	if !ok {
		return
	}
	pool.stop()
	delete(c.pools, k8sPool.GetSelfLink())
}

type floatingIPPool struct {
	k8s *flipopv1alpha1.FloatingIPPool
	// cache the parsed selectors
	nodeSelector labels.Selector
	podSelector  labels.Selector

	provider provider.Provider

	nodeNameToNode map[string]*node
	ipToNode       map[string]*node

	// TODO(cbaker) - Track last error time and avoid retrying for x period.
	ipToError map[string]string

	// assignableIPs are available for reassignment
	assignableIPs *list.List
	// assignableNodes are matching, but not yet assigned to an IP.
	assignableNodes map[string]*node

	ll       logrus.FieldLogger
	kubeCS   kubernetes.Interface
	flipopCS flipopCS.Interface

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	primed bool

	podIndexer cache.Indexer

	sync.Mutex
}

func (f *floatingIPPool) stop() {
	f.cancel()
	f.wg.Wait()
}

func (f *floatingIPPool) reset(ctx context.Context) {
	f.ctx, f.cancel = context.WithCancel(ctx)
	f.nodeNameToNode = make(map[string]*node)
	f.ipToNode = make(map[string]*node)
	for _, ip := range f.k8s.Spec.IPs {
		f.ipToNode[ip] = nil
	}
	f.ipToError = make(map[string]string)
	f.assignableNodes = make(map[string]*node)
	f.assignableIPs = list.New()
	for _, ip := range f.k8s.Spec.IPs {
		ip := ip
		f.assignableIPs.PushBack(ip)
	}
	f.primed = false
}

func podNodeNameIndexer(obj interface{}) ([]string, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod == nil {
		return nil, errors.New("expected pod type")
	}
	return []string{pod.Spec.NodeName}, nil
}

func (f *floatingIPPool) run() {
	defer f.wg.Done()
	// This does NOT use shared informers which CAN consume more memory and Kubernetes API
	// connections, IF there are other consumers which need the same subscription. Since we filter
	// on labels (and namespace for pod), we would need a shared-informer for each label-set/ns
	// combo, or an unfiltered shared informer. Since it seems likely we're only concerned about a
	// very small subset of pods, it seems better to filter these on the server. If this pattern
	// turns out to be expensive for some use cases, we could add logic/flags to enable better
	// decisions.
	var syncFuncs []cache.InformerSynced
	if f.k8s.Spec.Match.PodNamespace != "" || f.podSelector != nil {
		podInformer := corev1Informers.NewFilteredPodInformer(
			f.kubeCS,
			f.k8s.Spec.Match.PodNamespace,
			podResyncPeriod,
			cache.Indexers{
				podNodeNameIndexerName: podNodeNameIndexer,
			},
			func(opts *v1.ListOptions) {
				if f.podSelector != nil {
					opts.LabelSelector = f.podSelector.String()
				}
			},
		)
		f.podIndexer = podInformer.GetIndexer()
		podInformer.AddEventHandler(f)
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			podInformer.Run(f.ctx.Done())
		}()
		syncFuncs = append(syncFuncs, podInformer.HasSynced)
	} else {
		f.podIndexer = nil
	}

	nodeInformer := corev1Informers.NewFilteredNodeInformer(
		f.kubeCS, nodeResyncPeriod, cache.Indexers{},
		func(opts *v1.ListOptions) {
			if f.nodeSelector != nil {
				opts.LabelSelector = f.nodeSelector.String()
			}
		},
	)
	nodeInformer.AddEventHandler(f)
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		nodeInformer.Run(f.ctx.Done())
	}()
	syncFuncs = append(syncFuncs, nodeInformer.HasSynced)

	if !cache.WaitForCacheSync(f.ctx.Done(), syncFuncs...) {
		if f.ctx.Err() != nil {
			// We don't know why the context was canceled, but this can be a normal error if the
			// FloatingIPPool spec changed during initialization.
			f.ll.WithError(f.ctx.Err()).Error("failed to sync dependencies for FloatingIPPool; maybe spec changed")
		} else {
			f.ll.Error("failed to sync dependencies for FloatingIPPool")
		}
		return
	}

	// After the caches are sync'ed we need to loop through nodes again, otherwise pods which were
	// added before the node was known may be missing.
	f.Lock()
	defer f.Unlock()
	for _, o := range nodeInformer.GetStore().List() {
		k8sNode, ok := o.(*corev1.Node)
		if !ok {
			f.ll.Error("node informer store produced non-node")
			continue
		}
		err := f.updateNode(f.ctx, k8sNode)
		if err != nil {
			f.ll.WithError(err).Error("updating node")
		}
	}
	f.primed = true
	f.assign(f.ctx)
	f.ll.Info("FloatingIPPool synchronized")
	f.setStatus(f.ctx, "")
}

func (f *floatingIPPool) getNodePods(nodeName string) ([]*corev1.Pod, error) {
	var out []*corev1.Pod
	indexer := f.podIndexer
	items, err := indexer.ByIndex(podNodeNameIndexerName, nodeName)
	if err != nil {
		return nil, fmt.Errorf("retrieving pods: %w", err)
	}
	for _, o := range items {
		pod, ok := o.(*corev1.Pod)
		if !ok {
			return nil, fmt.Errorf("pod indexer return non-pod type %T", o)
		}
		out = append(out, pod)
	}
	return out, nil
}

func (f *floatingIPPool) assign(ctx context.Context) error {
	for _, n := range f.assignableNodes {
		if f.assignableIPs.Len() == 0 {
			return nil
		}
		ll := f.ll.WithField("node", n.getName())
		if n.ip != "" {
			// This node is assignable and by definition doesn't have an IP, if we're here the
			// program is broken.
			ll.WithField("node_ip", n.ip).Fatal("assignable node already has IP")
		}
		e := f.assignableIPs.Front()
		ip := e.Value.(string)
		ll = ll.WithField("ip", ip)
		ll.Info("assigning ip to node")
		err := f.provider.AssignIP(ctx, ip, n.getProviderID())
		if err != nil {
			// This error might be with the node (ex. already has an IP or a pending action)
			ll.Error(err, "assigning ip to node")
			return err
		}
		n.ip = ip
		f.assignableIPs.Remove(e)
		delete(f.assignableNodes, n.getName())
		f.ipToNode[ip] = n
	}
	return nil
}

func (f *floatingIPPool) deleteNode(k8s *corev1.Node) {
	n, ok := f.nodeNameToNode[k8s.Name]
	if !ok {
		return
	}
	ip := n.ip
	f.releaseNode(n)
	delete(f.nodeNameToNode, n.getName())
	delete(f.ipToNode, ip)
	return
}

func (f *floatingIPPool) updateNode(ctx context.Context, k8s *corev1.Node) error {
	// TODO - should this return an error?
	if !k8s.ObjectMeta.DeletionTimestamp.IsZero() {
		f.deleteNode(k8s)
		return nil
	}
	providerID := k8s.Spec.ProviderID
	ll := f.ll.WithFields(logrus.Fields{"node": k8s.Name, "node_provider_id": providerID})
	n, ok := f.nodeNameToNode[k8s.Name]
	if !ok {
		if providerID == "" {
			ll.Info("node has no provider id, ignoring")
			return nil
		}
		n = newNode(k8s)
		f.nodeNameToNode[n.getName()] = n
		ip, err := f.provider.NodeToIP(ctx, providerID)
		if err != nil {
			return err
		}
		if ip != "" {
			delete(f.ipToError, ip)
			oldNode, isIPKnown := f.ipToNode[ip]
			if isIPKnown {
				if oldNode != nil {
					// Only should happen w/ OOB API edits, nevertheless, keep our accounting clean
					oldNode.ip = ""
				}
				f.ipToNode[ip] = n
				n.ip = ip
			}
		}
	} else {
		n.k8s = k8s
	}

	if n.ip != "" {
		delete(f.ipToError, n.ip)
	}

	var oldNodeMatch = n.isNodeMatch
	n.isNodeMatch = f.isNodeMatch(n)

	if n.isNodeMatch && len(n.matchingPods) > 0 {
		// We stop tracking pods when the node doesn't match.
		n.matchingPods = make(map[string]*corev1.Pod)
	}

	if oldNodeMatch == n.isNodeMatch {
		ll.Debug("node match unchanged")
		return nil
	}

	if n.isNodeMatch {
		if f.k8s.Spec.Match.PodNamespace != "" || f.podSelector != nil {
			podList, err := f.getNodePods(n.getName())
			if err != nil {
				return fmt.Errorf("listing node pods: %w", err)
			}
			for _, pod := range podList {
				f.updatePod(pod)
			}
			return nil // updatePod will setNodeAssignable if appropriate
		}
		f.setNodeAssignable(n)
	} else {
		f.releaseNode(n)
	}
	return nil
}

func (f *floatingIPPool) updatePod(pod *corev1.Pod) error {
	ll := f.ll.WithFields(logrus.Fields{"pod": pod.Name, "pod_namespace": pod.Namespace})
	if pod.Spec.NodeName == "" {
		// This pod hasn't been assigned to a node. Once a pod is assigned to a node, it cannot be
		// unassigned.
		ll.Debug("ignoring unscheduled pod")
		return nil
	}
	if !pod.ObjectMeta.DeletionTimestamp.IsZero() {
		f.deletePod(pod)
		return nil
	}
	ll = ll.WithField("node", pod.Spec.NodeName)
	n, ok := f.nodeNameToNode[pod.Spec.NodeName]
	if !ok {
		// We don't know about this node.  If primed, we should, otherwise we'll catch it
		// when the node is added.
		if f.primed {
			ll.Info("pod referenced unknown node")
		}
		return nil
	}

	if !n.isNodeMatch {
		ll.Debug("ignoring pod on unmatching node")
		return nil
	}
	// Pods spec & metadata (labels+namespace) are immutable. If it doesn't match now it never did.
	if f.k8s.Spec.Match.PodNamespace != "" && pod.Namespace != f.k8s.Spec.Match.PodNamespace {
		// This is a warning because the informer should only deliver pods in the specified namespace.
		ll.Warn("unexpected pod namespace")
		return nil
	}
	if f.podSelector != nil && !f.podSelector.Matches(labels.Set(pod.Labels)) {
		// This is a warning because pod labels should be immutable, and the informer should only
		// give us matching pods.
		ll.Warn("pod labels did not match; informer should not have delivered")
		return nil
	}

	podKey := podNamespacedName(pod)
	_, active := n.matchingPods[podKey]

	running := pod.Status.Phase == corev1.PodRunning
	var ready bool
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			ready = (cond.Status == corev1.ConditionTrue)
		}
	}
	ll = ll.WithFields(logrus.Fields{"pod_ready": ready, "pod_phase": pod.Status.Phase})
	if (ready && running) == active {
		ll.Debug("pod matching state unchanged")
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
	return nil
}

func (f *floatingIPPool) deletePod(pod *corev1.Pod) {
	if pod.Spec.NodeName == "" {
		return
	}
	n, ok := f.nodeNameToNode[pod.Spec.NodeName]
	if !ok {
		return
	}
	delete(n.matchingPods, podNamespacedName(pod))
	if len(n.matchingPods) == 0 {
		f.releaseNode(n)
	}
}

func (f *floatingIPPool) setNodeAssignable(n *node) {
	ll := f.ll.WithField("node", n.getName())
	if n.ip != "" {
		// The node still has an IP assigned, we just need to remove it from the assignable pool.
		for e := f.assignableIPs.Front(); e != nil; e = e.Next() {
			ip := e.Value.(string)
			if n.ip == ip {
				f.assignableIPs.Remove(e)
				ll.WithField("ip", ip).Info("assignable node already has IP, removing from pool")
				return
			}
		}
		// If we're here, the program is broken. The node record was in the non-matching state,
		// held an old IP. That IP should be listed as available, but isn't.
		ll.WithField("node_ip", n.ip).Fatal("assignable node claims IP, but IP is not assignable.")
	}
	ll.Info("set node assignable")
	f.assignableNodes[n.getName()] = n
}

func (f *floatingIPPool) releaseNode(n *node) {
	f.ll.WithField("node", n.getName()).Info("node no longer matching, releasing")
	if n.ip == "" {
		delete(f.assignableNodes, n.getName())
		return
	}
	ip := n.ip
	f.assignableIPs.PushBack(ip)
}

func (f *floatingIPPool) setStatus(ctx context.Context, errMsg string) error {
	status := flipopv1alpha1.FloatingIPPoolStatus{
		IPs: make(map[string]flipopv1alpha1.IPStatus),
	}
	for ip, n := range f.ipToNode {
		ipStatus := &flipopv1alpha1.IPStatus{
			Error: f.ipToError[ip],
		}
		if n != nil {
			ipStatus.NodeName = n.getName()
			ipStatus.ProviderID = n.getProviderID()
			for _, pod := range n.matchingPods {
				ipStatus.Targets = append(ipStatus.Targets, flipopv1alpha1.Target{
					APIVersion: pod.APIVersion,
					Kind:       pod.Kind,
					Name:       pod.Name,
					Namespace:  pod.Namespace,
				})
			}
		}
		status.IPs[ip] = *ipStatus
	}
	status.Error = errMsg
	f.k8s.Status = status
	// We discard the returned status. If we updated here, we might also get spec changes which
	// have not yet been processed via FloatingIPPoolController's updateOrAdd(), and therefore
	// have not yet been reconciled.
	_, err := f.flipopCS.FlipopV1alpha1().FloatingIPPools(f.k8s.Namespace).UpdateStatus(f.k8s)
	if err != nil {
		f.ll.WithError(err).Error("failed to update status")
	}
	return err
}

func (f *floatingIPPool) isNodeMatch(n *node) bool {
	var ready bool
	for _, c := range n.k8s.Status.Conditions {
		if c.Type == corev1.NodeReady {
			ready = (c.Status == corev1.ConditionTrue)
		}
	}
	if !ready {
		return false
	}

	if f.nodeSelector != nil && !f.nodeSelector.Matches(labels.Set(n.k8s.Labels)) {
		return false
	}

taintLoop:
	for _, taint := range n.k8s.Spec.Taints {
		for _, tol := range f.k8s.Spec.Match.Tolerations {
			if tol.ToleratesTaint(&taint) {
				continue taintLoop
			}
		}
		return false
	}
	return true
}

// OnAdd implements the shared informer ResourceEventHandler for corev1.Pod & corev1.Node.
func (f *floatingIPPool) OnAdd(obj interface{}) {
	f.OnUpdate(nil, obj)
}

// OnUpdate implements the shared informer ResourceEventHandler for corev1.Pod & corev1.Node.
func (f *floatingIPPool) OnUpdate(_, newObj interface{}) {
	f.Lock()
	defer f.Unlock()
	switch r := newObj.(type) {
	case *corev1.Node:
		f.updateNode(f.ctx, r)
	case *corev1.Pod:
		f.updatePod(r)
	default:
		f.ll.Errorf("informer emitted unexpected type: %T", newObj)
	}
	if f.primed {
		// We only assign once we've seen all nodes/pods because we want to avoid unnecessary
		// churn caused by assigning IPs already in use.
		err := f.assign(f.ctx)
		if err != nil {
			f.ll.WithError(err).Error("failed to assign ip to node")
		}
	}
}

// OnDelete implements the shared informer ResourceEventHandler for corev1.Pod & corev1.Node.
func (f *floatingIPPool) OnDelete(obj interface{}) {
	f.Lock()
	defer f.Unlock()
	switch r := obj.(type) {
	case *corev1.Node:
		f.deleteNode(r)
	case *corev1.Pod:
		f.deletePod(r)
	default:
		f.ll.Errorf("informer emitted unexpected type: %T", obj)
	}
	if f.primed {
		err := f.assign(f.ctx)
		if err != nil {
			f.ll.WithError(err).Error("failed to assign ip to node")
		}
	}
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
