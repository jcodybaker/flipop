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
	"fmt"
	"reflect"
	"sync"
	"time"

	flipopv1alpha1 "github.com/jcodybaker/flipop/pkg/apis/flipop/v1alpha1"
	"github.com/jcodybaker/flipop/pkg/provider"
	"github.com/lytics/logrus"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	flipopCS "github.com/jcodybaker/flipop/pkg/apis/flipop/generated/clientset/versioned"
	flipopInformer "github.com/jcodybaker/flipop/pkg/apis/flipop/generated/informers/externalversions"

	// k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	// metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	corev1Informers "k8s.io/client-go/informers/core/v1"
)

const (
	floatingIPPoolResyncPeriod = 5 * time.Minute
	podResyncPeriod            = 5 * time.Minute
	nodeResyncPeriod           = 5 * time.Minute
	podNodeNameIndexerName = "podNodeName"
)

// FloatingIPPoolController watches for FloatingIPPool and then manages reconciliation for each
// pool.
type FloatingIPPoolController struct {
	kubeCS   *kubernetes.Clientset
	flipopCS *flipopClientSet.Clientset

	providers map[string]provider.Provider

	pools    map[string]*floatingIPPool
	poolLock sync.Mutex

	// Fields provided at runtime.
	ll  logrus.FieldLogger
	ctx context.Context
}

// NewFloatingIPPoolController creates a new FloatingIPPoolController.
func NewFloatingIPPoolController(kubeConfig *rest.Config, providers map[string]provider.Provider) (*FloatingIPPoolController, error) {
	c := &FloatingIPPoolController{
		providers: providers,
	}
	c.kubeCS, err = kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return nil, fmt.Errorf("building kubernetes clientset: %w", err)
	}
	c.flipopCS, err = flipopCS.NewForConfig(kubeConfig)
	if err != nil {
		return nil, fmt.Errorf("building flipop clientset: %w", err)
	}

	return c, nil
}

func (c *FloatingIPPoolController) Run(ctx context.Context, ll logrus.FieldLogger) error {
	factory := flipopInformer.NewSharedInformerFactory(c.flipopCS, floatingIPPoolResyncPeriod)
	informer := factory.Flipop().V1alpha1().v1alpha1().FloatingIPPool()
	informer.AddEventHandler(c)
}

func (c *FloatingIPPoolController) OnAdd(obj interface{}) {
	k8sPool, ok := obj.(*flipopv1alpha1.FloatingIPPool)
	if !ok {
		c.ll.WithField("unexpected_type", fmt.Sprintf("%T", obj)).Warn("unexpected type")
	}
	c.onUpdate(k8sPool)
}

func (c *FloatingIPPoolController) OnUpdate(_, newObj interface{}) {
	k8sPool, ok := newObj.(*flipopv1alpha1.FloatingIPPool)
	if !ok {
		c.ll.WithField("unexpected_type", fmt.Sprintf("%T", newObj)).Warn("unexpected type")
	}
	c.onUpdate(k8sPool)
}

func (c *FloatingIPPoolController) updateOrAdd(k8sPool *flipopv1alpha1.FloatingIPPool) {
	c.poolLock.Lock()
	defer c.poolLock.Unlock()
	pool, ok := c.pools[k8sPool.GetSelfLink()]
	if !ok {
		pool = &floatingIPPool{
			ll:       c.ll.WithFields(logrus.Fields{"pool": k8sPool.k8sPool.GetSelfLink()}),
			kubeCS:   c.kubeCS,
			flipopCS: c.flipopCS,
		}
		pool.ll.Info("FloatingIPPool added; beginning reconciliation")
	}
	specChange := !reflect.DeepEqual(pool.k8s, resource)
	pool.k8s = resource.DeepCopy()
	if !specChange {
		return // nothing to do
	}
	if !ok {
		pool.ll.Info("FloatingIPPool changed")
		pool.stop() // This blocks while we wait for the current execution to stop.
	}

	pool.reset()

	prov := c.providers[f.k8s.Spec.Provider]
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

	// TODO - Run
}

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
	delete(c.pools)
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
	kubeCS   *kubernetes.Clientset
	flipopCS *flipopClientSet.Clientset

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	podIndex cache.Indexer
}

func (f *floatingIPPool) stop() {
	f.cancel()
	f.wg.Wait()
}

func (f *floatingIPPool) reset(ctx context.Context) error {
	f.ctx, f.cancel = context.WithCancel()
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
}

func podNodeNameIndexer func(obj interface{}) ([]string, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod == nil {
		return "", errors.New("expected pod type")
	}
	return []string{pod.Spec.NodeName}, nil
}

func (f *floatingIPPool) run() {
	defer wg.Done()
	// This does NOT use shared informers which CAN consume more memory and Kubernetes API
	// connections, IF there are other consumers which need the same subscription. Since we filter
	// on labels (and namespace for pod), we would need a shared-informer for each label-set/ns 
	// combo, or an unfiltered shared informer. Since it seems likely we're only concerned about a
	// very small subset of pods, it seems better to filter these on the server. If this pattern
	// turns out to be expensive for some use cases, we could add logic/flags to enable better
	// decisions.
	var syncFuncs []cache.InformerSynced
	if f.k8s.Spec.Match.PodNamespace != "" || a.podSelector != nil {
		podInformer := corev1Informers.NewFilteredPodInformer(
			f.kubeCS,
			f.k8s.Spec.Match.PodNamespace,
			podResyncPeriod,
			cache.Indexers{
				podNodeNameIndexerName: podNodeNameIndexer,
			},
			func(opts *v1.ListOptions) {
				if f.podSelector != nil {
					opts.LabelSelector = a.podSelector.String()
				}
			},
		)
		f.podIndexer = podInformer.GetIndexer()
		podInformer.AddEventHandler(f)
		wg.Add(1)
		go func() {
			defer wg.Done()
			podInformer.Run(f.ctx.Done())
		}
		syncFuncs = append(syncFuncs, podInformer.HasSynced)	
	} else {
		f.podIndexer = nil
	}
	
	nodeInformer := corev1Informers.NewFilteredPodInformer(
		f.kubeCS, "", nodeResyncPeriod, cache.Indexers{}, 
		func(opts *v1.ListOptions) {
			if f.nodeSelector != nil {
				opts.LabelSelector = a.nodeSelector.String()
			}
		},
	)
	nodeInformer.AddEventHandler(f)
	wg.Add(1)
	go func() {
		defer wg.Done()
		nodeInformer.Run(f.ctx.Done())
	}
	syncFuncs = append(syncFuncs, nodeInformer.HasSynced)

	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		if ctx.Err() != nil {
			// We don't know why the context was canceled, but this can be a normal error if the 
			// FloatingIPPool spec changed during initialization.
			f.ll.WithError(ctx.Err()).Error("failed to sync dependencies for FloatingIPPool; maybe spec changed")
		} else {
			f.ll.Error("failed to sync dependencies for FloatingIPPool")
		}
	}

	// After the caches are sync'ed we need to loop through nodes again, otherwise 
	for _, k8sNode := range nodeList.Items {
		err = f.updateNode(ctx, &k8sNode)
		if err != nil {
			// TODO - Maybe retry here?
			return err
		}
	}

	
}

func (f *floatingIPPool) getNodePods(nodeName string) ([]corev1.Pods, error) {
	var out []*corev1.Pod
	indexer := f.podIndexer
	items, err := indexer.ByIndex(podNodeNameIndexerName, nodeName)
	if err != nil {
		return nil, fmt.Errorf("retrieving pods: %w", err)
	}
	for _, o := range items{
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
		e := f.assignableIPs.Front()
		ip := e.Value.(string)
		err := f.provider.AssignIP(ctx, ip, n.getProviderID())
		if err != nil {
			// This error might be with the node (ex. already has an IP or a pending action)
			f.ll.Error(err, "assigning ip to node", "node", n.getName(), "ip", ip)
			return err
		}
		f.assignableIPs.Remove(e)
		f.ipToNode[ip] = n
	}
	return nil
}

func (f *floatingIPPool) updateNode(ctx context.Context, k8s *corev1.Node) error {
	n, ok := f.nodeNameToNode[k8s.Name]
	if !ok {
		if !k8s.ObjectMeta.DeletionTimestamp.IsZero() {
			return nil
		}
		providerID := k8s.Spec.ProviderID
		if providerID == "" {
			f.ll.Info("node has no provider id, ignoring", "node", k8s.Name)
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

	if !k8s.ObjectMeta.DeletionTimestamp.IsZero() {
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

	if n.isNodeMatch && len(n.matchingPods) > 0 {
		// We stop tracking pods when the node doesn't match.
		n.matchingPods = make(map[string]*corev1.Pod)
	}

	if oldNodeMatch == n.isNodeMatch {
		return nil
	}

	if n.isNodeMatch {
		if f.k8s.Spec.Match.PodNamespace != "" || f.podSelector != nil {
			podList, err := f.getNodePods(n.getName())
			for _, pod := range podList {
				f.updatePod(&pod)
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
	if pod.Spec.NodeName == "" {
		// This pod hasn't been assigned to a node. Once a pod is assigned to a node, it cannot be
		// unassigned.
		return nil
	}
	n, ok := f.nodeNameToNode[pod.Spec.NodeName]
	if !ok {
		// We don't know about the node. When the node event comes in, we'll query all pods.
		f.ll.Info("pod referenced unknown node", "node", pod.Spec.NodeName, "pod", pod.Name, "namespace", pod.Namespace)
		return nil
	}
	// Pods spec & metadata (labels+namespace) are immutable. If it doesn't match now it never did.
	if f.k8s.Spec.Match.PodNamespace != "" && pod.Namespace != f.k8s.Spec.Match.PodNamespace {
		return nil
	}
	if f.podSelector != nil && !f.podSelector.Matches(labels.Set(pod.Labels)) {
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
		running := pod.Status.Phase == corev1.PodRunning
		var ready bool
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodReady {
				ready = (cond.Status == corev1.ConditionTrue)
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
				f.ll.Info("assignable node already has IP, removing from pool", "node", n.getName(), "ip", ip)
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
	ip := n.ip
	f.assignableIPs.PushBack(ip)
}

func (f *floatingIPPool) setStatus(ctx context.Context, errMsg string) error {
	status := flipopv1alpha1.FloatingIPPoolStatus{}
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
	// TODO
	err := f.client.Status().Update(ctx, f.k8s)
	if err != nil {
		f.ll.WithError(err).Error("failed to update status")
	}
	return err
}

func (f *floatingIPPool) isNodeMatch(n *node) (bool, error) {
	var ready bool
	for _, c := range n.k8s.Status.Conditions {
		if c.Type == corev1.NodeReady {
			ready = (c.Status == corev1.ConditionTrue)
		}
	}
	if !ready {
		return false, nil
	}

	if !f.nodeSelector.Matches(labels.Set(n.k8s.Labels)) {
		return false, nil
	}

taintLoop:
	for _, taint := range n.k8s.Spec.Taints {
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
