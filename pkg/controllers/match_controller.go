package controllers

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	flipopv1alpha1 "github.com/jcodybaker/flipop/pkg/apis/flipop/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	corev1Informers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	floatingIPPoolResyncPeriod = 5 * time.Minute
	podResyncPeriod            = 5 * time.Minute
	nodeResyncPeriod           = 5 * time.Minute
	podNodeNameIndexerName     = "podNodeName"
)

type nodeEnableDisabler interface {
	EnableNode(*corev1.Node)
	DisableNode(*corev1.Node)
}

type matchController struct {
	match *flipopv1alpha1.Match
	// cache the parsed selectors
	nodeSelector labels.Selector
	podSelector  labels.Selector

	nodeNameToNode map[string]*node

	ll           logrus.FieldLogger
	kubeCS       kubernetes.Interface
	nodeInformer cache.SharedIndexInformer

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	primed bool

	podIndexer cache.Indexer

	sync.Mutex

	action nodeEnableDisabler
}

func newMatchController(ll logrus.FieldLogger, kubeCS kubernetes.Interface, action nodeEnableDisabler) *matchController {
	m := &matchController{
		ll:     ll,
		kubeCS: kubeCS,
		action: action,
	}
	m.reset()
	return m
}

func (m *matchController) start(ctx context.Context) {
	if m.cancel != nil {
		return // already running.
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.wg.Add(1)
	go m.run()
}

func (m *matchController) stop() {
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	m.wg.Wait()
}

func (m *matchController) reset() {
	m.nodeNameToNode = make(map[string]*node)
	m.primed = false
	m.nodeInformer = nil
}

// updateCriteria sets the match criteria based on the match spec. If the criteria has changed
// any current reconciliation is stopped, and the match criteria are updated, and we return true.
// If the criteria are unchanged, execution will continue, and false is returned.
func (m *matchController) updateCriteria(match *flipopv1alpha1.Match) bool {
	if m.match != nil && reflect.DeepEqual(match, m.match) {
		return false
	}
	m.stop()
	m.reset()
	m.match = match.DeepCopy()

	var err error
	m.nodeSelector = nil
	if m.match.NodeLabel != "" {
		m.nodeSelector, err = labels.Parse(m.match.NodeLabel)
		if err != nil { // This shouldn't happen if the caller used validateMatch
			m.ll.WithError(err).Error("parsing node selector")
			m.match = nil
			return false
		}
	}

	m.podSelector = nil
	if m.match.PodLabel != "" {
		m.podSelector, err = labels.Parse(m.match.PodLabel)
		if err != nil {
			m.ll.WithError(err).Error("parsing pod selector")
			m.match = nil
			return false
		}
	}
	m.ll.Info("match criteria updated")

	return true
}

func podNodeNameIndexer(obj interface{}) ([]string, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod == nil {
		return nil, errors.New("expected pod type")
	}
	return []string{pod.Spec.NodeName}, nil
}

func (m *matchController) run() {
	defer m.wg.Done()
	if m.match == nil {
		// The only way this should happen is if updateK8s was never called, or a match criteria
		// passed validation with validateMatch, but then failed updateK8s.
		m.ll.Warn("no match criteria set; cannot reconcile")
		return
	}
	// This does NOT use shared informers which CAN consume more memory and Kubernetes API
	// connections, IF there are other consumers which need the same subscription. Since we filter
	// on labels (and namespace for pod), we would need a shared-informer for each label-set/ns
	// combo, or an unfiltered shared informer. Since it seems likely we're only concerned about a
	// very small subset of pods, it seems better to filter these on the server. If this pattern
	// turns out to be expensive for some use cases, we could add logic/flags to enable better
	// decisions.
	var syncFuncs []cache.InformerSynced
	if m.match.PodNamespace != "" || m.podSelector != nil {
		podInformer := corev1Informers.NewFilteredPodInformer(
			m.kubeCS,
			m.match.PodNamespace,
			podResyncPeriod,
			cache.Indexers{
				podNodeNameIndexerName: podNodeNameIndexer,
			},
			func(opts *v1.ListOptions) {
				if m.podSelector != nil {
					opts.LabelSelector = m.podSelector.String()
				}
			},
		)
		m.podIndexer = podInformer.GetIndexer()
		podInformer.AddEventHandler(m)
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			podInformer.Run(m.ctx.Done())
		}()
		syncFuncs = append(syncFuncs, podInformer.HasSynced)
	} else {
		m.podIndexer = nil
	}

	m.nodeInformer = corev1Informers.NewFilteredNodeInformer(
		m.kubeCS, nodeResyncPeriod, cache.Indexers{},
		func(opts *v1.ListOptions) {
			if m.nodeSelector != nil {
				opts.LabelSelector = m.nodeSelector.String()
			}
		},
	)
	m.nodeInformer.AddEventHandler(m)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.nodeInformer.Run(m.ctx.Done())
	}()
	syncFuncs = append(syncFuncs, m.nodeInformer.HasSynced)

	if !cache.WaitForCacheSync(m.ctx.Done(), syncFuncs...) {
		if m.ctx.Err() != nil {
			// We don't know why the context was canceled, but this can be a normal error if the
			// FloatingIPPool spec changed during initialization.
			m.ll.WithError(m.ctx.Err()).Error("failed to sync dependencies; maybe spec changed")
		} else {
			m.ll.Error("failed to sync dependencies")
		}
		return
	}
	m.Lock()
	m.primed = true
	m.Unlock()
	// After the caches are sync'ed we need to loop through nodes again, otherwise pods which were
	// added before the node was known may be missing.
	m.resync()
}

func (m *matchController) resync() {
	m.Lock()
	defer m.Unlock()
	if !m.primed {
		return // initial sync is still pending
	}
	for _, o := range m.nodeInformer.GetStore().List() {
		k8sNode, ok := o.(*corev1.Node)
		if !ok {
			m.ll.Error("node informer store produced non-node")
			continue
		}
		err := m.updateNode(m.ctx, k8sNode)
		if err != nil {
			m.ll.WithError(err).Error("updating node")
		}
	}
	m.ll.Info("synchronized")
}

func (m *matchController) getNodePods(nodeName string) ([]*corev1.Pod, error) {
	var out []*corev1.Pod
	indexer := m.podIndexer
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

func (m *matchController) deleteNode(k8sNode *corev1.Node) {
	m.action.DisableNode(k8sNode)
	delete(m.nodeNameToNode, k8sNode.Name)
	return
}

func (m *matchController) updateNode(ctx context.Context, k8sNode *corev1.Node) error {
	if !k8sNode.ObjectMeta.DeletionTimestamp.IsZero() {
		m.deleteNode(k8sNode)
		return nil
	}
	providerID := k8sNode.Spec.ProviderID
	ll := m.ll.WithFields(logrus.Fields{"node": k8sNode.Name, "node_provider_id": providerID})
	n, ok := m.nodeNameToNode[k8sNode.Name]
	if !ok {
		if providerID == "" {
			ll.Info("node has no provider id, ignoring")
			return nil
		}
		n = newNode(k8sNode)
		m.nodeNameToNode[n.getName()] = n
	} else {
		n.k8sNode = k8sNode
	}

	var oldNodeMatch = n.isNodeMatch
	n.isNodeMatch = m.isNodeMatch(n)

	if oldNodeMatch == n.isNodeMatch {
		ll.Debug("node match unchanged")
		return nil
	}

	if n.isNodeMatch && len(n.matchingPods) > 0 {
		// We stop tracking pods when the node doesn't match.
		n.matchingPods = make(map[string]*corev1.Pod)
	}

	if n.isNodeMatch {
		if m.match.PodNamespace != "" || m.podSelector != nil {
			podList, err := m.getNodePods(n.getName())
			if err != nil {
				return fmt.Errorf("listing node pods: %w", err)
			}
			for _, pod := range podList {
				m.updatePod(pod)
			}
			return nil // updatePod will enable the node if appropriate
		}
		m.action.EnableNode(n.k8sNode)
	} else {
		m.action.DisableNode(n.k8sNode)
	}
	return nil
}

func (m *matchController) updatePod(pod *corev1.Pod) error {
	ll := m.ll.WithFields(logrus.Fields{"pod": pod.Name, "pod_namespace": pod.Namespace})
	if pod.Spec.NodeName == "" {
		// This pod hasn't been assigned to a node. Once a pod is assigned to a node, it cannot be
		// unassigned.
		ll.Debug("ignoring unscheduled pod")
		return nil
	}
	if !pod.ObjectMeta.DeletionTimestamp.IsZero() {
		m.deletePod(pod)
		return nil
	}
	ll = ll.WithField("node", pod.Spec.NodeName)
	n, ok := m.nodeNameToNode[pod.Spec.NodeName]
	if !ok {
		// We don't know about this node.  If primed, we should, otherwise we'll catch it
		// when the node is added.
		if m.primed {
			ll.Info("pod referenced unknown node")
		}
		return nil
	}

	if !n.isNodeMatch {
		ll.Debug("ignoring pod on unmatching node")
		return nil
	}
	// Pods spec & metadata (labels+namespace) are immutable. If it doesn't match now it never did.
	if m.match.PodNamespace != "" && pod.Namespace != m.match.PodNamespace {
		// This is a warning because the informer should only deliver pods in the specified namespace.
		ll.Warn("unexpected pod namespace")
		return nil
	}
	if m.podSelector != nil && !m.podSelector.Matches(labels.Set(pod.Labels)) {
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
			m.action.EnableNode(n.k8sNode)
		}
	} else {
		delete(n.matchingPods, podKey)
		if len(n.matchingPods) == 0 {
			m.action.DisableNode(n.k8sNode)
		}
	}
	return nil
}

func (m *matchController) deletePod(pod *corev1.Pod) {
	if pod.Spec.NodeName == "" {
		return
	}
	n, ok := m.nodeNameToNode[pod.Spec.NodeName]
	if !ok {
		return
	}
	podKey := podNamespacedName(pod)
	delete(n.matchingPods, podKey)
	if len(n.matchingPods) == 0 {
		m.action.DisableNode(n.k8sNode)
	}
}

func (m *matchController) setStatus(ctx context.Context, errMsg string) error {
	// status := flipopv1alpha1.FloatingIPPoolStatus{
	// 	IPs: make(map[string]flipopv1alpha1.IPStatus),
	// }
	// for ip, n := range m.ipToNode {
	// 	ipStatus := &flipopv1alpha1.IPStatus{
	// 		Error: m.ipToError[ip],
	// 	}
	// 	if n != nil {
	// 		ipStatus.NodeName = n.getName()
	// 		ipStatus.ProviderID = n.getProviderID()
	// 		for _, pod := range n.matchingPods {
	// 			ipStatus.Targets = append(ipStatus.Targets, flipopv1alpha1.Target{
	// 				APIVersion: pod.APIVersion,
	// 				Kind:       pod.Kind,
	// 				Name:       pod.Name,
	// 				Namespace:  pod.Namespace,
	// 			})
	// 		}
	// 	}
	// 	status.IPs[ip] = *ipStatus
	// }
	// status.Error = errMsg
	// m.k8s.Status = status
	// // We discard the returned status. If we updated here, we might also get spec changes which
	// // have not yet been processed via FloatingIPPoolController's updateOrAdd(), and therefore
	// // have not yet been reconciled.
	// _, err := m.flipopCS.FlipopV1alpha1().FloatingIPPools(m.k8s.Namespace).UpdateStatus(m.k8s)
	// if err != nil {
	// 	m.ll.WithError(err).WithField("namespace", m.k8s.Namespace).Error("failed to update status")
	// }
	// return err
	return nil
}

func (m *matchController) isNodeMatch(n *node) bool {
	var ready bool
	for _, c := range n.k8sNode.Status.Conditions {
		if c.Type == corev1.NodeReady {
			ready = (c.Status == corev1.ConditionTrue)
		}
	}
	if !ready {
		return false
	}

	if m.nodeSelector != nil && !m.nodeSelector.Matches(labels.Set(n.k8sNode.Labels)) {
		return false
	}

taintLoop:
	for _, taint := range n.k8sNode.Spec.Taints {
		for _, tol := range m.match.Tolerations {
			if tol.ToleratesTaint(&taint) {
				continue taintLoop
			}
		}
		return false
	}
	return true
}

// OnAdd implements the shared informer ResourceEventHandler for corev1.Pod & corev1.Node.
func (m *matchController) OnAdd(obj interface{}) {
	m.OnUpdate(nil, obj)
}

// OnUpdate implements the shared informer ResourceEventHandler for corev1.Pod & corev1.Node.
func (m *matchController) OnUpdate(_, newObj interface{}) {
	m.Lock()
	defer m.Unlock()
	switch r := newObj.(type) {
	case *corev1.Node:
		m.updateNode(m.ctx, r)
	case *corev1.Pod:
		m.updatePod(r)
	default:
		m.ll.Errorf("informer emitted unexpected type: %T", newObj)
	}
}

// OnDelete implements the shared informer ResourceEventHandler for corev1.Pod & corev1.Node.
func (m *matchController) OnDelete(obj interface{}) {
	m.Lock()
	defer m.Unlock()
	switch r := obj.(type) {
	case *corev1.Node:
		m.deleteNode(r)
	case *corev1.Pod:
		m.deletePod(r)
	default:
		m.ll.Errorf("informer emitted unexpected type: %T", obj)
	}
}

type node struct {
	k8sNode      *corev1.Node
	isNodeMatch  bool
	matchingPods map[string]*corev1.Pod
}

func newNode(k8sNode *corev1.Node) *node {
	return &node{
		k8sNode:      k8sNode.DeepCopy(),
		matchingPods: make(map[string]*corev1.Pod),
	}
}

func (n *node) getName() string {
	return n.k8sNode.Name
}

func (n *node) getProviderID() string {
	return n.k8sNode.Spec.ProviderID
}

func podNamespacedName(pod *corev1.Pod) string {
	return fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
}

func validateMatch(match *flipopv1alpha1.Match) error {
	if match.NodeLabel != "" {
		_, err := labels.Parse(match.NodeLabel)
		if err != nil {
			return fmt.Errorf("parsing node selector: %w", err)
		}
	}

	if match.PodLabel != "" {
		_, err := labels.Parse(match.PodLabel)
		if err != nil {
			return fmt.Errorf("parsing pod selector: %w", err)
		}
	}
	return nil
}
