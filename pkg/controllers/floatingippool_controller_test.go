package controllers

import (
	"context"
	"fmt"
	"testing"
	"time"

	flipopv1alpha1 "github.com/digitalocean/flipop/pkg/apis/flipop/v1alpha1"
	"github.com/digitalocean/flipop/pkg/provider"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"

	flipCSFake "github.com/digitalocean/flipop/pkg/apis/flipop/generated/clientset/versioned/fake"

	kubeCSFake "k8s.io/client-go/kubernetes/fake"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// These tests try to approximate an end-to-end workflow.  The ipController and matchController
// also have their own more comprehensive tests.
func TestFloatingIPPoolUpdateK8s(t *testing.T) {
	tcs := []struct {
		name                  string
		objs                  []metav1.Object
		manip                 func(*flipopv1alpha1.FloatingIPPool)
		initialIPAssignment   map[string]string
		createIPs             []string
		expectError           string
		expectAssignedIPs     int // just a count because node assignment is non-deterministic
		expectAssignableIPs   int
		expectAssignableNodes int
		expectIPAssignment    map[string]string // expect a specific node to have a specific ip
	}{
		{
			name: "happy path",
			objs: []metav1.Object{
				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
				makeNode("ganges", "mock://2"), // should be ignored
				makeNode("orinoco", "mock://3", // should also be ignored because of taint.
					markReady, setLabels(matchingNodeLabels), setTaints(noSchedule)),
				makePod("benjamin-sisko", "rio-grande",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
				makePod("worf", "orinoco",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
			},
			expectAssignedIPs:   1,
			expectAssignableIPs: 1,
		},
		{
			name: "create new ips",
			objs: []metav1.Object{
				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
				makePod("benjamin-sisko", "rio-grande",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
			},
			createIPs: []string{"10.0.1.1", "10.0.2.2"},
			manip: func(f *flipopv1alpha1.FloatingIPPool) {
				f.Spec.IPs = nil
				f.Spec.DesiredIPs = 2
			},
			expectAssignedIPs:   1,
			expectAssignableIPs: 1,
		},
		{
			name: "already has ip",
			objs: []metav1.Object{
				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
				makeNode("ganges", "mock://2"), // should be ignored
				// makeNode("orinoco", "mock://3", // should also be ignored because of taint.
				// 	markReady, setLabels(matchingNodeLabels), setTaints(noSchedule)),
				makePod("benjamin-sisko", "rio-grande",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
				makePod("worf", "orinoco",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
			},
			initialIPAssignment: map[string]string{
				"172.16.2.2": "mock://1",
			},
			expectIPAssignment: map[string]string{
				"172.16.2.2": "mock://1",
			},
			expectAssignedIPs:   1,
			expectAssignableIPs: 1,
		},
		{
			name: "no node selector",
			objs: []metav1.Object{
				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
				makeNode("ganges", "mock://2", markReady), // should be ignored
				makePod("benjamin-sisko", "rio-grande",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
				makePod("worf", "ganges",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
			},
			manip: func(f *flipopv1alpha1.FloatingIPPool) {
				f.Spec.Match.NodeLabel = ""
			},
			expectAssignedIPs: 2,
		},
		{
			name: "bad pod matches",
			objs: []metav1.Object{
				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
				makeNode("ganges", "mock://2", markReady), // should be ignored
				makePod("odo", "rio-grande", // wrong namespace
					markReady, markRunning, setNamespace("bajoran"), setLabels(matchingPodLabels)),
				makePod("jadzia-dax", "rio-grande", // wrong-labels
					markReady, markRunning, setNamespace("star-fleet")),
				makePod("nog", "rio-grande", // not ready
					markRunning, setNamespace("star-fleet")),
				makePod("julian-bashir", "rio-grande", // not running (pending)
					markReady, setNamespace("star-fleet")),
				makePod("miles-obrien", "ganges", // wrong node
					markReady, setNamespace("star-fleet")),
			},
			expectAssignableIPs: 2,
		},
		{
			name: "no pod constraints",
			objs: []metav1.Object{
				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
				makeNode("ganges", "mock://2"), // should be ignored
				makeNode("orinoco", "mock://3",
					markReady, setLabels(matchingNodeLabels), setTaints(noSchedule)),
			},
			manip: func(f *flipopv1alpha1.FloatingIPPool) {
				f.Spec.Match.PodNamespace = ""
				f.Spec.Match.PodLabel = ""
			},
			expectAssignedIPs:   1,
			expectAssignableIPs: 1,
		},
		{
			name: "IP needs to be reassigned",
			objs: []metav1.Object{
				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)), // match
				makeNode("ganges", "mock://2"), // should be ignored - labels don't match
				makeNode("orinoco", "mock://3", // tainted
					markReady, setLabels(matchingNodeLabels), setTaints(noSchedule)),
				makeNode("rubicon", "mock://4", markReady, setLabels(matchingNodeLabels)),    // match
				makeNode("shenandoah", "mock://5", markReady, setLabels(matchingNodeLabels)), // match
			},
			initialIPAssignment: map[string]string{
				"192.168.1.1": "mock://3", // orinoco is tainted
				"172.16.2.2":  "mock://5",
			},
			manip: func(f *flipopv1alpha1.FloatingIPPool) {
				f.Spec.Match.PodNamespace = ""
				f.Spec.Match.PodLabel = ""
			},
			expectIPAssignment: map[string]string{
				// It's non-deterministic if rio-grande or rubicon will get 192.168.1.1, but
				// 172.16.2.2 should stay w/ shenandoah.
				"172.16.2.2": "mock://5",
			},
			expectAssignableNodes: 1, // We have 3 matching nodes, but only 2 ips, one has to wait.
			expectAssignedIPs:     2,
		},
		{
			name: "invalid pod selector",
			objs: []metav1.Object{},
			manip: func(f *flipopv1alpha1.FloatingIPPool) {
				f.Spec.Match.PodLabel = "#invalid#"
			},
			expectError: "Error parsing pod selector: unable to parse requirement: " +
				"invalid label key \"#invalid#\": name part must consist of alphanumeric characters, " +
				"'-', '_' or '.', and must start and end with an alphanumeric character " +
				"(e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')",
		},
		{
			name: "invalid node selector",
			objs: []metav1.Object{},
			manip: func(f *flipopv1alpha1.FloatingIPPool) {
				f.Spec.Match.NodeLabel = "#invalid#"
			},
			expectError: "Error parsing node selector: unable to parse requirement: " +
				"invalid label key \"#invalid#\": name part must consist of alphanumeric characters, " +
				"'-', '_' or '.', and must start and end with an alphanumeric character " +
				"(e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')",
		},
		{
			name: "unknown provider",
			objs: []metav1.Object{},
			manip: func(f *flipopv1alpha1.FloatingIPPool) {
				f.Spec.Provider = "Ferengi"
			},
			expectError: "unknown provider \"Ferengi\"",
		},
		{
			name: "no ips or desired ips",
			objs: []metav1.Object{},
			manip: func(f *flipopv1alpha1.FloatingIPPool) {
				f.Spec.IPs = nil
				f.Spec.DesiredIPs = 0
			},
			expectError: "ips or desiredIPs must be provided",
		},
	}
	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			defer cancel()
			k8s := makeFloatingIPPool()
			if tc.manip != nil {
				tc.manip(k8s)
			}

			ipAssignment := make(map[string]string)
			for ip, providerIP := range tc.initialIPAssignment {
				ipAssignment[ip] = providerIP
			}

			ll := logrus.New()
			ll.SetLevel(logrus.DebugLevel)
			c := &FloatingIPPoolController{
				kubeCS:   kubeCSFake.NewSimpleClientset(asRuntimeObjects(tc.objs)...),
				flipopCS: flipCSFake.NewSimpleClientset(k8s),
				providers: map[string]provider.Provider{
					"mock": &provider.MockProvider{
						IPtoProviderIDFunc: func(ctx context.Context, ip string) (string, error) {
							return ipAssignment[ip], nil
						},
						AssignIPFunc: func(ctx context.Context, ip, providerID string) error {
							ipAssignment[ip] = providerID
							return nil
						},
						CreateIPFunc: func(ctx context.Context, region string) (string, error) {
							require.GreaterOrEqual(t, len(tc.createIPs), 1, "unexpected CreateIP call")
							ip := tc.createIPs[0]
							tc.createIPs = tc.createIPs[1:]
							return ip, nil
						},
					},
				},
				pools: make(map[string]floatingIPPool),
				ctx:   ctx,
				ll:    ll,
			}
			c.updateOrAdd(k8s)
			if tc.expectError != "" {
				updatedK8s, err := c.flipopCS.FlipopV1alpha1().FloatingIPPools(k8s.Namespace).Get(k8s.Name, metav1.GetOptions{})
				require.NoError(t, err)
				require.NotNil(t, updatedK8s)
				require.Equal(t, tc.expectError, updatedK8s.Status.Error)
				return
			}

			f, ok := c.pools[k8s.GetSelfLink()]
			require.True(t, ok)

			f.runToSynchronize(ctx)
			require.NotNil(t, f.matchController.match)
			// require.Empty(t, f.k8s.Status.Error)

			require.Len(t, ipAssignment, tc.expectAssignedIPs)
			require.Equal(t, tc.expectAssignableIPs, f.ipController.assignableIPs.Len())
			require.Equal(t, tc.expectAssignableNodes, f.ipController.assignableNodes.Len())
		})
	}
}

// func TestUpdateNode(t *testing.T) {
// 	// NOTE - This also gets exercised in updateK8s
// 	tcs := []struct {
// 		name                  string
// 		pods                  []*corev1.Pod
// 		initialIPAssignment   map[string]string
// 		updates               []*corev1.Node
// 		manip                 func(*flipopv1alpha1.FloatingIPPool)
// 		expectAssignableNodes []string
// 		expectIPs             map[string]string
// 		expectAssignableIPs   []string
// 	}{
// 		{
// 			name: "initial update ready",
// 			pods: []*corev1.Pod{
// 				makePod("benjamin-sisko", "rio-grande",
// 					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
// 			},
// 			updates: []*corev1.Node{
// 				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
// 			},
// 			expectAssignableNodes: []string{"rio-grande"},
// 			expectAssignableIPs:   []string{"192.168.1.1", "172.16.2.2"},
// 		},
// 		{
// 			name: "initial update not-ready",
// 			pods: []*corev1.Pod{
// 				makePod("benjamin-sisko", "rio-grande",
// 					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
// 			},
// 			updates: []*corev1.Node{
// 				makeNode("rio-grande", "mock://1", setLabels(matchingNodeLabels)),
// 			},
// 			expectAssignableNodes: []string{},
// 			expectAssignableIPs:   []string{"192.168.1.1", "172.16.2.2"},
// 		},
// 		{
// 			name: "initial update no pod match",
// 			updates: []*corev1.Node{
// 				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
// 			},
// 			manip: func(f *flipopv1alpha1.FloatingIPPool) {
// 				f.Spec.Match.PodNamespace = ""
// 				f.Spec.Match.PodLabel = ""
// 			},
// 			expectAssignableNodes: []string{"rio-grande"},
// 			expectAssignableIPs:   []string{"192.168.1.1", "172.16.2.2"},
// 		},
// 		{
// 			name: "update from not-ready to ready",
// 			pods: []*corev1.Pod{
// 				makePod("benjamin-sisko", "rio-grande",
// 					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
// 				makePod("worf", "orinoco",
// 					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
// 			},
// 			updates: []*corev1.Node{
// 				makeNode("rio-grande", "mock://1", setLabels(matchingNodeLabels)), // not yet ready
// 				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
// 			},
// 			expectAssignableNodes: []string{}, // empty because the node already has an IP.
// 			initialIPAssignment: map[string]string{ // Mark the IP as already attached
// 				"mock://1": "172.16.2.2",
// 			},
// 			expectIPs: map[string]string{
// 				"rio-grande": "172.16.2.2",
// 			},
// 			expectAssignableIPs: []string{"192.168.1.1"},
// 		},
// 		{
// 			name: "update from ready to not-ready",
// 			pods: []*corev1.Pod{
// 				makePod("benjamin-sisko", "rio-grande",
// 					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
// 				makePod("worf", "orinoco",
// 					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
// 			},
// 			updates: []*corev1.Node{
// 				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
// 				makeNode("rio-grande", "mock://1", setLabels(matchingNodeLabels)), // not ready
// 			},
// 			expectAssignableNodes: []string{},
// 			initialIPAssignment: map[string]string{ // Mark the IP as already attached
// 				"mock://1": "172.16.2.2",
// 			},
// 			// The rio-grande node record should still know that the IP is associated, but it should
// 			// also be available for assignment if another matching node is available.
// 			expectIPs: map[string]string{
// 				"rio-grande": "172.16.2.2",
// 			},
// 			expectAssignableIPs: []string{"192.168.1.1", "172.16.2.2"},
// 		},
// 	}
// 	for _, tc := range tcs {
// 		tc := tc

// 		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
// 		defer cancel()

// 		t.Run(tc.name, func(t *testing.T) {
// 			f := &floatingIPPool{
// 				ll: logrus.New(),
// 			}
// 			k8s := makeFloatingIPPool()
// 			if tc.manip != nil {
// 				tc.manip(k8s)
// 			}
// 			f.k8s = k8s
// 			var err error
// 			f.reset(ctx)

// 			// These definitions get parsed in updateOrAdd, which we don't run because its tested
// 			// elsewhere and adds a lot of dependencies.
// 			if f.k8s.Spec.Match.NodeLabel != "" {
// 				f.nodeSelector, err = labels.Parse(f.k8s.Spec.Match.NodeLabel)
// 				require.NoError(t, err)
// 			}
// 			if f.k8s.Spec.Match.PodLabel != "" {
// 				f.podSelector, err = labels.Parse(f.k8s.Spec.Match.PodLabel)
// 				require.NoError(t, err)
// 			}

// 			f.provider = &provider.MockProvider{
// 				NodeToIPFunc: func(ctx context.Context, providerID string) (string, error) {
// 					return tc.initialIPAssignment[providerID], nil
// 				},
// 			}

// 			f.podIndexer = cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
// 				podNodeNameIndexerName: podNodeNameIndexer,
// 			})
// 			for _, p := range tc.pods {
// 				f.podIndexer.Add(p)
// 			}

// 			for _, n := range tc.updates {
// 				err := f.updateNode(ctx, n)
// 				require.NoError(t, err)
// 			}

// 			var assignableIPs, assignableNodes []string
// 			for e := f.assignableIPs.Front(); e != nil; e = e.Next() {
// 				assignableIPs = append(assignableIPs, e.Value.(string))
// 			}
// 			for _, n := range f.assignableNodes {
// 				assignableNodes = append(assignableNodes, n.getName())
// 			}
// 			for name, ip := range tc.expectIPs {
// 				n, ok := f.nodeNameToNode[name]
// 				require.Truef(t, ok, "node %q does not have ip %q", name, ip)
// 				require.Equal(t, ip, n.ip)
// 			}
// 			for name, n := range f.nodeNameToNode {
// 				if _, ok := f.nodeNameToNode[name]; ok {
// 					continue
// 				}
// 				require.Equal(t, "", n.ip)
// 			}
// 			require.ElementsMatch(t, tc.expectAssignableNodes, assignableNodes)
// 			require.ElementsMatch(t, tc.expectAssignableIPs, assignableIPs)
// 		})
// 	}
// }

// func TestUpdatePod(t *testing.T) {
// 	// NOTE - This also gets exercised in updateK8s and updateNode
// 	tcs := []struct {
// 		name                  string
// 		initialIPAssignment   map[string]string
// 		updates               []*corev1.Pod
// 		manip                 func(*flipopv1alpha1.FloatingIPPool)
// 		expectAssignableNodes []string
// 		expectIPs             map[string]string
// 		expectAssignableIPs   []string
// 	}{
// 		{
// 			name: "pod makes node assignable",
// 			updates: []*corev1.Pod{
// 				makePod("benjamin-sisko", "rio-grande",
// 					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
// 			},
// 			expectAssignableNodes: []string{"rio-grande"},
// 			expectAssignableIPs:   []string{"192.168.1.1", "172.16.2.2"},
// 		},
// 		{
// 			name: "pod not-ready causes node to no longer match",
// 			updates: []*corev1.Pod{
// 				makePod("benjamin-sisko", "rio-grande",
// 					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
// 				makePod("benjamin-sisko", "rio-grande",
// 					markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
// 			},
// 			expectAssignableNodes: []string{},
// 			initialIPAssignment: map[string]string{ // Mark the IP as already attached
// 				"mock://1": "172.16.2.2",
// 			},
// 			// Node should have IP, but it should be assignable since it doesn't match
// 			expectIPs: map[string]string{
// 				"rio-grande": "172.16.2.2",
// 			},
// 			expectAssignableIPs: []string{"192.168.1.1", "172.16.2.2"},
// 		},
// 	}
// 	for _, tc := range tcs {
// 		tc := tc

// 		ctx := context.Background()

// 		t.Run(tc.name, func(t *testing.T) {
// 			f := &floatingIPPool{
// 				ll: logrus.New(),
// 			}
// 			k8s := makeFloatingIPPool()
// 			if tc.manip != nil {
// 				tc.manip(k8s)
// 			}
// 			f.k8s = k8s
// 			var err error
// 			f.reset(ctx)

// 			// These definitions get parsed in updateOrAdd, which we don't run because its tested
// 			// elsewhere and adds a lot of dependencies.
// 			if f.k8s.Spec.Match.PodLabel != "" {
// 				f.podSelector, err = labels.Parse(f.k8s.Spec.Match.PodLabel)
// 				require.NoError(t, err)
// 			}
// 			f.podIndexer = cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
// 				podNodeNameIndexerName: podNodeNameIndexer, // Necessary for updateNode used in setup
// 			})
// 			f.provider = &provider.MockProvider{
// 				NodeToIPFunc: func(ctx context.Context, providerID string) (string, error) {
// 					return tc.initialIPAssignment[providerID], nil
// 				},
// 			}
// 			err = f.updateNode(ctx, makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)))
// 			require.NoError(t, err)

// 			for _, p := range tc.updates {
// 				f.updatePod(p)
// 			}

// 			var assignableIPs, assignableNodes []string
// 			for e := f.assignableIPs.Front(); e != nil; e = e.Next() {
// 				assignableIPs = append(assignableIPs, e.Value.(string))
// 			}
// 			for _, n := range f.assignableNodes {
// 				assignableNodes = append(assignableNodes, n.getName())
// 			}
// 			for name, ip := range tc.expectIPs {
// 				n, ok := f.nodeNameToNode[name]
// 				require.Truef(t, ok, "node %q does not have ip %q", name, ip)
// 				require.Equal(t, ip, n.ip)
// 			}
// 			for name, n := range f.nodeNameToNode {
// 				if _, ok := f.nodeNameToNode[name]; ok {
// 					continue
// 				}
// 				require.Equal(t, "", n.ip)
// 			}
// 			require.ElementsMatch(t, tc.expectAssignableNodes, assignableNodes)
// 			require.ElementsMatch(t, tc.expectAssignableIPs, assignableIPs)
// 		})
// 	}
// }

// runToSynchronized
func (f *floatingIPPool) runToSynchronize(ctx context.Context) {
	poll := time.NewTicker(200 * time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
			// Wait for the matchController's informers to fully populate their caches.
			if f.matchController.primed {
				// The resync, synchronously applies all cached items. This lets us
				// guarantee that the matchController has had the opportunity to process
				// the resources
				f.matchController.resync()
				f.matchController.stop()
				f.ipController.stop()
				// synchronously run through the ipController reconcile loop.
				f.ipController.reconcile(ctx)
				return
			}
		}
	}
}

var matchingPodLabels = labels.Set(map[string]string{
	"vessel": "runabout",
	"class":  "danube",
})

var matchingNodeLabels = labels.Set(map[string]string{
	"system":   "bajor",
	"quadrant": "alpha",
})

func setLabels(l labels.Set) func(metav1.Object) metav1.Object {
	return func(o metav1.Object) metav1.Object {
		o.SetLabels(l)
		return o
	}
}

var noSchedule = []corev1.Taint{
	corev1.Taint{
		Key:    "node.kubernetes.io/unschedulable",
		Effect: corev1.TaintEffectNoSchedule,
	},
}

func setTaints(t []corev1.Taint) func(metav1.Object) metav1.Object {
	return func(o metav1.Object) metav1.Object {
		n := o.(*corev1.Node)
		n.Spec.Taints = t
		return o
	}
}

func makePod(name, nodeName string, manipulations ...func(pod metav1.Object) metav1.Object) *corev1.Pod {
	var p metav1.Object = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: labels.Set(map[string]string{
				"vessel": "starship",
				"class":  "galaxy",
			}),
			Namespace: "star-fleet",
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{
				corev1.PodCondition{
					Type:   corev1.PodReady,
					Status: corev1.ConditionFalse,
				},
			},
		},
	}
	for _, f := range manipulations {
		p = f(p)
	}
	return p.(*corev1.Pod)
}

func makeNode(name, providerID string, manipulations ...func(node metav1.Object) metav1.Object) *corev1.Node {
	var n metav1.Object = &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: labels.Set(map[string]string{
				"vessel": "starship",
				"class":  "galaxy",
			}),
			Namespace: "star-fleet",
		},
		Spec: corev1.NodeSpec{
			ProviderID: providerID,
		},
		Status: corev1.NodeStatus{
			Phase: corev1.NodePending,
			Conditions: []corev1.NodeCondition{
				corev1.NodeCondition{
					Type:   corev1.NodeReady,
					Status: corev1.ConditionFalse,
				},
			},
		},
	}
	for _, f := range manipulations {
		n = f(n)
	}
	return n.(*corev1.Node)
}

func makeFloatingIPPool() *flipopv1alpha1.FloatingIPPool {
	return &flipopv1alpha1.FloatingIPPool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "deep-space-nine",
		},
		Spec: flipopv1alpha1.FloatingIPPoolSpec{
			Provider: "mock",
			Region:   "alpha-quadrant",
			Match: flipopv1alpha1.Match{
				NodeLabel:    "system=bajor",
				PodNamespace: "star-fleet",
				PodLabel:     "vessel=runabout,class=danube",
				Tolerations: []corev1.Toleration{
					corev1.Toleration{
						Key:      "shields",
						Value:    "down",
						Operator: corev1.TolerationOpEqual,
						Effect:   corev1.TaintEffectNoExecute,
					},
					corev1.Toleration{
						Key:      "alert",
						Value:    "red",
						Operator: corev1.TolerationOpEqual,
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
			},
			IPs: []string{
				"192.168.1.1",
				"172.16.2.2",
			},
		},
	}
}

func markReady(o metav1.Object) metav1.Object {
	switch r := o.(type) {
	case *corev1.Node:
		r.Status.Conditions = []corev1.NodeCondition{
			corev1.NodeCondition{
				Type:   corev1.NodeReady,
				Status: corev1.ConditionTrue,
			},
		}
	case *corev1.Pod:
		r.Status.Conditions = []corev1.PodCondition{
			corev1.PodCondition{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			},
		}
	default:
		panic(fmt.Sprintf("unexpected type: %T", r))
	}
	return o
}

func markRunning(o metav1.Object) metav1.Object {
	pod := o.(*corev1.Pod)
	pod.Status.Phase = corev1.PodRunning
	return pod
}

func markDeleting(o metav1.Object) metav1.Object {
	now := metav1.Now()
	o.SetDeletionTimestamp(&now)
	return o
}

func setNamespace(ns string) func(o metav1.Object) metav1.Object {
	return func(o metav1.Object) metav1.Object {
		o.SetNamespace(ns)
		return o
	}
}

func asRuntimeObjects(in []metav1.Object) (out []runtime.Object) {
	for _, m := range in {
		out = append(out, m.(runtime.Object))
	}
	return out
}
