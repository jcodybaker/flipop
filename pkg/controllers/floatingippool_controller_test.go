package controllers

import (
	"context"
	"fmt"
	"testing"
	"time"

	flipopv1alpha1 "github.com/jcodybaker/flipop/pkg/apis/flipop/v1alpha1"
	"github.com/jcodybaker/flipop/pkg/provider"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"

	flipCSFake "github.com/jcodybaker/flipop/pkg/apis/flipop/generated/clientset/versioned/fake"

	kubeCSFake "k8s.io/client-go/kubernetes/fake"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func TestFloatingIPPoolUpdateK8s(t *testing.T) {
	tcs := []struct {
		name                  string
		objs                  []metav1.Object
		manip                 func(*flipopv1alpha1.FloatingIPPool)
		initialIPAssignment   map[string]string
		expectAssignedIPs     int // just a count because node assignment is non-deterministic
		expectAssignableIPs   int
		expectAssignableNodes int
		expectIPAssignment    map[string]string // expect a specific node to have a specific ip
		expectPrimed          bool
		eval                  func(t *testing.T, f *floatingIPPool)
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
			expectPrimed:        true,
		},
		{
			name: "already has ip",
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
			initialIPAssignment: map[string]string{
				"172.16.2.2": "mock://1",
			},
			expectIPAssignment: map[string]string{
				"172.16.2.2": "mock://1",
			},
			expectAssignedIPs:   1,
			expectAssignableIPs: 1,
			expectPrimed:        true,
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
			expectPrimed:        true,
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
			expectPrimed:        true,
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
			expectPrimed:          true,
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

			c := &FloatingIPPoolController{
				kubeCS:   kubeCSFake.NewSimpleClientset(asRuntimeObjects(tc.objs)...),
				flipopCS: flipCSFake.NewSimpleClientset(k8s),
				providers: map[string]provider.Provider{
					"mock": &provider.MockProvider{
						NodeToIPFunc: func(ctx context.Context, providerID string) (string, error) {
							for ip, pID := range ipAssignment {
								if providerID == pID {
									return ip, nil
								}
							}
							return "", nil
						},
						AssignIPFunc: func(ctx context.Context, ip, provider string) error {
							ipAssignment[ip] = provider
							return nil
						},
					},
				},
				pools: make(map[string]*floatingIPPool),
				ctx:   ctx,
				ll:    logrus.New(),
			}
			c.updateOrAdd(k8s, false)

			f, ok := c.pools[k8s.GetSelfLink()]
			require.True(t, ok)
			require.NotNil(t, f.k8s)
			require.Empty(t, f.k8s.Status.Error)
			require.Equal(t, tc.expectPrimed, f.primed)

			cancel()
			f.wg.Wait()

			require.Len(t, ipAssignment, tc.expectAssignedIPs)
			require.Equal(t, tc.expectAssignableIPs, f.assignableIPs.Len())

			require.Len(t, f.assignableNodes, tc.expectAssignableNodes)
		})
	}
}

func TestUpdateNode(t *testing.T) {
	// NOTE - This also gets exercised in updateK8s
	tcs := []struct {
		name                  string
		initialIPAssignment   map[string]string
		updates               []metav1.Object
		expectAssignableNodes []string
		expectIPs             map[string]string
		expectAssignableIPs   []string
	}{
		{
			name: "initial update ready",
			updates: []metav1.Object{
				makePod("benjamin-sisko", "rio-grande",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
			},
			expectAssignableNodes: []string{"rio-grande"},
			expectAssignableIPs:   []string{"192.168.1.1", "172.16.2.2"},
		},
		{
			name: "initial update not-ready",
			updates: []metav1.Object{
				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
			},
			expectAssignableNodes: []string{},
			expectAssignableIPs:   []string{"192.168.1.1", "172.16.2.2"},
		},
		{
			name: "update from not-ready to ready",
			updates: []metav1.Object{
				makePod("benjamin-sisko", "rio-grande",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
				makePod("worf", "orinoco",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
				makeNode("rio-grande", "mock://1", setLabels(matchingNodeLabels)), // not yet ready
				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
			},
			expectAssignableNodes: []string{}, // empty because the node already has an IP.
			initialIPAssignment: map[string]string{ // Mark the IP as already attached
				"mock://1": "172.16.2.2",
			},
			expectIPs: map[string]string{
				"rio-grande": "172.16.2.2",
			},
			expectAssignableIPs: []string{"192.168.1.1"},
		},
		{
			name: "update from ready to not-ready",
			updates: []metav1.Object{
				makePod("benjamin-sisko", "rio-grande",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
				makePod("worf", "orinoco",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
				makeNode("rio-grande", "mock://1", setLabels(matchingNodeLabels)), // not ready
			},
			expectAssignableNodes: []string{},
			initialIPAssignment: map[string]string{ // Mark the IP as already attached
				"mock://1": "172.16.2.2",
			},
			// The rio-grande node record should still know that the IP is associated, but it should
			// also be available for assignment if another matching node is available.
			expectIPs: map[string]string{
				"rio-grande": "172.16.2.2",
			},
			expectAssignableIPs: []string{"192.168.1.1", "172.16.2.2"},
		},
	}
	for _, tc := range tcs {
		tc := tc

		ctx := context.Background()

		t.Run(tc.name, func(t *testing.T) {
			f := &floatingIPPool{
				kubeCS: kubeCSFake.NewSimpleClientset(),
				ll:     logrus.New(),
			}
			k8s := makeFloatingIPPool()
			f.reset(ctx)
			f.k8s = k8s
			for _, o := range asRuntimeObjects(tc.updates) {
				err := f.client.Create(ctx, o)
				if apierrors.IsAlreadyExists(err) {
					err = f.client.Update(ctx, o)
				}
				require.NoError(t, err)
				if n, ok := o.(*corev1.Node); ok {
					err := f.updateNode(ctx, n)
					require.NoError(t, err)
				}
			}
			var assignableIPs, assignableNodes []string
			for e := f.assignableIPs.Front(); e != nil; e = e.Next() {
				assignableIPs = append(assignableIPs, e.Value.(string))
			}
			for _, n := range f.assignableNodes {
				assignableNodes = append(assignableNodes, n.getName())
			}
			for name, ip := range tc.expectIPs {
				n, ok := f.nodeNameToNode[name]
				require.Truef(t, ok, "node %q does not have ip %q", name, ip)
				require.Equal(t, ip, n.ip)
			}
			for name, n := range f.nodeNameToNode {
				if _, ok := f.nodeNameToNode[name]; ok {
					continue
				}
				require.Equal(t, "", n.ip)
			}
			require.ElementsMatch(t, tc.expectAssignableNodes, assignableNodes)
			require.ElementsMatch(t, tc.expectAssignableIPs, assignableIPs)
		})
	}
}

// func TestUpdatePod(t *testing.T) {
// 	// NOTE - This also gets exercised in updateK8s and updateNode
// 	tcs := []struct {
// 		name                  string
// 		initialIPAssignment            map[string]string
// 		updates               []metav1.Object
// 		expectAssignableNodes []string
// 		expectIPs             map[string]string
// 		expectAssignableIPs   []string
// 	}{
// 		{
// 			name: "pod makes node assignable",
// 			updates: []metav1.Object{
// 				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
// 				makePod("benjamin-sisko", "rio-grande",
// 					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
// 			},
// 			expectAssignableNodes: []string{"rio-grande"},
// 			expectAssignableIPs:   []string{"192.168.1.1", "172.16.2.2"},
// 		},
// 		{
// 			name: "pod not-ready causes node to no longer match",
// 			updates: []metav1.Object{
// 				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
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
// 				kubeCS: kubeCSFake.NewSimpleClientset(),
// 				ll:     logrus.New(),
// 			}
// 			providers := map[string]provider.Provider{
// 				"mock": &provider.MockProvider{
// 					NodeToIPFunc: func(ctx context.Context, providerID string) (string, error) {
// 						return tc.initialIPAssignment[providerID], nil
// 					},
// 				},
// 			}
// 			k8s := makeFloatingIPPool()
// 			f.updateK8s(ctx, k8s, providers)
// 			err := f.resync(ctx) // resync w/ no nodes to initialize data structures.
// 			require.NoError(t, err)
// 			for _, o := range asRuntimeObjects(tc.updates) {
// 				err := f.client.Create(ctx, o)
// 				if apierrors.IsAlreadyExists(err) {
// 					err = f.client.Update(ctx, o)
// 				}
// 				require.NoError(t, err)
// 				switch r := o.(type) {
// 				case *corev1.Pod:
// 					err := f.updatePod(r)
// 					require.NoError(t, err)
// 				case *corev1.Node:
// 					err := f.updateNode(ctx, r)
// 					require.NoError(t, err)
// 				default:
// 					t.Fatalf("unexpected resource type: %T", o)
// 				}
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

func makePod(name, nodeName string, manipulations ...func(pod metav1.Object) metav1.Object) metav1.Object {
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
	return p
}

func makeNode(name, providerID string, manipulations ...func(node metav1.Object) metav1.Object) metav1.Object {
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
	return n
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
