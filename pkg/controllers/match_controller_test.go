package controllers

import (
	"context"
	"testing"
	"time"

	flipopv1alpha1 "github.com/digitalocean/flipop/pkg/apis/flipop/v1alpha1"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

type mockNodeEnableDisabler struct {
	nodes map[string]*corev1.Node
}

func (mned *mockNodeEnableDisabler) EnableNode(n *corev1.Node) {
	mned.nodes[n.Name] = n
}

func (mned *mockNodeEnableDisabler) DisableNode(n *corev1.Node) {
	delete(mned.nodes, n.Name)
}

func (mned *mockNodeEnableDisabler) names() []string {
	var out []string
	for n := range mned.nodes {
		out = append(out, n)
	}
	return out
}

func TestMatchControllerIsNodeMatch(t *testing.T) {
	tcs := []struct {
		name        string
		node        *corev1.Node
		match       *flipopv1alpha1.Match
		expectMatch bool
	}{
		{
			name: "full match",
			node: makeNode("", "",
				setLabels(matchingNodeLabels),
				setTaints([]corev1.Taint{
					corev1.Taint{Key: "shields", Value: "down", Effect: corev1.TaintEffectNoExecute},
				}),
				markReady,
			),
			match: &flipopv1alpha1.Match{
				NodeLabel:   "system=bajor",
				Tolerations: podTolerations,
			},
			expectMatch: true,
		},
		{
			name: "bad taint",
			node: makeNode("", "",
				setTaints([]corev1.Taint{
					corev1.Taint{Key: "shields", Value: "up", Effect: corev1.TaintEffectNoExecute},
				}),
				markReady,
			),
			match: &flipopv1alpha1.Match{
				Tolerations: podTolerations,
			},
		},
		{
			name: "bad label",
			node: makeNode("", "",
				setLabels(matchingNodeLabels),
				markReady,
			),
			match: &flipopv1alpha1.Match{
				NodeLabel: "system=klingon",
			},
		},
		{
			name:        "all nodes",
			node:        makeNode("", "", markReady),
			match:       &flipopv1alpha1.Match{},
			expectMatch: true,
		},
	}
	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			m := &matchController{ll: logrus.New()}
			m.updateCriteria(tc.match)
			n := &node{
				k8sNode:      tc.node,
				matchingPods: make(map[string]*corev1.Pod),
			}
			require.Equal(t, tc.expectMatch, m.isNodeMatch(n))
		})
	}
}

func TestMatchControllerUpdateNode(t *testing.T) {
	tcs := []struct {
		name                string
		pods                []*corev1.Pod
		initialIPAssignment map[string]string
		updates             []*corev1.Node
		manip               func(*flipopv1alpha1.Match)
		expectEnabledNodes  []string
	}{
		{
			name: "initial update ready",
			pods: []*corev1.Pod{
				makePod("benjamin-sisko", "rio-grande",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
			},
			updates: []*corev1.Node{
				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
			},
			expectEnabledNodes: []string{"rio-grande"},
		},
		{
			name: "initial update not-ready",
			pods: []*corev1.Pod{
				makePod("benjamin-sisko", "rio-grande",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
			},
			updates: []*corev1.Node{
				makeNode("rio-grande", "mock://1", setLabels(matchingNodeLabels)),
			},
			expectEnabledNodes: []string{},
		},
		{
			name: "initial update no pod match",
			updates: []*corev1.Node{
				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
			},
			manip: func(m *flipopv1alpha1.Match) {
				m.PodNamespace = ""
				m.PodLabel = ""
			},
			expectEnabledNodes: []string{"rio-grande"},
		},
		{
			name: "update from not-ready to ready",
			pods: []*corev1.Pod{
				makePod("benjamin-sisko", "rio-grande",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
				makePod("worf", "orinoco",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
			},
			updates: []*corev1.Node{
				makeNode("rio-grande", "mock://1", setLabels(matchingNodeLabels)), // not yet ready
				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
			},
			expectEnabledNodes: []string{"rio-grande"},
		},
		{
			name: "update from ready to not-ready",
			pods: []*corev1.Pod{
				makePod("benjamin-sisko", "rio-grande",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
				makePod("worf", "orinoco",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
			},
			updates: []*corev1.Node{
				makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)),
				makeNode("rio-grande", "mock://1", setLabels(matchingNodeLabels)), // not ready
			},
			expectEnabledNodes: []string{},
		},
	}
	for _, tc := range tcs {
		tc := tc

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		t.Run(tc.name, func(t *testing.T) {
			m := &matchController{ll: logrus.New()}
			k8s := makeMatch()
			if tc.manip != nil {
				tc.manip(&k8s)
			}
			m.updateCriteria(&k8s)
			nMock := &mockNodeEnableDisabler{nodes: make(map[string]*corev1.Node)}
			m.action = nMock

			m.podIndexer = cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
				podNodeNameIndexerName: podNodeNameIndexer,
			})
			for _, p := range tc.pods {
				m.podIndexer.Add(p)
			}

			for _, n := range tc.updates {
				err := m.updateNode(ctx, n)
				require.NoError(t, err)
			}
			require.ElementsMatch(t, tc.expectEnabledNodes, nMock.names())

		})
	}
}

func TestUpdatePod(t *testing.T) {
	tcs := []struct {
		name                string
		initialIPAssignment map[string]string
		updates             []*corev1.Pod
		expectEnabledNodes  []string
	}{
		{
			name: "pod makes node assignable",
			updates: []*corev1.Pod{
				makePod("benjamin-sisko", "rio-grande",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
			},
			expectEnabledNodes: []string{"rio-grande"},
		},
		{
			name: "pod not-ready causes node to no longer match",
			updates: []*corev1.Pod{
				makePod("benjamin-sisko", "rio-grande",
					markReady, markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
				makePod("benjamin-sisko", "rio-grande",
					markRunning, setNamespace("star-fleet"), setLabels(matchingPodLabels)),
			},
			expectEnabledNodes: []string{},
		},
	}
	for _, tc := range tcs {
		tc := tc

		ctx := context.Background()

		t.Run(tc.name, func(t *testing.T) {
			m := &matchController{ll: logrus.New()}
			k8s := makeMatch()
			m.updateCriteria(&k8s)
			nMock := &mockNodeEnableDisabler{nodes: make(map[string]*corev1.Node)}
			m.action = nMock

			m.podIndexer = cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
				podNodeNameIndexerName: podNodeNameIndexer, // Necessary for updateNode used in setup
			})

			err := m.updateNode(ctx, makeNode("rio-grande", "mock://1", markReady, setLabels(matchingNodeLabels)))
			require.NoError(t, err)

			for _, p := range tc.updates {
				m.updatePod(p)
			}

			require.ElementsMatch(t, tc.expectEnabledNodes, nMock.names())
		})
	}
}

func TestMatchControllerUpdatePod(t *testing.T) {
	tcs := []struct {
		name string
	}{}
	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

		})
	}
}
