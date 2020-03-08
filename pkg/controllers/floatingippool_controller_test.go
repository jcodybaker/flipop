package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/digitalocean/flipop/pkg/provider"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeCSFake "k8s.io/client-go/kubernetes/fake"

	flipCSFake "github.com/digitalocean/flipop/pkg/apis/flipop/generated/clientset/versioned/fake"
	flipopv1alpha1 "github.com/digitalocean/flipop/pkg/apis/flipop/v1alpha1"
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

			updatedK8s, err := c.flipopCS.FlipopV1alpha1().FloatingIPPools(k8s.Namespace).Get(k8s.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, f.ipController.ips, updatedK8s.Spec.IPs)
		})
	}
}

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

func makeFloatingIPPool() *flipopv1alpha1.FloatingIPPool {
	return &flipopv1alpha1.FloatingIPPool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "deep-space-nine",
		},
		Spec: flipopv1alpha1.FloatingIPPoolSpec{
			Provider: "mock",
			Region:   "alpha-quadrant",
			Match:    makeMatch(),
			IPs: []string{
				"192.168.1.1",
				"172.16.2.2",
			},
		},
	}
}
