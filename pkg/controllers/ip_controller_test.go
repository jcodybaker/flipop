package controllers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jcodybaker/flipop/pkg/provider"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestIPControllerReconcileDesiredIPs(t *testing.T) {
	type createIPRes struct {
		ip     string
		err    error
		region string
	}
	tcs := []struct {
		name             string
		desiredIPs       int
		existingIPs      []string
		region           string
		responses        []createIPRes
		expectPendingIPs []string
		expectRetry      bool
	}{
		{
			name:             "success",
			desiredIPs:       3,
			existingIPs:      []string{"192.168.1.1"},
			responses:        []createIPRes{{ip: "192.168.1.2", region: "earth"}, {ip: "192.168.1.3", region: "earth"}},
			expectPendingIPs: []string{"192.168.1.2", "192.168.1.3"},
		},
		{
			name:        "create fails",
			desiredIPs:  3,
			existingIPs: []string{"192.168.1.1"},
			responses:   []createIPRes{{err: errors.New("nope"), region: "earth"}},
			expectRetry: true,
		},
	}
	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			i := &ipController{
				desiredIPs: tc.desiredIPs,
				ips:        tc.existingIPs,
				region:     tc.region,
				provider: &provider.MockProvider{
					CreateIPFunc: func(_ context.Context, region string) (string, error) {
						require.GreaterOrEqual(t, len(tc.responses), 1, "unexpected call to CreateIPFunc")
						require.Equal(t, tc.region, region)
						ip := tc.responses[0].ip
						err := tc.responses[0].err
						tc.responses = tc.responses[1:]
						return ip, err
					},
				},
				ll: logrus.New(),
			}
			i.reconcileDesiredIPs(ctx)
			require.ElementsMatch(t, tc.expectPendingIPs, i.pendingIPs)
			require.Equal(t, tc.expectRetry, i.nextRetry != time.Time{})
			require.Empty(t, tc.responses) // We should have used all expected responses.
		})
	}
}

func TestIPControllerReconcilePendingIPs(t *testing.T) {
	tcs := []struct {
		name           string
		pendingIPs     []string
		existingIPs    []string
		onNewIPsReturn error
		expectedIPs    []string
	}{}
	for _, tc := range tcs {
		tc := tc
		ctx := context.Background()
		t.Run(tc.name, func(t *testing.T) {

			i := &ipController{
				pendingIPs: tc.pendingIPs,

				onNewIPs: func(ctx context.Context, ips []string) error {
					require.EqualValues(t, tc.expectedIPs, ips)
					return tc.onNewIPsReturn
				},
				ll: logrus.New(),
			}
			copy(i.ips, tc.existingIPs)
			copy(i.pendingIPs, tc.pendingIPs)
			i.reconcilePendingIPs(ctx)
			if tc.onNewIPsReturn == nil {
				require.EqualValues(t, tc.expectedIPs, i.ips)
				require.False(t, i.nextRetry != time.Time{}, "unexpected retry")
			} else {
				require.True(t, i.nextRetry != time.Time{}, "expected retry")
				require.EqualValues(t, tc.existingIPs, i.ips)
				require.EqualValues(t, tc.pendingIPs, i.pendingIPs)
			}
		})
	}
}

func TestIPControllerReconcileIPStatus(t *testing.T) {
	type ipToProviderIDRes struct {
		ip          string
		err         error
		providerID  string
		expectRetry bool
	}
	tcs := []struct {
		name                  string
		ips                   []string
		responses             []ipToProviderIDRes
		setup                 func(i *ipController)
		expectProviderIDToIP  map[string]string
		expectRetry           bool
		expectAssignableIPs   []string
		expectAssignableNodes []string
	}{
		{
			name:      "new ips",
			ips:       []string{"192.168.1.1", "192.168.1.2"},
			responses: []ipToProviderIDRes{{ip: "192.168.1.1", providerID: "mock://1"}, {ip: "192.168.1.2"}},
			expectProviderIDToIP: map[string]string{
				"mock://1": "192.168.1.1",
			},
			expectAssignableIPs: []string{"192.168.1.1", "192.168.1.2"},
		},
		{
			name:      "provider error",
			ips:       []string{"192.168.1.1"},
			responses: []ipToProviderIDRes{{ip: "192.168.1.1", err: provider.ErrInProgress}},
			expectProviderIDToIP: map[string]string{
				"mock://1": "192.168.1.1",
			},
			setup: func(i *ipController) {
				i.providerIDToIP["mock://1"] = "192.168.1.1"
				i.ipToStatus["192.168.1.1"] = &ipStatus{
					nodeProviderID: "mock://1",
				}
			},
			expectRetry: true,
		},
		{
			name:      "ip not found",
			ips:       []string{"192.168.1.1"},
			responses: []ipToProviderIDRes{{ip: "192.168.1.1", err: provider.ErrNotFound}},
			setup: func(i *ipController) {
				i.providerIDToIP["mock://1"] = "192.168.1.1"
				i.ipToStatus["192.168.1.1"] = &ipStatus{
					nodeProviderID: "mock://1",
				}
				i.providerIDToNodeName["mock://1"] = "some-node"
			},
			expectRetry:           true,
			expectProviderIDToIP:  map[string]string{},
			expectAssignableIPs:   []string{},
			expectAssignableNodes: []string{"mock://1"},
		},
		{
			name: "provider reports ip reassigned",
			ips:  []string{"192.168.1.1", "172.16.2.2"},
			responses: []ipToProviderIDRes{
				{ip: "192.168.1.1", providerID: "mock://2"},
				// report in-progress for 172.16.2.2 to avoid impacting results.
				{ip: "172.16.2.2", err: provider.ErrInProgress},
			},
			setup: func(i *ipController) {
				i.providerIDToIP["mock://1"] = "192.168.1.1"
				i.providerIDToIP["mock://2"] = "172.16.2.2"
				i.ipToStatus["192.168.1.1"] = &ipStatus{
					nodeProviderID: "mock://1",
				}
				i.ipToStatus["172.16.2.2"] = &ipStatus{
					nodeProviderID: "mock://2",
				}
				i.providerIDToNodeName["mock://1"] = "mock-one"
				i.providerIDToNodeName["mock://2"] = "mock-two"
			},
			expectRetry:           true,
			expectProviderIDToIP:  map[string]string{"mock://2": "192.168.1.1"},
			expectAssignableIPs:   []string{"172.16.2.2"},
			expectAssignableNodes: []string{"mock://1"},
		},
	}
	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			i := newIPController(logrus.New(), &provider.MockProvider{
				IPtoProviderIDFunc: func(_ context.Context, ip string) (string, error) {
					require.GreaterOrEqual(t, len(tc.responses), 1, "unexpected call to IPtoProviderIDFunc")
					require.Equal(t, tc.responses[0].ip, ip)
					providerID := tc.responses[0].providerID
					err := tc.responses[0].err
					tc.responses = tc.responses[1:]
					return providerID, err
				},
			}, "", nil)
			i.ips = tc.ips
			if tc.setup != nil {
				tc.setup(i)
			}
			i.reconcileIPStatus(ctx)

			require.Equal(t, tc.expectProviderIDToIP, i.providerIDToIP)
			for providerID, ip := range tc.expectProviderIDToIP {
				status := i.ipToStatus[ip]
				require.NotNil(t, status)
				require.Equal(t, providerID, status.nodeProviderID)
			}

			require.Equal(t, tc.expectRetry, i.nextRetry != time.Time{})
			require.Equal(t, len(tc.expectAssignableIPs), i.assignableIPs.Len())
			for _, ip := range tc.expectAssignableIPs {
				require.True(t, i.assignableIPs.IsSet(ip))
			}
			for _, ip := range tc.expectAssignableIPs {
				require.Contains(t, i.ipToStatus, ip)
			}

			require.Equal(t, len(tc.expectAssignableNodes), i.assignableNodes.Len())
			for _, providerID := range tc.expectAssignableNodes {
				require.True(t, i.assignableNodes.IsSet(providerID))
			}
		})
	}
}
