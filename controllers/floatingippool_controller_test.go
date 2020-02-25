package controllers

import (
	"context"
	"testing"

	flipopv1 "github.com/jcodybaker/flipop/api/v1"
	"github.com/jcodybaker/flipop/pkg/provider"
	"github.com/mmcshane/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
)

func TestFloatingIPPoolUpdateK8s(t *testing.T) {
	ctx := context.Background()
	k8s := &flipopv1.FloatingIPPool{
		Spec: flipopv1.FloatingIPPoolSpec{
			Provider: "mock",
			Region:   "DS9",
			Match: flipopv1.Match{
				NodeLabel:    "quadrent=alpha,system=bajor",
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
	f := &floatingIPPool{
		client: fake.NewFakeClientWithScheme(scheme.Scheme),
	}
	providers := map[string]provider.Provider{
		"mock": &provider.MockProvider{},
	}
	err := f.updateK8s(ctx, k8s, providers)
	require.NoError(t, err)
}
