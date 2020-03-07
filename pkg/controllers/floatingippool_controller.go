// /*
// MIT License

// Copyright (c) 2020 John Cody Baker

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// */

package controllers

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	flipopv1alpha1 "github.com/jcodybaker/flipop/pkg/apis/flipop/v1alpha1"
	"github.com/jcodybaker/flipop/pkg/provider"
	"github.com/sirupsen/logrus"

	flipopCS "github.com/jcodybaker/flipop/pkg/apis/flipop/generated/clientset/versioned"

	// k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	// metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	flipopInformers "github.com/jcodybaker/flipop/pkg/apis/flipop/generated/informers/externalversions/flipop/v1alpha1"
)

// FloatingIPPoolController watches for FloatingIPPool and then manages reconciliation for each
// pool.
type FloatingIPPoolController struct {
	kubeCS   kubernetes.Interface
	flipopCS flipopCS.Interface

	providers map[string]provider.Provider

	pools    map[string]floatingIPPool
	poolLock sync.Mutex

	// Fields provided at runtime.
	ll  logrus.FieldLogger
	ctx context.Context
}

type floatingIPPool struct {
	matchController *matchController
	ipController    *ipController
}

// NewFloatingIPPoolController creates a new FloatingIPPoolController.
func NewFloatingIPPoolController(kubeConfig clientcmd.ClientConfig, providers map[string]provider.Provider, ll logrus.FieldLogger) (*FloatingIPPoolController, error) {
	c := &FloatingIPPoolController{
		providers: providers,
		pools:     make(map[string]floatingIPPool),
		ll:        ll,
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
func (c *FloatingIPPoolController) Run(ctx context.Context) {
	informer := flipopInformers.NewFloatingIPPoolInformer(c.flipopCS, "", floatingIPPoolResyncPeriod, cache.Indexers{})
	informer.AddEventHandler(c)
	c.ctx = ctx
	informer.Run(ctx.Done())
	c.poolLock.Lock()
	defer c.poolLock.Lock()
	for _, m := range c.pools {
		// Our parent's canceling of the context should stop all of the children concurrently.
		// This loop just verifies all children have completed.
		m.matchController.stop()
		// TODO - stop the ipController
	}
}

// OnAdd implements the shared informer ResourceEventHandler for FloatingIPPools.
func (c *FloatingIPPoolController) OnAdd(obj interface{}) {
	k8sPool, ok := obj.(*flipopv1alpha1.FloatingIPPool)
	if !ok {
		c.ll.WithField("unexpected_type", fmt.Sprintf("%T", obj)).Warn("unexpected type")
	}
	c.updateOrAdd(k8sPool)
}

// OnUpdate implements the shared informer ResourceEventHandler for FloatingIPPools.
func (c *FloatingIPPoolController) OnUpdate(_, newObj interface{}) {
	k8sPool, ok := newObj.(*flipopv1alpha1.FloatingIPPool)
	if !ok {
		c.ll.WithField("unexpected_type", fmt.Sprintf("%T", newObj)).Warn("unexpected type")
	}
	c.updateOrAdd(k8sPool)
}

func (c *FloatingIPPoolController) updateOrAdd(k8sPool *flipopv1alpha1.FloatingIPPool) {
	c.poolLock.Lock()
	defer c.poolLock.Unlock()
	ll := c.ll.WithField("floating_ip_pool", fmt.Sprintf("%s/%s", k8sPool.Namespace, k8sPool.Name))
	isValid := c.validate(ll, k8sPool)

	pool, ok := c.pools[k8sPool.GetSelfLink()]
	if !ok {
		if !isValid {
			return
		}
		ipc := newIPController(ll, nil)
		pool = floatingIPPool{
			matchController: newMatchController(ll, c.kubeCS, ipc),
			ipController:    ipc,
		}
		ll.Info("FloatingIPPool added; beginning reconciliation")
		c.pools[k8sPool.GetSelfLink()] = pool
	}
	if !isValid {
		pool.matchController.stop()
		pool.ipController.stop()
		delete(c.pools, k8sPool.GetSelfLink())
		return
	}

	prov := c.providers[k8sPool.Spec.Provider]
	ipChange := pool.ipController.updateProvider(prov, k8sPool.Spec.Region)

	matchChange := pool.matchController.updateCriteria(&k8sPool.Spec.Match)
	if matchChange {
		// Changing match criteria invalids any existing assignment, restart the ipController.
		// Assignments for nodes matching both old and new criteria, should remain in place.
		pool.ipController.stop()
		ipChange = true
		pool.matchController.start(c.ctx)
	}

	pool.ipController.updateIPs(k8sPool.Spec.IPs, k8sPool.Spec.DesiredIPs)
	if ipChange {
		pool.ipController.start(c.ctx)
		pool.matchController.resync()
	}
}

func (c *FloatingIPPoolController) validate(ll logrus.FieldLogger, k8sPool *flipopv1alpha1.FloatingIPPool) bool {
	if _, ok := c.providers[k8sPool.Spec.Provider]; !ok {
		c.updateStatus(k8sPool, fmt.Sprintf("unknown provider %q", k8sPool.Spec.Provider))
		ll.Warn("FloatingIPPool referenced unknown provider")
		return false
	}
	if len(k8sPool.Spec.IPs) == 0 && k8sPool.Spec.DesiredIPs == 0 {
		c.updateStatus(k8sPool, "ips or desiredIPs must be provided")
		ll.Warn("FloatingIPPool had neither ips nor desiredIPs")
		return false
	}
	err := validateMatch(&k8sPool.Spec.Match)
	if err != nil {
		c.updateStatus(k8sPool, "Error "+err.Error())
		ll.WithError(err).Warn("FloatingIPPool had invalid match criteria")
		return false
	}
	return true
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
	c.ll.WithField("floating_ip_pool", fmt.Sprintf("%s/%s", k8sPool.Namespace, k8sPool.Name)).Info("pool deleted")
	pool.matchController.stop()
	pool.ipController.stop()
	delete(c.pools, k8sPool.GetSelfLink())
}

func (c *FloatingIPPoolController) updateStatus(k8sPool *flipopv1alpha1.FloatingIPPool, errMsg string) {
	s := flipopv1alpha1.FloatingIPPoolStatus{
		Error: errMsg,
	}
	if reflect.DeepEqual(s, k8sPool.Status) {
		return
	}
	k8sPool.Status = s
	_, err := c.flipopCS.FlipopV1alpha1().FloatingIPPools(k8sPool.Namespace).UpdateStatus(k8sPool)
	if err != nil {
		c.ll.WithError(err).Error("updating FloatingIPPool status")
	}
}
