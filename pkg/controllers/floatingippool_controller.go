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
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	flipopv1alpha1 "github.com/jcodybaker/flipop/pkg/apis/flipop/v1alpha1"
	"github.com/jcodybaker/flipop/pkg/provider"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/labels"

	flipopCS "github.com/jcodybaker/flipop/pkg/apis/flipop/generated/clientset/versioned"

	// k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	// metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	flipopInformers "github.com/jcodybaker/flipop/pkg/apis/flipop/generated/informers/externalversions/flipop/v1alpha1"
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

	pools    map[string]*matchController
	poolLock sync.Mutex

	// Fields provided at runtime.
	ll  logrus.FieldLogger
	ctx context.Context
}

// NewFloatingIPPoolController creates a new FloatingIPPoolController.
func NewFloatingIPPoolController(kubeConfig clientcmd.ClientConfig, providers map[string]provider.Provider, ll logrus.FieldLogger) (*FloatingIPPoolController, error) {
	c := &FloatingIPPoolController{
		providers: providers,
		pools:     make(map[string]*matchController),
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
	for _, m := range c.pools {
		// Our parent's canceling of the context should stop all of the children concurrently.
		// This loop just verifies all children have completed.
		m.stop()
	}
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
		pool = &matchController{
			ll:       c.ll.WithFields(logrus.Fields{"pool": k8sPool.GetSelfLink()}),
			kubeCS:   c.kubeCS,
			flipopCS: c.flipopCS,
		}
		pool.ll.Info("FloatingIPPool added; beginning reconciliation")
		c.pools[k8sPool.GetSelfLink()] = pool
	}
	specChange := true
	if pool.match != nil {
		specChange = !reflect.DeepEqual(&pool.match, &k8sPool.Spec.Match)
	}
	pool.match = k8sPool.Spec.Match.DeepCopy()
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

	prov := c.providers[k8sPool.Spec.Provider]
	if prov == nil {
		pool.ll.WithFields(logrus.Fields{"provider": k8sPool.Spec.Provider}).
			Error("FloatingIPPool referenced unknown provider")
		pool.setStatus(pool.ctx, fmt.Sprintf("unknown provider %q", k8sPool.Spec.Provider))
		return
	}
	// TODO

	var err error
	pool.nodeSelector = nil

	if pool.match.NodeLabel != "" {
		pool.nodeSelector, err = labels.Parse(pool.match.NodeLabel)
		if err != nil {
			pool.ll.WithError(err).Error("parsing node selector")
			pool.setStatus(pool.ctx, fmt.Sprintf("parsing node selector: %s", err))
			return
		}
	}

	pool.podSelector = nil
	if pool.match.PodLabel != "" {
		pool.podSelector, err = labels.Parse(pool.match.PodLabel)
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
