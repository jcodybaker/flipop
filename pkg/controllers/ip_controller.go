package controllers

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jcodybaker/flipop/pkg/provider"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
)

const (
	reconcilePeriod = time.Minute
)

var (
	healthyRetrySchedule = provider.RetrySchedule{5 * time.Minute}
)

// NewIPFunc describes a callback used when the list of IPs is updated.
type NewIPFunc func(ctx context.Context, ips []string) error

type ipController struct {
	nextRetry  time.Time
	retryTimer *time.Timer

	provider provider.Provider
	region   string

	desiredIPs int
	ips        []string
	pendingIPs []string

	onNewIPs NewIPFunc

	ll   logrus.FieldLogger
	poke chan struct{}
	lock sync.Mutex

	createRetrySchedule provider.RetrySchedule
	createAttempts      int
	createNextRetry     time.Time

	// ipToStatus tracks each IP address, including its current assignment, errors, and retries.
	ipToStatus map[string]*ipStatus
	// providerIDToIP maps a node providerID to an IP address. It retains references for nodes
	// which are not currently active, but may become active again.
	providerIDToIP map[string]string
	// providerIDToNodeName contains ONLY active nodes. It is the source of truth for which
	// node providerIDs are active.
	providerIDToNodeName map[string]string

	providerIDToRetry map[string]*retry

	assignableIPs   *orderedSet
	assignableNodes *orderedSet
}

type ipStatus struct {
	retry
	message        string
	nodeProviderID string
}

type retry struct {
	attempts      int
	nextRetry     time.Time
	retrySchedule provider.RetrySchedule
}

// newIPController initializes an ipController.
func newIPController(ll logrus.FieldLogger, prov provider.Provider, region string, onNewIPs NewIPFunc) *ipController {
	return &ipController{
		ll:                   ll,
		provider:             prov,
		region:               region,
		onNewIPs:             onNewIPs,
		ipToStatus:           make(map[string]*ipStatus),
		providerIDToRetry:    make(map[string]*retry),
		providerIDToIP:       make(map[string]string),
		providerIDToNodeName: make(map[string]string),
		assignableIPs:        newOrderedSet(),
		assignableNodes:      newOrderedSet(),
		// poke MUST be a buffered channel because we hold the lock when poking, AND when consuming
		// the poke.
		poke:       make(chan struct{}, 10),
		retryTimer: time.NewTimer(0),
	}
}

// Run will start reconciliation of floating IPs until the context is canceled.
func (i *ipController) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-i.poke:
			i.retryTimer.Stop() // need to drain the timer
			i.reconcile(ctx)
		case <-i.retryTimer.C:
			i.reconcile(ctx)
		}
		i.retryTimer.Reset(i.retryTimerDuration())
	}
}

// retryTimerDuration converts our nextRetry timestamp to a duration from now.
func (i *ipController) retryTimerDuration() time.Duration {
	dur := i.nextRetry.Sub(time.Now())
	if dur < 0 {
		return 0
	}
	return dur
}

func (i *ipController) reconcile(ctx context.Context) {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.nextRetry = time.Now().Add(reconcilePeriod)

	i.reconcileDesiredIPs(ctx)
	i.reconcilePendingIPs(ctx)
	i.reconcileIPStatus(ctx)
	i.reconcileAssignment(ctx)

}

func (i *ipController) retry(next time.Time) {
	if (i.nextRetry == time.Time{}) || (i.nextRetry.After(next)) {
		i.nextRetry = next
	}
}

func (i *ipController) reconcileDesiredIPs(ctx context.Context) {
	if ctx.Err() != nil {
		return // short-circuit on context cancel.
	}
	if i.createNextRetry.After(time.Now()) {
		i.retry(i.createNextRetry)
		return
	}
	// Acquire new IPs if needed. If this fails, we can try again next reconcile.
	for j := len(i.ips); j < i.desiredIPs; j++ {
		ip, err := i.provider.CreateIP(ctx, i.region)
		if err != nil {
			i.createRetrySchedule = provider.ErrorToRetrySchedule(err)
			i.createAttempts, i.createNextRetry = i.createRetrySchedule.Next(i.createAttempts)
			i.retry(i.createNextRetry)
			i.ll.WithError(err).Error("requesting new IP from provider")
			return
		}
		i.pendingIPs = append(i.pendingIPs, ip)
		i.createAttempts = 0
	}
}

func (i *ipController) reconcilePendingIPs(ctx context.Context) {
	if ctx.Err() != nil {
		return // short-circuit on context cancel.
	}
	allIPs := append(append([]string{}, i.ips...), i.pendingIPs...)
	err := i.onNewIPs(ctx, allIPs)
	if err != nil {
		i.ll.WithError(err).Error("updating IPs with caller")
		return
	}
	for _, ip := range i.pendingIPs {
		// shortcut lookup for ip provider
		i.ipToStatus[ip] = &ipStatus{
			retry:   retry{retrySchedule: healthyRetrySchedule},
			message: "available",
		}
		// This IP is empty and should be a priority for assignment, put it at the front.
		i.assignableIPs.Add(ip, true)
	}
	i.ips = allIPs
	i.pendingIPs = nil
}

func (i *ipController) reconcileIPStatus(ctx context.Context) {
	for _, ip := range i.ips {
		if ctx.Err() != nil {
			return // short-circuit on context cancel.
		}

		status, ipInitialized := i.ipToStatus[ip]
		if !ipInitialized {
			status = &ipStatus{
				retry: retry{retrySchedule: provider.RetryFast},
			}
			i.ipToStatus[ip] = status
		}

		if status.nextRetry.After(time.Now()) {
			continue
		}

		expectedProviderID := status.nodeProviderID
		ll := i.ll.WithField("ip", ip)
		ll.Debug("retrieving IP current provider ID")
		providerID, err := i.provider.IPtoProviderID(ctx, ip)
		if err != nil {
			if err == provider.ErrNotFound {
				// If the IP's not found, try to do the best we can. We'll continue to check its
				// status according to the retry schedule. If it recovers, it should be added back.
				oldProviderID := status.nodeProviderID
				status.nodeProviderID = ""
				delete(i.providerIDToIP, oldProviderID)
				if nodeName, ok := i.providerIDToNodeName[oldProviderID]; ok {
					i.assignableNodes.Add(oldProviderID, true)
					ll.WithField("node", nodeName).Error("ip not found; node will be reassigned")
				} else {
					ll.Error("ip not found; node will be removed from assignable")
					i.assignableIPs.Delete(ip)
				}
			}
			status.retrySchedule = provider.ErrorToRetrySchedule(err)
			status.attempts, status.nextRetry = status.retrySchedule.Next(status.attempts)
			status.message = fmt.Sprintf("retrieving IPs current provider ID: %s", err)
			i.retry(status.nextRetry)
			ll.WithError(err).Error("retrieving IPs current provider ID")
			continue
		}
		ll = ll.WithField("provider_id", providerID)

		var isProviderIDActiveNode bool
		if providerID == "" {
			// This IP isn't pointed anywhere, mark it as available for assignment.
			i.assignableIPs.Add(ip, true)
			ll.Info("ip address is available for assignment")
		} else {
			var nodeName string
			nodeName, isProviderIDActiveNode = i.providerIDToNodeName[providerID]
			if isProviderIDActiveNode {
				ll = ll.WithField("node", nodeName)
			}
		}

		if expectedProviderID != providerID {
			// Update our records to reflect reality.
			status.nodeProviderID = providerID

			if !ipInitialized {
				if isProviderIDActiveNode {
					i.assignableNodes.Delete(providerID)
					ll.Info("ip address has existing assignment, reusing")
				} else {
					// The IP references a node we don't know about yet.
					ll.Info("ip address has existing assignment, but is available")
					i.assignableIPs.Add(ip, false)
				}
			}

			expectedIP := i.providerIDToIP[providerID]
			if expectedIP != "" && expectedIP != ip {
				ll.WithField("expected_ip", expectedIP).
					Warn("node assignment mismatch; updating cache to reflect provider")
				i.assignableIPs.Add(expectedIP, false)
				// mark the node's old IP for immediate retry.
				i.ipToStatus[expectedIP] = &ipStatus{
					retry:          retry{retrySchedule: provider.RetryFast},
					message:        "state unknown; cache / provider mismatch",
					nodeProviderID: "", // reset
				}
			}
			i.providerIDToIP[providerID] = ip

			delete(i.providerIDToIP, expectedProviderID)
			if evictedNodeName, ok := i.providerIDToNodeName[providerID]; ok {
				ll.WithFields(logrus.Fields{
					"node": evictedNodeName,
					"ip":   expectedIP,
				}).Info("nodes ip was claimed by other node; marking for reassignment")
				i.assignableNodes.Add(expectedProviderID, true)
			}
		}

		status.message = ""
		status.attempts = 0
		status.retrySchedule = healthyRetrySchedule
		_, status.nextRetry = status.retrySchedule.Next(status.attempts)
		ll.Debug("provider ip mapping verified")
	}
}

func (i *ipController) reconcileAssignment(ctx context.Context) {
	var retryIPs, retryProviders []string
	defer func() {
		// Requeue anything we skipped or errored on.
		for _, ip := range retryIPs {
			i.assignableIPs.Add(ip, false)
		}
		for _, providerID := range retryProviders {
			i.assignableNodes.Add(providerID, false)
		}
	}()
	for i.assignableIPs.Len() != 0 && i.assignableNodes.Len() != 0 {
		if ctx.Err() != nil {
			return // short-circuit on context cancel.
		}

		ip := i.assignableIPs.Front()

		// If this IP was previously involved in an error we shouldn't attempt to try again before
		// its retry timestamp.
		status := i.ipToStatus[ip]
		if (status.nextRetry != time.Time{}) && !status.nextRetry.After(time.Now()) {
			retryIPs = append(retryIPs, ip)
			i.retry(status.nextRetry)
			continue
		}

		providerID := i.assignableNodes.Front()

		// Similarly, if this node was involved in an error we should wait until after its retry
		// timestamp has elapsed.
		nRetry, ok := i.providerIDToRetry[providerID]
		if ok && (nRetry.nextRetry != time.Time{}) && !nRetry.nextRetry.After(time.Now()) {
			retryIPs = append(retryIPs, ip)
			retryProviders = append(retryProviders, providerID)
			i.retry(nRetry.nextRetry)
			continue
		}

		oldProviderID := status.nodeProviderID
		// This IP may have been released by a different node. We gave the old node a chance to
		// recover, but this new node needs an IP. Remove the old node's claim it one exists.
		delete(i.providerIDToIP, oldProviderID)
		status.nodeProviderID = providerID

		// record the assignment now, but also record it as pending
		i.providerIDToIP[providerID] = ip

		ll := i.ll.WithFields(logrus.Fields{
			"ip":         ip,
			"providerID": providerID,
		})
		ll.Info("assigning IP to node")

		err := i.provider.AssignIP(ctx, ip, providerID)
		if err == nil || err == provider.ErrInProgress {
			status.message = "pending verification"
			status.retrySchedule = provider.RetryFast
			delete(i.providerIDToRetry, providerID)
			_, status.nextRetry = status.retrySchedule.Next(status.attempts)
		} else {
			status.retrySchedule = provider.ErrorToRetrySchedule(err)
			status.message = fmt.Sprintf("assigning IP to node: %s", err)
			ll.WithError(err).Error("assigning IP to node")
			if nRetry == nil {
				nRetry = &retry{}
			}
			nRetry.attempts, nRetry.nextRetry = nRetry.retrySchedule.Next(nRetry.attempts)
			i.providerIDToRetry[providerID] = nRetry
			i.retry(nRetry.nextRetry)
		}
		i.retry(status.nextRetry)
	}
}

func (i *ipController) DisableNode(node *corev1.Node) {
	providerID := node.Spec.ProviderID
	if providerID == "" {
		return
	}
	i.lock.Lock()
	defer i.lock.Unlock()
	i.poke <- struct{}{}
	delete(i.providerIDToNodeName, providerID)
	if ip, ok := i.providerIDToIP[providerID]; ok {
		// Add this IP to the back of the list. This increases the chances that the IP mapping
		// can be retained if the node recovers.
		i.assignableIPs.Add(ip, false)
		// cancel any pending retries
		delete(i.ipToStatus, ip)
	}
	i.assignableNodes.Delete(providerID)
	// We leave the providerID<->IP mappings in providerIDToIP/ipStatus.nodeProviderID so we can
	// reuse the IP mapping, if it's not immediately recovered.
}

func (i *ipController) EnableNode(node *corev1.Node) {
	providerID := node.Spec.ProviderID
	if providerID == "" {
		return
	}
	i.lock.Lock()
	defer i.lock.Unlock()
	i.poke <- struct{}{}
	i.providerIDToNodeName[providerID] = node.Name
	if ip := i.providerIDToIP[providerID]; ip != "" {
		return // Already has an IP.
	}
	i.assignableNodes.Add(providerID, false)
}

type orderedSet struct {
	l *list.List
	m map[string]*list.Element
}

func newOrderedSet() *orderedSet {
	return &orderedSet{
		l: list.New(),
		m: make(map[string]*list.Element),
	}
}

// Add v to the s, if it doesn't already exist. If front is true it will be
// added/moved to the front, otherwise its added to the end.
func (o *orderedSet) Add(v string, front bool) {
	e, ok := o.m[v]
	if ok {
		if front {
			o.l.MoveToFront(e)
		}
		return
	}
	if front {
		o.m[v] = o.l.PushFront(v)
	} else {
		o.m[v] = o.l.PushBack(v)
	}
}

// Front returns the first item in the set, or "" if the set is empty.
func (o *orderedSet) Front() string {
	e := o.l.Front()
	if e == nil {
		return ""
	}
	v := e.Value.(string)
	delete(o.m, v)
	o.l.Remove(e)
	return v
}

// Len returns the length of the set.
func (o *orderedSet) Len() int {
	return o.l.Len()
}

// Delete removes v from the set.
func (o *orderedSet) Delete(v string) bool {
	e, ok := o.m[v]
	if !ok {
		return false
	}
	delete(o.m, v)
	o.l.Remove(e)
	return true
}

// IsSet returns true if v is in the set.
func (o *orderedSet) IsSet(v string) bool {
	_, ok := o.m[v]
	if !ok {
		return false
	}
	return true
}
