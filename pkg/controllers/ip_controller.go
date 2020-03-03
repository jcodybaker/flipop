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
	createRetries       int
	createNextRetry     time.Time

	// ipToStatus tracks each IP address, including its current assignment, errors, and retries.
	ipToStatus map[string]*ipStatus
	// providerIDToIP maps a node providerID to an IP address. It retains references for nodes
	// which are not currently active, but may become active again.
	providerIDToIP map[string]string
	// providerIDToNodeName contains ONLY active nodes. It is the source of truth for which
	// node providerIDs are active.
	providerIDToNodeName map[string]string

	assignableIPs   *orderedSet
	assignableNodes *orderedSet
}

type ipStatus struct {
	retries        int
	nextRetry      time.Time
	message        string
	retrySchedule  provider.RetrySchedule
	nodeProviderID string
}

// newIPController initializes an ipController.
func newIPController(ll logrus.FieldLogger, prov provider.Provider, region string, onNewIPs NewIPFunc) *ipController {
	return &ipController{
		ll:                   ll,
		provider:             prov,
		region:               region,
		onNewIPs:             onNewIPs,
		ipToStatus:           make(map[string]*ipStatus),
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
	for j := len(i.ips) - 1; j < i.desiredIPs; j++ {
		ip, err := i.provider.CreateIP(ctx, i.region)
		if err != nil {
			i.createRetrySchedule = provider.ErrorToRetrySchedule(err)
			i.createRetries, i.createNextRetry = i.createRetrySchedule.Next(i.createRetries)
			i.retry(i.createNextRetry)
			i.ll.WithError(err).Error("requesting new IP from provider")
			return
		}
		i.pendingIPs = append(i.pendingIPs, ip)
		i.createRetries = 0
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
			retrySchedule: healthyRetrySchedule,
			message:       "available",
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
				retrySchedule: provider.RetryFast,
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
			status.retries, status.nextRetry = status.retrySchedule.Next(status.retries)
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
					retrySchedule:  provider.RetryFast,
					message:        "state unknown; cache / provider mismatch",
					nodeProviderID: "", // reset
				}
			}
			i.providerIDToIP[providerID] = ip
		}

		status.message = ""
		status.retries = 0
		status.retrySchedule = healthyRetrySchedule
		_, status.nextRetry = status.retrySchedule.Next(status.retries)
		ll.Debug("provider ip mapping verified")
	}
}

func (i *ipController) reconcileAssignment(ctx context.Context) {
	for i.assignableIPs.Len() != 0 && i.assignableNodes.Len() != 0 {
		if ctx.Err() != nil {
			return // short-circuit on context cancel.
		}
		providerID := i.assignableNodes.Front()
		ip := i.assignableIPs.Front()

		status := i.ipToStatus[ip]
		status.message = "pending verification"
		status.retrySchedule = provider.RetryFast
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
		if err != nil {
			status.retrySchedule = provider.ErrorToRetrySchedule(err)
			status.message = fmt.Sprintf("assigning IP to node: %s")
			ll.WithError(err).Error("assigning IP to node")
		}
		_, status.nextRetry = status.retrySchedule.Next(status.retries)
		i.retry(status.nextRetry)
	}
}

func (i *ipController) ReleaseNode(node *corev1.Node) {
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
	if _, ok := i.providerIDToIP[providerID]; ok {
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
