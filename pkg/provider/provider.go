package provider

import (
	"context"
	"errors"
)

var (
	// ErrNotFound wraps provider specific not found errors.
	ErrNotFound = errors.New("not found")

	// ErrNodeHasIP is returned if the address cannot be assigned because
	// it already has a Floating IP assigned.
	ErrNodeHasIP = errors.New("node already has Floating IP")
)

// Provider defines a platform which offers kubernetes VMs and floating ips.
type Provider interface {
	IPtoProviderID(ctx context.Context, ip string) (string, error)
	AssignIP(ctx context.Context, ip, providerID string) error
}
