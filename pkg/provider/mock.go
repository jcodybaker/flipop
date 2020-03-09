package provider

import (
	"context"
)

type MockProvider struct {
	IPToProviderIDFunc func(ctx context.Context, ip string) (string, error)
	AssignIPFunc       func(ctx context.Context, ip, providerID string) error
	NodeToIPFunc       func(ctx context.Context, providerID string) (string, error)
	CreateIPFunc       func(ctx context.Context, region string) (string, error)
}

func (m *MockProvider) IPToProviderID(ctx context.Context, ip string) (string, error) {
	return m.IPToProviderIDFunc(ctx, ip)
}

func (m *MockProvider) AssignIP(ctx context.Context, ip, providerID string) error {
	return m.AssignIPFunc(ctx, ip, providerID)
}

func (m *MockProvider) NodeToIP(ctx context.Context, providerID string) (string, error) {
	return m.NodeToIPFunc(ctx, providerID)
}

func (m *MockProvider) CreateIP(ctx context.Context, region string) (string, error) {
	return m.CreateIPFunc(ctx, region)
}
