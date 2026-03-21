// MIT License
//
// Copyright (c) 2022-2026 Arsene Tochemey Gandote
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package ego

import (
	"context"
	"time"

	"github.com/tochemey/goakt/v4/discovery"
)

// ClusterProvider is the abstraction for cluster peer discovery.
//
// Implementations must be safe for concurrent use. The engine calls Start once
// before any calls to DiscoverPeers, and Stop once during graceful shutdown.
type ClusterProvider interface {
	// ID returns a unique, human-readable name for this provider (e.g., "consul", "static").
	ID() string

	// Start initializes the provider and registers the current node with the
	// discovery backend. Called once when the engine enters cluster mode.
	// The provided context is cancelled on shutdown.
	Start(ctx context.Context) error

	// DiscoverPeers returns the current list of peer addresses.
	// Each entry must be a "host:port" string where host is an IP address or
	// hostname and port is the discovery port (e.g., "192.168.1.10:7946",
	// "[::1]:7946"). Use [net.JoinHostPort] to build entries correctly,
	// especially for IPv6 addresses.
	// Called periodically by the cluster subsystem to refresh the peer set.
	DiscoverPeers(ctx context.Context) ([]string, error)

	// Stop deregisters the current node and releases resources.
	// Called once during engine shutdown. No further calls to DiscoverPeers
	// will be made after Stop returns.
	Stop(ctx context.Context) error
}

// clusterProviderAdapter wraps a ClusterProvider to satisfy GoAkt's
// discovery.Provider interface. The adapter owns a background-derived context
// that is cancelled on Close, keeping the discovery lifecycle independent of
// any caller-scoped context.
type clusterProviderAdapter struct {
	provider ClusterProvider
	ctx      context.Context
	cancel   context.CancelFunc
}

// Compile-time interface check.
var _ discovery.Provider = (*clusterProviderAdapter)(nil)

// newClusterProviderAdapter creates an adapter that bridges ClusterProvider
// to GoAkt's discovery.Provider. The adapter owns its own background-derived
// context so the discovery lifecycle is independent of any caller-scoped
// context (e.g. a startup context with a deadline). The adapter context is
// cancelled when Close is called.
func newClusterProviderAdapter(provider ClusterProvider) *clusterProviderAdapter {
	ctx, cancel := context.WithCancel(context.Background())
	return &clusterProviderAdapter{
		provider: provider,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// ID returns the provider name.
func (a *clusterProviderAdapter) ID() string {
	return a.provider.ID()
}

// Initialize delegates to the provider's Start method.
// GoAkt calls Initialize once during actor system startup.
func (a *clusterProviderAdapter) Initialize() error {
	return a.provider.Start(a.ctx)
}

// Register is a no-op. The simplified DiscoveryProvider interface merges
// registration into Start.
func (a *clusterProviderAdapter) Register() error {
	return nil
}

// Deregister is a no-op. The simplified DiscoveryProvider interface merges
// deregistration into Stop.
func (a *clusterProviderAdapter) Deregister() error {
	return nil
}

// DiscoverPeers delegates to the provider's DiscoverPeers method,
// passing the adapter's context for cancellation and timeout support.
func (a *clusterProviderAdapter) DiscoverPeers() ([]string, error) {
	return a.provider.DiscoverPeers(a.ctx)
}

// Close cancels the adapter context (aborting any in-flight DiscoverPeers
// calls) and delegates to the provider's Stop method with a bounded timeout
// so a stuck provider cannot block actor system shutdown indefinitely.
func (a *clusterProviderAdapter) Close() error {
	a.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return a.provider.Stop(ctx)
}
