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

package main

import (
	"context"

	"github.com/tochemey/goakt/v4/discovery/kubernetes"

	"github.com/tochemey/ego/v4"
)

// KubernetesProvider implements ego.ClusterProvider by wrapping goakt's
// Kubernetes discovery. It uses the Kubernetes API to list pods with
// matching labels and extract their discovery port addresses.
type KubernetesProvider struct {
	inner *kubernetes.Discovery
}

var _ ego.ClusterProvider = (*KubernetesProvider)(nil)

// NewKubernetesProvider creates a provider that discovers cluster peers
// via the Kubernetes API.
//
// Parameters:
//   - namespace: the Kubernetes namespace to search for pods.
//   - podLabels: label selector for matching ego cluster pods.
//   - discoveryPortName: the named container port used for gossip/discovery.
//   - remotingPortName: the named container port used for remote actor communication.
//   - peersPortName: the named container port used for peer-to-peer communication.
func NewKubernetesProvider(namespace string, podLabels map[string]string, discoveryPortName, remotingPortName, peersPortName string) *KubernetesProvider {
	config := &kubernetes.Config{
		Namespace:         namespace,
		PodLabels:         podLabels,
		DiscoveryPortName: discoveryPortName,
		RemotingPortName:  remotingPortName,
		PeersPortName:     peersPortName,
	}
	return &KubernetesProvider{
		inner: kubernetes.NewDiscovery(config),
	}
}

func (p *KubernetesProvider) ID() string {
	return p.inner.ID()
}

// Start initializes the discovery provider and registers the current node.
func (p *KubernetesProvider) Start(_ context.Context) error {
	if err := p.inner.Initialize(); err != nil {
		return err
	}
	return p.inner.Register()
}

// DiscoverPeers returns the list of peer "host:port" addresses.
func (p *KubernetesProvider) DiscoverPeers(_ context.Context) ([]string, error) {
	return p.inner.DiscoverPeers()
}

// Stop deregisters the current node and releases resources.
func (p *KubernetesProvider) Stop(_ context.Context) error {
	if err := p.inner.Deregister(); err != nil {
		return err
	}
	return p.inner.Close()
}
