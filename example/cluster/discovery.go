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
	"github.com/tochemey/goakt/v4/discovery"
	"github.com/tochemey/goakt/v4/discovery/kubernetes"
)

// NewKubernetesProvider returns a goakt discovery.Provider that discovers
// cluster peers via the Kubernetes API. It delegates to goakt's built-in
// Kubernetes provider; the example keeps this thin wrapper purely as a
// documentation seam.
//
// Parameters:
//   - namespace: the Kubernetes namespace to search for pods.
//   - podLabels: label selector for matching ego cluster pods.
//   - discoveryPortName: the named container port used for gossip/discovery.
//   - remotingPortName: the named container port used for remote actor communication.
//   - peersPortName: the named container port used for peer-to-peer communication.
func NewKubernetesProvider(namespace string, podLabels map[string]string, discoveryPortName, remotingPortName, peersPortName string) discovery.Provider {
	return kubernetes.NewDiscovery(&kubernetes.Config{
		Namespace:         namespace,
		PodLabels:         podLabels,
		DiscoveryPortName: discoveryPortName,
		RemotingPortName:  remotingPortName,
		PeersPortName:     peersPortName,
	})
}
