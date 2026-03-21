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

package testkit

import (
	"context"
	"crypto/rand"
	"sync"

	"github.com/google/uuid"

	"github.com/tochemey/ego/v4/encryption"
)

// KeyStore is an in-memory implementation of encryption.KeyStore for testing.
type KeyStore struct {
	mu   sync.RWMutex
	keys map[string][]byte // keyID -> key bytes
	pids map[string]string // persistenceID -> keyID
}

var _ encryption.KeyStore = (*KeyStore)(nil)

// NewKeyStore creates an in-memory key store for testing.
func NewKeyStore() *KeyStore {
	return &KeyStore{
		keys: make(map[string][]byte),
		pids: make(map[string]string),
	}
}

func (k *KeyStore) GetOrCreateKey(_ context.Context, persistenceID string) (string, []byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if keyID, ok := k.pids[persistenceID]; ok {
		return keyID, k.keys[keyID], nil
	}

	key := make([]byte, 32) // AES-256
	if _, err := rand.Read(key); err != nil {
		return "", nil, err
	}

	keyID := uuid.NewString()
	k.keys[keyID] = key
	k.pids[persistenceID] = keyID
	return keyID, key, nil
}

func (k *KeyStore) GetKey(_ context.Context, keyID string) ([]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	key, ok := k.keys[keyID]
	if !ok {
		return nil, encryption.ErrKeyNotFound
	}
	return key, nil
}

func (k *KeyStore) DeleteKey(_ context.Context, persistenceID string) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	keyID, ok := k.pids[persistenceID]
	if !ok {
		return nil
	}

	delete(k.keys, keyID)
	delete(k.pids, persistenceID)
	return nil
}
