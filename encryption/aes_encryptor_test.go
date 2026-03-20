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

package encryption_test

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/ego/v4/encryption"
)

// testKeyStore is an in-memory key store for testing
type testKeyStore struct {
	keys map[string][]byte
	pids map[string]string
}

func newTestKeyStore() *testKeyStore {
	return &testKeyStore{
		keys: make(map[string][]byte),
		pids: make(map[string]string),
	}
}

func (k *testKeyStore) GetOrCreateKey(_ context.Context, persistenceID string) (string, []byte, error) {
	if keyID, ok := k.pids[persistenceID]; ok {
		return keyID, k.keys[keyID], nil
	}
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	keyID := uuid.NewString()
	k.keys[keyID] = key
	k.pids[persistenceID] = keyID
	return keyID, key, nil
}

func (k *testKeyStore) GetKey(_ context.Context, keyID string) ([]byte, error) {
	key, ok := k.keys[keyID]
	if !ok {
		return nil, encryption.ErrKeyNotFound
	}
	return key, nil
}

func (k *testKeyStore) DeleteKey(_ context.Context, persistenceID string) error {
	keyID, ok := k.pids[persistenceID]
	if !ok {
		return nil
	}
	delete(k.keys, keyID)
	delete(k.pids, persistenceID)
	return nil
}

func TestAESEncryptor_EncryptDecryptRoundTrip(t *testing.T) {
	ctx := context.TODO()
	ks := newTestKeyStore()
	enc := encryption.NewAESEncryptor(ks)

	plaintext := []byte("hello world, this is a test message")

	ciphertext, keyID, err := enc.Encrypt(ctx, "entity-1", plaintext)
	require.NoError(t, err)
	require.NotEmpty(t, ciphertext)
	require.NotEmpty(t, keyID)

	decrypted, err := enc.Decrypt(ctx, "entity-1", ciphertext, keyID)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decrypted)
}

func TestAESEncryptor_EncryptProducesDifferentCiphertext(t *testing.T) {
	ctx := context.TODO()
	ks := newTestKeyStore()
	enc := encryption.NewAESEncryptor(ks)

	plaintext := []byte("same message")

	ct1, _, err := enc.Encrypt(ctx, "entity-1", plaintext)
	require.NoError(t, err)

	ct2, _, err := enc.Encrypt(ctx, "entity-1", plaintext)
	require.NoError(t, err)

	assert.NotEqual(t, ct1, ct2, "ciphertext should differ due to random nonce")
}

func TestAESEncryptor_DecryptWithWrongKeyID(t *testing.T) {
	ctx := context.TODO()
	ks := newTestKeyStore()
	enc := encryption.NewAESEncryptor(ks)

	plaintext := []byte("secret data")
	ciphertext, _, err := enc.Encrypt(ctx, "entity-1", plaintext)
	require.NoError(t, err)

	_, err = enc.Decrypt(ctx, "entity-1", ciphertext, "wrong-key-id")
	require.Error(t, err)
}

func TestAESEncryptor_DecryptShortCiphertext(t *testing.T) {
	ctx := context.TODO()
	ks := newTestKeyStore()
	enc := encryption.NewAESEncryptor(ks)

	// ensure a key exists
	_, _, err := ks.GetOrCreateKey(ctx, "entity-1")
	require.NoError(t, err)

	// encrypt to get a valid keyID
	_, keyID, err := enc.Encrypt(ctx, "entity-1", []byte("data"))
	require.NoError(t, err)

	_, err = enc.Decrypt(ctx, "entity-1", []byte("short"), keyID)
	require.Error(t, err)
}
