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

package encryption

import (
	"context"
	"errors"
)

// ErrKeyNotFound is returned when the encryption key for a persistence ID has
// been deleted or was never created. This is the expected error after
// crypto-shredding has been performed.
var ErrKeyNotFound = errors.New("encryption key not found")

// KeyStore manages encryption keys per persistence ID. This is the component
// that enables crypto-shredding: deleting a key makes all events for that
// persistence ID irrecoverable.
type KeyStore interface {
	// GetOrCreateKey returns the current encryption key for the given persistence ID.
	// If no key exists, a new one is generated and stored.
	GetOrCreateKey(ctx context.Context, persistenceID string) (keyID string, key []byte, err error)
	// GetKey returns the encryption key identified by keyID.
	// Returns ErrKeyNotFound if the key has been deleted (crypto-shredding).
	GetKey(ctx context.Context, keyID string) (key []byte, err error)
	// DeleteKey permanently destroys the encryption key for the given persistence ID.
	// After this call, events encrypted with this key cannot be decrypted (crypto-shredding).
	DeleteKey(ctx context.Context, persistenceID string) error
}
