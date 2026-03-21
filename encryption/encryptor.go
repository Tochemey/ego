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

import "context"

// Encryptor encrypts and decrypts event and snapshot payloads.
// Implementations must be safe for concurrent use.
type Encryptor interface {
	// Encrypt encrypts the raw bytes for the given persistence ID.
	// Returns the ciphertext and the key ID used for encryption.
	Encrypt(ctx context.Context, persistenceID string, plaintext []byte) (ciphertext []byte, keyID string, err error)
	// Decrypt decrypts the raw bytes for the given persistence ID.
	// The keyID identifies which key was used during encryption.
	Decrypt(ctx context.Context, persistenceID string, ciphertext []byte, keyID string) (plaintext []byte, err error)
}
