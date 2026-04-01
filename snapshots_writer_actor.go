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
	"fmt"

	goakt "github.com/tochemey/goakt/v4/actor"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/encryption"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/persistence"
)

// persistSnapshotRequest is sent from the EventSourcedActor to the
// snapshotsWriterActor to persist a point-in-time snapshot of the entity state.
//
// The snapshot carries unencrypted state; encryption is handled by the writer
// so that crypto work runs off the parent's hot path.
//
// When a retention policy is active, retentionReq and janitor are set so the
// writer can forward the cleanup request after the snapshot is confirmed
// persisted. This eliminates the race where retention could delete old data
// before the new snapshot is safely written.
type persistSnapshotRequest struct {
	snapshot     *egopb.Snapshot
	retentionReq *applyRetentionRequest
	janitor      *goakt.PID
}

// snapshotsWriterActor persists snapshots to the snapshot store asynchronously.
// Snapshot persistence is an optimization for faster recovery and is not required
// for correctness. Failures are retried with exponential backoff; if all
// attempts fail, the error is logged but does not propagate to the parent actor
// or the command caller.
//
// When encryption is configured, the writer encrypts the snapshot state before
// writing, keeping crypto off the parent's command-processing path.
//
// After a successful write, the writer forwards any attached retention request
// to the janitor actor, guaranteeing retention only runs after the new snapshot
// is safely persisted.
//
// This actor is spawned as a child of the EventSourcedActor. It receives
// persistSnapshotRequest messages via Tell (fire-and-forget).
type snapshotsWriterActor struct {
	snapshotStore persistence.SnapshotStore
	encryptor     encryption.Encryptor
}

var _ goakt.Actor = (*snapshotsWriterActor)(nil)

// newSnapshotsWriterActor creates an instance of [snapshotsWriterActor].
func newSnapshotsWriterActor() *snapshotsWriterActor {
	return &snapshotsWriterActor{}
}

// PreStart loads the snapshot store and optional encryptor from the actor
// system extensions.
func (a *snapshotsWriterActor) PreStart(ctx *goakt.Context) error {
	if ext := ctx.Extension(extensions.SnapshotStoreExtensionID); ext != nil {
		a.snapshotStore = ext.(*extensions.SnapshotStoreExt).Underlying()
	}
	if ext := ctx.Extension(extensions.EncryptorExtensionID); ext != nil {
		a.encryptor = ext.(*extensions.EncryptorExtension).Encryptor()
	}
	return nil
}

// Receive handles incoming messages. Only persistSnapshotRequest is expected.
func (a *snapshotsWriterActor) Receive(ctx *goakt.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case *goakt.PostStart:
		// no-op
	case *persistSnapshotRequest:
		a.handlePersistSnapshot(ctx, msg)
	default:
		ctx.Unhandled()
	}
}

// PostStop performs cleanup when the actor is stopped.
func (a *snapshotsWriterActor) PostStop(_ *goakt.Context) error {
	return nil
}

// handlePersistSnapshot encrypts the snapshot state (if configured), writes
// it to the store with retry, and forwards the retention request to the
// janitor on success.
func (a *snapshotsWriterActor) handlePersistSnapshot(ctx *goakt.ReceiveContext, req *persistSnapshotRequest) {
	if a.snapshotStore == nil {
		return
	}

	snapshot := req.snapshot

	if a.encryptor != nil && snapshot.GetState() != nil {
		encrypted, err := a.encryptSnapshotState(ctx.Context(), snapshot)
		if err != nil {
			ctx.Logger().Errorf("failed to encrypt snapshot for %s at sequence %d: %s",
				snapshot.GetPersistenceId(), snapshot.GetSequenceNumber(), err)
			return
		}
		snapshot = encrypted
	}

	if err := retryWithBackoff(ctx.Context(), defaultMaxRetries, func() error {
		return a.snapshotStore.WriteSnapshot(ctx.Context(), snapshot)
	}); err != nil {
		ctx.Logger().Errorf("failed to persist snapshot for %s at sequence %d: %s",
			snapshot.GetPersistenceId(), snapshot.GetSequenceNumber(), err)
		return
	}

	if req.retentionReq != nil && req.janitor != nil {
		ctx.Tell(req.janitor, req.retentionReq)
	}
}

// encryptSnapshotState returns a copy of the snapshot with an encrypted state
// field. The original snapshot is not modified.
func (a *snapshotsWriterActor) encryptSnapshotState(ctx context.Context, snapshot *egopb.Snapshot) (*egopb.Snapshot, error) {
	stateBytes, err := proto.Marshal(snapshot.GetState())
	if err != nil {
		return nil, fmt.Errorf("failed to marshal state for encryption: %w", err)
	}

	ciphertext, keyID, err := a.encryptor.Encrypt(ctx, snapshot.GetPersistenceId(), stateBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt state: %w", err)
	}

	encrypted := proto.Clone(snapshot).(*egopb.Snapshot)
	encrypted.State = &anypb.Any{
		TypeUrl: snapshot.GetState().GetTypeUrl(),
		Value:   ciphertext,
	}
	encrypted.IsEncrypted = true
	encrypted.EncryptionKeyId = keyID
	return encrypted, nil
}
