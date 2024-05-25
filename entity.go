/*
 * MIT License
 *
 * Copyright (c) 2022-2024 Tochemey
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ego

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/tochemey/goakt/v2/actors"

	"github.com/tochemey/ego/v2/egopb"
)

var (
	// ErrEngineRequired is returned when the eGo engine is not set
	ErrEngineRequired = errors.New("eGo engine is not defined")
	// ErrEngineNotStarted is returned when the eGo engine has not started
	ErrEngineNotStarted = errors.New("eGo engine has not started")
	// ErrUndefinedEntity is returned when sending a command to an undefined entity
	ErrUndefinedEntity = errors.New("eGo entity is not defined")
)

// Entity defines the event sourced persistent entity
// This handles commands in order
type Entity[T State] struct {
	actor actors.PID
}

// NewEntity creates an instance of Entity
func NewEntity[T State](ctx context.Context, behavior EntityBehavior[T], engine *Engine) (*Entity[T], error) {
	if engine == nil {
		return nil, ErrEngineRequired
	}

	if !engine.started.Load() {
		return nil, ErrEngineNotStarted
	}

	pid, err := engine.actorSystem.Spawn(ctx, behavior.ID(), newActor(behavior, engine.eventsStore, engine.eventStream))
	if err != nil {
		return nil, err
	}
	return &Entity[T]{
		actor: pid,
	}, nil
}

// SendCommand sends command to a given entity ref. This will return:
// 1. the resulting state after the command has been handled and the emitted event persisted
// 2. nil when there is no resulting state or no event persisted
// 3. an error in case of error
func (x Entity[T]) SendCommand(ctx context.Context, command Command) (resultingState T, revision uint64, err error) {
	var nilOfT T

	if x.actor == nil || !x.actor.IsRunning() {
		return nilOfT, 0, ErrUndefinedEntity
	}

	reply, err := actors.Ask(ctx, x.actor, command, time.Second)
	if err != nil {
		return nilOfT, 0, err
	}

	commandReply, ok := reply.(*egopb.CommandReply)
	if ok {
		return parseCommandReply[T](commandReply)
	}

	return nilOfT, 0, errors.New("failed to parse command reply")
}

// parseCommandReply parses the command reply
func parseCommandReply[T State](reply *egopb.CommandReply) (T, uint64, error) {
	var (
		state T
		err   error
	)

	switch r := reply.GetReply().(type) {
	case *egopb.CommandReply_StateReply:
		msg, err := r.StateReply.GetState().UnmarshalNew()
		if err != nil {
			return state, 0, err
		}

		switch v := msg.(type) {
		case T:
			return v, r.StateReply.GetSequenceNumber(), nil
		default:
			return state, 0, fmt.Errorf("got %s", r.StateReply.GetState().GetTypeUrl())
		}
	case *egopb.CommandReply_ErrorReply:
		err = errors.New(r.ErrorReply.GetMessage())
		return state, 0, err
	}
	return state, 0, errors.New("no state received")
}
