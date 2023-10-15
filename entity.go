/*
 * Copyright (c) 2022-2023 Tochemey
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
	"github.com/tochemey/ego/egopb"
	"github.com/tochemey/goakt/actors"
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
	// check whether the ego engine is defined
	if engine == nil {
		return nil, ErrEngineRequired
	}
	// check whether the eGo engine has started or not
	if !engine.started.Load() {
		return nil, ErrEngineNotStarted
	}

	// create the instance of the actor
	pid, err := engine.actorSystem.Spawn(ctx, behavior.ID(), newActor(behavior, engine.eventsStore))
	// return the error in case there is one
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
	// define a nil state
	var nilT T

	// check whether the underlying actor is set and running
	if x.actor == nil || !x.actor.IsRunning() {
		return nilT, 0, ErrUndefinedEntity
	}

	// send the command to the actor
	reply, err := actors.Ask(ctx, x.actor, command, time.Second)
	// handle the error
	if err != nil {
		return nilT, 0, err
	}

	// cast the reply to a command reply because that is the expected return type
	commandReply, ok := reply.(*egopb.CommandReply)
	// when casting is successful
	if ok {
		// parse the command reply and return the appropriate responses
		return parseCommandReply[T](commandReply)
	}
	// casting failed
	return nilT, 0, errors.New("failed to parse command reply")
}

// parseCommandReply parses the command reply
func parseCommandReply[T State](reply *egopb.CommandReply) (T, uint64, error) {
	var (
		state T
		err   error
	)
	// parse the command reply
	switch r := reply.GetReply().(type) {
	case *egopb.CommandReply_StateReply:
		// unmarshal the state
		msg, err := r.StateReply.GetState().UnmarshalNew()
		// return the error in case there is one
		if err != nil {
			return state, 0, err
		}

		// unpack the state properly
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
