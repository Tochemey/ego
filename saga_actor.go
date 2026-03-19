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
	"time"

	goakt "github.com/tochemey/goakt/v4/actor"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/persistence"
)

// sagaTimeoutMsg is an internal message sent when the saga timeout expires.
type sagaTimeoutMsg struct{}

// SagaActor implements a saga/process manager as a Go-Akt actor.
// It subscribes to the event stream, reacts to events via the SagaBehavior,
// persists its own events, and coordinates commands to other entities.
type SagaActor struct {
	behavior      SagaBehavior
	eventsStore   persistence.EventsStore
	eventsStream  eventstream.Stream
	subscriber    eventstream.Subscriber
	currentState  State
	eventsCounter uint64
	status        SagaStatus
	sagaID        string
	timeout       time.Duration

	// stopCh is used to stop the event consumption loop
	stopCh chan struct{}
}

// implements the goakt.Actor interface
var _ goakt.Actor = (*SagaActor)(nil)

// newSagaActor creates a new saga actor.
// No arguments are passed in the constructor to support cluster relocation.
// The SagaBehavior and timeout are passed via dependencies.
func newSagaActor() *SagaActor {
	return &SagaActor{
		stopCh: make(chan struct{}, 1),
	}
}

// PreStart initializes the saga actor: loads stores, recovers state, subscribes to events.
func (s *SagaActor) PreStart(ctx *goakt.Context) error {
	s.eventsStore = ctx.Extension(extensions.EventsStoreExtensionID).(*extensions.EventsStore).Underlying()
	s.eventsStream = ctx.Extension(extensions.EventsStreamExtensionID).(*extensions.EventsStream).Underlying()
	s.sagaID = ctx.ActorName()

	for _, dependency := range ctx.Dependencies() {
		if dependency == nil {
			continue
		}
		if behavior, ok := dependency.(SagaBehavior); ok {
			s.behavior = behavior
		}
		if cfg, ok := dependency.(*extensions.SagaConfig); ok {
			s.timeout = cfg.Timeout
		}
	}

	if s.behavior == nil {
		return fmt.Errorf("saga behavior is required")
	}

	if err := s.eventsStore.Ping(ctx.Context()); err != nil {
		return fmt.Errorf("saga events store ping failed: %w", err)
	}

	if err := s.recover(ctx.Context()); err != nil {
		return err
	}

	// Subscribe to event stream topics
	s.subscriber = s.eventsStream.AddSubscriber()
	partitionsCount := uint64(ctx.ActorSystem().Partition(s.sagaID)) + 1
	topics := generateTopics(eventsTopic, partitionsCount)
	for _, topic := range topics {
		s.eventsStream.Subscribe(s.subscriber, topic)
	}

	return nil
}

// Receive handles messages sent to the saga actor.
func (s *SagaActor) Receive(ctx *goakt.ReceiveContext) {
	switch ctx.Message().(type) {
	case *goakt.PostStart:
		// Start consuming events from the stream
		go s.consumeEvents(ctx)
		// Schedule timeout if configured
		if s.timeout > 0 {
			self := ctx.Self()
			_ = ctx.ActorSystem().ScheduleOnce(ctx.Context(), &sagaTimeoutMsg{}, self, s.timeout)
		}
	case *sagaTimeoutMsg:
		if s.status == SagaRunning {
			s.status = SagaCompensating
			s.compensate(ctx)
		}
	case *egopb.GetStateCommand:
		s.replyWithState(ctx)
	default:
		ctx.Unhandled()
	}
}

// PostStop cleans up the saga actor.
func (s *SagaActor) PostStop(_ *goakt.Context) error {
	close(s.stopCh)
	if s.subscriber != nil {
		s.subscriber.Shutdown()
	}
	return nil
}

// recover rebuilds the saga state from persisted events.
func (s *SagaActor) recover(ctx context.Context) error {
	s.currentState = s.behavior.InitialState()
	s.status = SagaRunning

	latestEvent, err := s.eventsStore.GetLatestEvent(ctx, s.sagaID)
	if err != nil {
		return fmt.Errorf("failed to get latest saga event: %w", err)
	}

	if latestEvent == nil {
		return nil
	}

	latestSeqNr := latestEvent.GetSequenceNumber()
	events, err := s.eventsStore.ReplayEvents(ctx, s.sagaID, 1, latestSeqNr, latestSeqNr)
	if err != nil {
		return fmt.Errorf("failed to replay saga events: %w", err)
	}

	for _, envelope := range events {
		eventMsg, err := envelope.GetEvent().UnmarshalNew()
		if err != nil {
			return fmt.Errorf("failed to unmarshal saga event at sequence %d: %w", envelope.GetSequenceNumber(), err)
		}

		s.currentState, err = s.behavior.ApplyEvent(ctx, eventMsg.(Event), s.currentState)
		if err != nil {
			return fmt.Errorf("failed to apply saga event at sequence %d: %w", envelope.GetSequenceNumber(), err)
		}
	}

	s.eventsCounter = latestSeqNr
	return nil
}

// consumeEvents reads events from the stream and processes them via the behavior.
func (s *SagaActor) consumeEvents(ctx *goakt.ReceiveContext) {
	for {
		select {
		case <-s.stopCh:
			return
		case message := <-s.subscriber.Iterator():
			if message == nil {
				continue
			}
			if s.status != SagaRunning {
				continue
			}

			event, ok := message.Payload().(*egopb.Event)
			if !ok {
				continue
			}

			// Skip our own saga events
			if event.GetPersistenceId() == s.sagaID {
				continue
			}

			eventMsg, err := event.GetEvent().UnmarshalNew()
			if err != nil {
				ctx.Logger().Errorf("saga %s: failed to unmarshal event: %v", s.sagaID, err)
				continue
			}

			action, err := s.behavior.HandleEvent(ctx.Context(), eventMsg.(Event), s.currentState)
			if err != nil {
				ctx.Logger().Errorf("saga %s: HandleEvent failed: %v", s.sagaID, err)
				continue
			}

			s.processAction(ctx, action)
		}
	}
}

// processAction executes a SagaAction: persists saga events, sends commands, and handles completion/compensation.
func (s *SagaActor) processAction(ctx *goakt.ReceiveContext, action *SagaAction) {
	if action == nil {
		return
	}

	// Persist saga events
	if len(action.Events) > 0 {
		if err := s.persistAndApplyEvents(ctx.Context(), action.Events); err != nil {
			ctx.Logger().Errorf("saga %s: failed to persist events: %v", s.sagaID, err)
			return
		}
	}

	// Send commands to entities
	for _, cmd := range action.Commands {
		s.sendCommand(ctx, cmd)
	}

	// Handle completion
	if action.Complete {
		s.status = SagaCompleted
		return
	}

	// Handle compensation
	if action.Compensate {
		s.status = SagaCompensating
		s.compensate(ctx)
	}
}

// persistAndApplyEvents persists saga events and applies them to the saga state.
func (s *SagaActor) persistAndApplyEvents(ctx context.Context, events []Event) error {
	var envelopes []*egopb.Event
	for _, event := range events {
		s.eventsCounter++
		eventAny, _ := anypb.New(event)
		envelope := &egopb.Event{
			PersistenceId:  s.sagaID,
			SequenceNumber: s.eventsCounter,
			IsDeleted:      false,
			Event:          eventAny,
			Timestamp:      time.Now().Unix(),
		}
		envelopes = append(envelopes, envelope)

		newState, err := s.behavior.ApplyEvent(ctx, event, s.currentState)
		if err != nil {
			return fmt.Errorf("failed to apply saga event: %w", err)
		}
		s.currentState = newState
	}

	return s.eventsStore.WriteEvents(ctx, envelopes)
}

// sendCommand sends a command to an entity and handles the result.
func (s *SagaActor) sendCommand(ctx *goakt.ReceiveContext, cmd SagaCommand) {
	timeout := cmd.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	noSender := ctx.ActorSystem().NoSender()
	reply, err := noSender.SendSync(ctx.Context(), cmd.EntityID, cmd.Command, timeout)
	if err != nil {
		action, handleErr := s.behavior.HandleError(ctx.Context(), cmd.EntityID, err, s.currentState)
		if handleErr != nil {
			ctx.Logger().Errorf("saga %s: HandleError failed for entity %s: %v", s.sagaID, cmd.EntityID, handleErr)
			return
		}
		s.processAction(ctx, action)
		return
	}

	// Parse the reply
	commandReply, ok := reply.(*egopb.CommandReply)
	if !ok {
		ctx.Logger().Errorf("saga %s: unexpected reply type from entity %s", s.sagaID, cmd.EntityID)
		return
	}

	resultState, _, err := parseCommandReply(commandReply)
	if err != nil {
		action, handleErr := s.behavior.HandleError(ctx.Context(), cmd.EntityID, err, s.currentState)
		if handleErr != nil {
			ctx.Logger().Errorf("saga %s: HandleError failed for entity %s: %v", s.sagaID, cmd.EntityID, handleErr)
			return
		}
		s.processAction(ctx, action)
		return
	}

	action, err := s.behavior.HandleResult(ctx.Context(), cmd.EntityID, resultState, s.currentState)
	if err != nil {
		ctx.Logger().Errorf("saga %s: HandleResult failed for entity %s: %v", s.sagaID, cmd.EntityID, err)
		return
	}
	s.processAction(ctx, action)
}

// compensate executes the compensation logic defined by the behavior.
func (s *SagaActor) compensate(ctx *goakt.ReceiveContext) {
	commands, err := s.behavior.Compensate(ctx.Context(), s.currentState)
	if err != nil {
		ctx.Logger().Errorf("saga %s: Compensate failed: %v", s.sagaID, err)
		s.status = SagaFailed
		return
	}

	for _, cmd := range commands {
		timeout := cmd.Timeout
		if timeout == 0 {
			timeout = 5 * time.Second
		}
		noSender := ctx.ActorSystem().NoSender()
		if _, err := noSender.SendSync(ctx.Context(), cmd.EntityID, cmd.Command, timeout); err != nil {
			ctx.Logger().Errorf("saga %s: compensation command to %s failed: %v", s.sagaID, cmd.EntityID, err)
			s.status = SagaFailed
			return
		}
	}

	s.status = SagaCompleted
}

// replyWithState replies with the saga's current state.
func (s *SagaActor) replyWithState(ctx *goakt.ReceiveContext) {
	state, _ := anypb.New(s.currentState)
	reply := &egopb.CommandReply{
		Reply: &egopb.CommandReply_StateReply{
			StateReply: &egopb.StateReply{
				PersistenceId:  s.sagaID,
				State:          state,
				SequenceNumber: s.eventsCounter,
			},
		},
	}
	ctx.Response(reply)
}
