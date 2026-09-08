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
	"sync/atomic"
	"time"

	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/offsetstore"
	"github.com/tochemey/ego/v4/persistence"
	"github.com/tochemey/ego/v4/projection"
)

const (
	// defaultSagaCommandTimeout is how long a saga waits for a participant reply
	// when the SagaCommand does not set a timeout of its own.
	defaultSagaCommandTimeout = 5 * time.Second
	// sagaEventTimeout is how long the journal runner waits for the saga to
	// acknowledge an event before the delivery is retried.
	sagaEventTimeout = 30 * time.Second
	// sagaRunnerNamePrefix prefixes the saga ID to name the journal runner
	// that feeds it, and with it the offset rows recording its progress.
	sagaRunnerNamePrefix = "ego.saga."
	// sagaRunnerRetries is how many times the journal runner redelivers an
	// event the saga could not acknowledge before it fails the saga.
	sagaRunnerRetries = 5
	// sagaRunnerRetryDelay is the delay between two redeliveries of the same
	// event to the saga.
	sagaRunnerRetryDelay = time.Second
)

// sagaTimeoutMsg is an internal message sent when the saga timeout expires.
type sagaTimeoutMsg struct{}

// sagaCommandResult carries the outcome of a participant call back to the saga
// mailbox. The call itself runs off the actor goroutine, so the saga stays
// responsive while a participant is slow, and the reply is still handled on the
// actor's serialized message loop.
type sagaCommandResult struct {
	// entityID is the participant the command was sent to.
	entityID string
	// reply is whatever the participant answered. It is nil when err is set.
	reply any
	// err is the failure the call returned, if any.
	err error
	// compensation reports whether the call was a compensation command.
	compensation bool
}

// SagaActor implements a saga/process manager as a Go-Akt actor.
// It reads the journal, reacts to events via the SagaBehavior, persists its
// own events, and coordinates commands to other entities.
type SagaActor struct {
	behavior      SagaBehavior
	eventsStore   persistence.EventsStore
	eventsStream  eventstream.Stream
	offsetStore   offsetstore.OffsetStore
	currentState  State
	eventsCounter uint64
	status        SagaStatus
	sagaID        string
	timeout       time.Duration

	// runner reads the journal on the saga's behalf and hands every event
	// written since the saga started to eventHandler. It is the saga's only
	// source of events; the local stream merely nudges it awake.
	runner *projectionRunner

	// eventHandler feeds the runner's events into the saga mailbox. It is
	// held here so PostStart can give it the saga's own PID.
	eventHandler *sagaEventHandler

	// firstStart reports that the saga has no journaled start yet, so its
	// start time and its running status must be recorded once it is up.
	firstStart bool

	// shard is the journal shard the saga's own events belong to. It is
	// computed once in PostStart from the saga ID, the same way entities
	// derive theirs, so saga events land on the shard that owns the saga.
	shard uint64

	// startedAt is the wall clock time, in nanoseconds, at which the saga
	// first moved to SagaRunning. It is journaled with every status
	// transition so a restarted saga keeps its original deadline.
	startedAt int64

	// pendingCompensations counts the compensation commands of the current
	// round that have not answered yet. The saga settles its final status
	// when the count reaches zero.
	pendingCompensations int

	// compensationFailed reports whether any compensation of the current
	// round failed, so the saga can settle on SagaFailed once every
	// compensation has answered.
	compensationFailed bool

	// actorSystem and logger are stored during PostStart so that code running
	// outside a receive turn can use them safely after the ReceiveContext
	// from PostStart has been returned to the pool.
	actorSystem goakt.ActorSystem
	logger      log.Logger
}

// implements the goakt.Actor interface
var _ goakt.Actor = (*SagaActor)(nil)

// newSagaActor creates a new saga actor.
// No arguments are passed in the constructor to support cluster relocation.
// The SagaBehavior and timeout are passed via dependencies.
func newSagaActor() *SagaActor {
	return new(SagaActor)
}

// PreStart initializes the saga actor: loads stores, recovers state, and
// prepares the runner that feeds it the journal.
func (s *SagaActor) PreStart(ctx *goakt.Context) error {
	eventsStore, err := requiredEventsStore(ctx)
	if err != nil {
		return err
	}

	eventsStream, err := requiredEventsStream(ctx)
	if err != nil {
		return err
	}

	offsetStore, err := requiredOffsetStore(ctx)
	if err != nil {
		return err
	}

	s.eventsStore = eventsStore
	s.eventsStream = eventsStream
	s.offsetStore = offsetStore
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

	// A saga that has never run yet stamps its start now: the runner resumes
	// the journal from it, so the saga only ever sees the events of the
	// process it was started to coordinate.
	s.firstStart = s.startedAt == 0
	if s.firstStart {
		s.startedAt = time.Now().UnixNano()
	}

	s.eventHandler = &sagaEventHandler{sagaID: s.sagaID}
	s.runner = newProjectionRunner(sagaRunnerNamePrefix+s.sagaID, s.eventHandler, s.eventsStore, s.offsetStore, s.runnerOptions(ctx)...)

	// context.Background() is used instead of ctx.Context() because PreStart's
	// context is ephemeral — goakt wraps it in context.WithTimeout and cancels
	// it immediately after PreStart returns — while the runner ties the life
	// of its workers to the context it is started with.
	return s.runner.Start(context.Background())
}

// runnerOptions builds the settings of the runner that feeds the saga: it
// resumes the journal at the saga's start time, is woken by the events
// persisted on this node, and retries a delivery the saga could not
// acknowledge before it gives up and fails the saga.
func (s *SagaActor) runnerOptions(ctx *goakt.Context) []runnerOption {
	opts := []runnerOption{
		withLogger(ctx.ActorSystem().Logger()),
		withStartOffset(time.Unix(0, s.startedAt)),
		withEventsStream(s.eventsStream),
		withRecoveryStrategy(projection.NewRecovery(
			projection.WithRecoveryPolicy(projection.RetryAndFail),
			projection.WithRetries(sagaRunnerRetries),
			projection.WithRetryDelay(sagaRunnerRetryDelay),
		)),
	}

	if ext := ctx.Extension(extensions.EventAdaptersExtensionID); ext != nil {
		opts = append(opts, withEventAdapters(ext.(*extensions.EventAdapters).Adapters()))
	}

	if ext := ctx.Extension(extensions.EncryptorExtensionID); ext != nil {
		opts = append(opts, withEncryptor(ext.(*extensions.EncryptorExtension).Encryptor()))
	}

	return opts
}

// Receive handles messages sent to the saga actor.
//
// All saga state (currentState, eventsCounter, status) is read and written
// exclusively from here: the runner hands journaled events to the actor's own
// mailbox instead of processing them itself, so the actor's serialized message
// loop is the only writer.
func (s *SagaActor) Receive(ctx *goakt.ReceiveContext) {
	switch message := ctx.Message().(type) {
	case *goakt.PostStart:
		// Capture stable references before the ReceiveContext is returned to the pool.
		s.actorSystem = ctx.ActorSystem()
		s.logger = ctx.Logger()
		s.shard = ctx.ActorSystem().Partition(s.sagaID)

		// Hand the actor PID to the runner and its handler before the journal
		// is read: the handler delivers through it, and a runner that dies on
		// an unprocessable event reports the failure back through it.
		s.eventHandler.pid.Store(ctx.Self())
		s.runner.pid = ctx.Self()

		// A saga that has never run yet journals its start so the deadline
		// and the status survive a restart or a relocation.
		if s.firstStart {
			s.recordStatus(ctx.Context(), SagaRunning)
		}

		s.runner.Run(ctx.Context())
		s.armTimeout(ctx)
	case *egopb.Event:
		s.handleJournalEvent(ctx, message)

		// The runner waits for this acknowledgement before it records the
		// progress of the saga. The response is a no-op when the event was
		// told rather than asked.
		ctx.Response(new(egopb.NoReply))
	case *runnerFailed:
		s.handleRunnerFailure(ctx, message.err)
	case *sagaCommandResult:
		s.handleCommandResult(ctx, message)
	case *sagaTimeoutMsg:
		if s.status == SagaRunning {
			s.recordStatus(ctx.Context(), SagaCompensating)
			s.compensate(ctx)
		}
	case *egopb.GetStateCommand:
		s.replyWithState(ctx)
	case *egopb.GetSagaStatus:
		s.replyWithStatus(ctx)
	default:
		ctx.Unhandled()
	}
}

// PostStop cleans up the saga actor.
func (s *SagaActor) PostStop(ctx *goakt.Context) error {
	// Stop is idempotent: a saga that already settled stopped its runner then.
	if s.runner != nil {
		_ = s.runner.Stop()
	}

	// Release the timeout schedule so a restarted saga can arm it again with
	// the time its deadline has left.
	s.cancelTimeout(ctx.ActorSystem())
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

		// Status transitions are eGo's own bookkeeping: they carry the saga
		// status and start time, never a domain change for the behavior.
		if statusChanged, ok := eventMsg.(*egopb.SagaStatusChanged); ok {
			s.status = SagaStatus(statusChanged.GetStatus())
			s.startedAt = statusChanged.GetStartedAt()
			continue
		}

		s.currentState, err = s.behavior.ApplyEvent(ctx, eventMsg, s.currentState)
		if err != nil {
			return fmt.Errorf("failed to apply saga event at sequence %d: %w", envelope.GetSequenceNumber(), err)
		}
	}

	s.eventsCounter = latestSeqNr
	return nil
}

// sagaEventHandler hands the events read from the journal to a saga.
//
// It runs on the runner's goroutines, so it delivers every event through the
// saga mailbox rather than touching saga state: the actor's serialized message
// loop stays the only writer. The delivery is an ask, so the runner records the
// saga's progress only once the saga has processed the event.
type sagaEventHandler struct {
	// sagaID is the persistence ID of the saga being fed. The saga's own
	// events are its private bookkeeping and are never handed back to it.
	sagaID string
	// pid is the saga actor to deliver to. It is set once the saga is up,
	// before the runner reads the journal.
	pid atomic.Pointer[goakt.PID]
}

// implements the projection handler contract
var _ projection.Handler = (*sagaEventHandler)(nil)

// Handle delivers one journaled event to the saga and waits for its
// acknowledgement. The returned error makes the runner redeliver the event,
// so nothing the saga did not accept is marked as processed.
//
// A cancelled context is how the runner winds down once the saga has settled.
// It is reported at once, so the runner's retries do not keep pushing the same
// event into a saga that no longer reads the journal.
func (h *sagaEventHandler) Handle(ctx context.Context, persistenceID string, event *anypb.Any, revision uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if persistenceID == h.sagaID {
		return nil
	}

	_, err := goakt.Ask(ctx, h.pid.Load(), &egopb.Event{
		PersistenceId:  persistenceID,
		SequenceNumber: revision,
		Event:          event,
	}, sagaEventTimeout)

	return err
}

// handleRunnerFailure records that the journal runner stopped for good.
//
// Only a running saga is failed by it: without the journal it can no longer
// follow the process it coordinates. A saga that is compensating settles on
// the outcome of its compensations, and a settled saga keeps its status.
func (s *SagaActor) handleRunnerFailure(ctx *goakt.ReceiveContext, err error) {
	s.logger.Errorf("saga %s: the journal runner stopped: %v", s.sagaID, err)

	if s.status != SagaRunning {
		return
	}

	s.recordStatus(ctx.Context(), SagaFailed)
}

// handleJournalEvent processes an event the runner read from the journal.
// It runs on the actor's message loop, so it can freely touch saga state.
func (s *SagaActor) handleJournalEvent(ctx *goakt.ReceiveContext, event *egopb.Event) {
	if s.status != SagaRunning {
		return
	}

	// A transient handler error must not lose the event: the saga acknowledges
	// it once this returns, so the error is retried here and only an exhausted
	// retry moves the saga to SagaFailed.
	var action *SagaAction
	err := retryWithBackoff(ctx.Context(), defaultMaxRetries, func() error {
		eventMsg, unmarshalErr := event.GetEvent().UnmarshalNew()
		if unmarshalErr != nil {
			return fmt.Errorf("failed to unmarshal event: %w", unmarshalErr)
		}

		var handleErr error
		action, handleErr = s.behavior.HandleEvent(context.Background(), eventMsg, s.currentState)
		return handleErr
	})
	if err != nil {
		s.logger.Errorf("saga %s: HandleEvent failed: %v", s.sagaID, err)
		s.recordStatus(ctx.Context(), SagaFailed)
		return
	}

	s.processAction(ctx, action)
}

// processAction executes a SagaAction: persists saga events, sends commands, and handles completion/compensation.
func (s *SagaActor) processAction(ctx *goakt.ReceiveContext, action *SagaAction) {
	if action == nil {
		return
	}

	// Persist saga events
	if len(action.Events) > 0 {
		err := retryWithBackoff(ctx.Context(), defaultMaxRetries, func() error {
			return s.persistAndApplyEvents(context.Background(), action.Events)
		})
		if err != nil {
			s.logger.Errorf("saga %s: failed to persist events: %v", s.sagaID, err)
			s.recordStatus(ctx.Context(), SagaFailed)
			return
		}
	}

	// Send commands to entities
	for _, cmd := range action.Commands {
		s.askParticipant(ctx, cmd, false)
	}

	// Handle completion
	if action.Complete {
		s.recordStatus(ctx.Context(), SagaCompleted)
		return
	}

	// Handle compensation
	if action.Compensate {
		s.recordStatus(ctx.Context(), SagaCompensating)
		s.compensate(ctx)
	}
}

// persistAndApplyEvents persists saga events and applies them to the saga state.
//
// The events are applied to a local copy of the state and a local copy of the
// sequence counter; both are committed to the saga only once the journal write
// succeeds. A failed write therefore leaves the saga exactly as the journal has
// it, so the next write continues the sequence without a gap.
func (s *SagaActor) persistAndApplyEvents(ctx context.Context, events []Event) error {
	pendingState := s.currentState
	pendingCounter := s.eventsCounter
	envelopes := make([]*egopb.Event, 0, len(events))

	for _, event := range events {
		eventAny, err := anypb.New(event)
		if err != nil {
			return fmt.Errorf("failed to marshal saga event: %w", err)
		}

		pendingCounter++
		envelopes = append(envelopes, &egopb.Event{
			PersistenceId:  s.sagaID,
			SequenceNumber: pendingCounter,
			IsDeleted:      false,
			Event:          eventAny,
			Timestamp:      time.Now().UnixNano(),
			Shard:          s.shard,
		})

		newState, err := s.behavior.ApplyEvent(ctx, event, pendingState)
		if err != nil {
			return fmt.Errorf("failed to apply saga event: %w", err)
		}

		pendingState = newState
	}

	if err := s.eventsStore.WriteEvents(ctx, envelopes); err != nil {
		return err
	}

	s.currentState = pendingState
	s.eventsCounter = pendingCounter
	return nil
}

// askParticipant runs a participant call off the actor goroutine and pipes its
// outcome back to the saga mailbox as a sagaCommandResult. The saga therefore
// keeps answering status queries, stream events and its own timeout while a
// participant is slow.
func (s *SagaActor) askParticipant(ctx *goakt.ReceiveContext, cmd SagaCommand, compensation bool) {
	timeout := cmd.Timeout
	if timeout == 0 {
		timeout = defaultSagaCommandTimeout
	}

	// Everything the task needs is captured now: the ReceiveContext goes back
	// to the pool as soon as this message is handled.
	entityID := cmd.EntityID
	command := cmd.Command
	noSender := ctx.ActorSystem().NoSender()

	ctx.PipeTo(ctx.Self(), func() (any, error) {
		reply, err := noSender.SendSync(context.Background(), entityID, command, timeout)
		return &sagaCommandResult{
			entityID:     entityID,
			reply:        reply,
			err:          err,
			compensation: compensation,
		}, nil
	})
}

// handleCommandResult feeds a participant reply to the saga behavior on the
// actor's message loop and processes whatever action it returns.
//
// Replies arrive asynchronously, so one can land after the saga has already
// moved on: completed, failed, or started compensating. Such a reply belongs to
// a step the saga has left and is dropped; feeding it to the behavior could
// drive a finished saga into new commands or settle a compensation round early.
func (s *SagaActor) handleCommandResult(ctx *goakt.ReceiveContext, result *sagaCommandResult) {
	if result.compensation {
		if s.status != SagaCompensating {
			s.logger.Debugf("saga %s: ignoring a compensation reply from %s received while %s", s.sagaID, result.entityID, s.status)
			return
		}

		s.handleCompensationResult(ctx, result)
		return
	}

	if s.status != SagaRunning {
		s.logger.Debugf("saga %s: ignoring a reply from %s received while %s", s.sagaID, result.entityID, s.status)
		return
	}

	var action *SagaAction

	resultState, err := participantState(result)
	if err != nil {
		handleErr := retryWithBackoff(ctx.Context(), defaultMaxRetries, func() error {
			var behaviorErr error
			action, behaviorErr = s.behavior.HandleError(context.Background(), result.entityID, err, s.currentState)
			return behaviorErr
		})
		if handleErr != nil {
			s.logger.Errorf("saga %s: HandleError failed for entity %s: %v", s.sagaID, result.entityID, handleErr)
			s.recordStatus(ctx.Context(), SagaFailed)
			return
		}

		s.processAction(ctx, action)
		return
	}

	handleErr := retryWithBackoff(ctx.Context(), defaultMaxRetries, func() error {
		var behaviorErr error
		action, behaviorErr = s.behavior.HandleResult(context.Background(), result.entityID, resultState, s.currentState)
		return behaviorErr
	})
	if handleErr != nil {
		s.logger.Errorf("saga %s: HandleResult failed for entity %s: %v", s.sagaID, result.entityID, handleErr)
		s.recordStatus(ctx.Context(), SagaFailed)
		return
	}

	s.processAction(ctx, action)
}

// participantState returns the state a participant replied with, or the error
// that makes the call a failure: a transport error, a reply that is not a
// command reply, or an error reply.
func participantState(result *sagaCommandResult) (State, error) {
	if result.err != nil {
		return nil, result.err
	}

	commandReply, ok := result.reply.(*egopb.CommandReply)
	if !ok {
		return nil, fmt.Errorf("unexpected reply type %T from entity %s", result.reply, result.entityID)
	}

	state, _, err := parseCommandReply(commandReply)
	if err != nil {
		return nil, err
	}

	return state, nil
}

// compensate executes the compensation logic defined by the behavior.
//
// Every compensation command is issued, even when one of them fails: a
// participant must not be left uncompensated because an earlier one could not
// be reached. The outcomes come back through the mailbox and the saga settles
// its status once the last one has answered.
func (s *SagaActor) compensate(ctx *goakt.ReceiveContext) {
	commands, err := s.behavior.Compensate(context.Background(), s.currentState)
	if err != nil {
		s.logger.Errorf("saga %s: Compensate failed: %v", s.sagaID, err)
		s.recordStatus(ctx.Context(), SagaFailed)
		return
	}

	if len(commands) == 0 {
		s.recordStatus(ctx.Context(), SagaCompleted)
		return
	}

	s.pendingCompensations = len(commands)
	s.compensationFailed = false

	for _, cmd := range commands {
		s.askParticipant(ctx, cmd, true)
	}
}

// handleCompensationResult records one compensation outcome and settles the
// saga status once every compensation of the round has answered.
//
// A compensation fails when the participant could not be reached and also when
// it answered with an error reply: a rejected compensation leaves the
// participant uncompensated just as an unreachable one does.
func (s *SagaActor) handleCompensationResult(ctx *goakt.ReceiveContext, result *sagaCommandResult) {
	if _, err := participantState(result); err != nil {
		s.logger.Errorf("saga %s: compensation command to %s failed: %v", s.sagaID, result.entityID, err)
		s.compensationFailed = true
	}

	s.pendingCompensations--
	if s.pendingCompensations > 0 {
		return
	}

	if s.compensationFailed {
		s.recordStatus(ctx.Context(), SagaFailed)
		return
	}

	s.recordStatus(ctx.Context(), SagaCompleted)
}

// recordStatus journals a saga status transition and moves the saga to it.
//
// The transition is written to the saga's own journal so a restarted or
// relocated saga comes back with the status it had instead of running again a
// process that already finished. The write is retried with backoff so a
// transient store error does not leave the journal on a stale status. The
// in-memory status is updated even when every attempt fails: the saga must stop
// acting on a status it has left, and the failure is logged for the operator.
func (s *SagaActor) recordStatus(ctx context.Context, status SagaStatus) {
	err := retryWithBackoff(ctx, defaultMaxRetries, func() error {
		return s.journalStatus(ctx, status)
	})
	if err != nil {
		s.logger.Errorf("saga %s: failed to journal the %s status: %v", s.sagaID, status, err)
	}

	s.status = status

	if status == SagaCompleted || status == SagaFailed {
		s.cancelTimeout(s.actorSystem)

		// A settled saga reacts to nothing further: the journal is no longer
		// read on its behalf. Stop is non-blocking, so it is safe here.
		if s.runner != nil {
			_ = s.runner.Stop()
		}
	}
}

// journalStatus appends a SagaStatusChanged envelope to the saga journal and
// advances the sequence counter once the write succeeds.
func (s *SagaActor) journalStatus(ctx context.Context, status SagaStatus) error {
	statusAny, err := anypb.New(&egopb.SagaStatusChanged{
		Status:    uint32(status),
		Timestamp: time.Now().UnixNano(),
		StartedAt: s.startedAt,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal the saga status: %w", err)
	}

	sequenceNumber := s.eventsCounter + 1
	envelope := &egopb.Event{
		PersistenceId:  s.sagaID,
		SequenceNumber: sequenceNumber,
		Event:          statusAny,
		Timestamp:      time.Now().UnixNano(),
		Shard:          s.shard,
	}

	if err := s.eventsStore.WriteEvents(ctx, []*egopb.Event{envelope}); err != nil {
		return err
	}

	s.eventsCounter = sequenceNumber
	return nil
}

// armTimeout schedules the saga timeout for the time the deadline has left.
//
// The remaining duration is derived from the journaled start time, so a saga
// that is restarted or relocated keeps its original deadline instead of
// starting the clock again. A saga whose deadline has already passed is timed
// out through the mailbox, exactly like a scheduled timeout.
func (s *SagaActor) armTimeout(ctx *goakt.ReceiveContext) {
	if s.timeout <= 0 || s.status != SagaRunning {
		return
	}

	remaining := s.timeout - time.Duration(time.Now().UnixNano()-s.startedAt)
	if remaining <= 0 {
		if err := ctx.ActorSystem().NoSender().Tell(ctx.Context(), ctx.Self(), &sagaTimeoutMsg{}); err != nil {
			s.logger.Errorf("saga %s: failed to signal the expired timeout: %v", s.sagaID, err)
		}
		return
	}

	if err := ctx.ActorSystem().ScheduleOnce(ctx.Context(), &sagaTimeoutMsg{}, ctx.Self(), remaining, goakt.WithReference(s.sagaID)); err != nil {
		s.logger.Errorf("saga %s: failed to schedule the timeout: %v", s.sagaID, err)
	}
}

// cancelTimeout releases the saga timeout schedule. The schedule is keyed by
// the saga ID, so releasing it is what allows a later start to arm it again.
//
// The actor system is passed in rather than read from the actor: PostStop runs
// on the goroutine that stops the actor, not on the actor's message loop.
func (s *SagaActor) cancelTimeout(actorSystem goakt.ActorSystem) {
	if s.timeout <= 0 || actorSystem == nil {
		return
	}

	// A schedule that already fired, or was never armed, is not an error here.
	_ = actorSystem.CancelSchedule(s.sagaID)
}

// replyWithStatus replies with the saga's status, current state and the
// sequence number its journal has reached.
func (s *SagaActor) replyWithStatus(ctx *goakt.ReceiveContext) {
	state, _ := anypb.New(s.currentState)
	ctx.Response(&egopb.SagaStatusReply{
		SagaId:         s.sagaID,
		Status:         uint32(s.status),
		State:          state,
		SequenceNumber: s.eventsCounter,
	})
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
