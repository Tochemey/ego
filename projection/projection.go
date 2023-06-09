package projection

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/flowchartsman/retry"
	"github.com/pkg/errors"
	"github.com/tochemey/ego/egopb"
	"github.com/tochemey/ego/eventstore"
	"github.com/tochemey/ego/internal/telemetry"
	"github.com/tochemey/ego/offsetstore"
	"github.com/tochemey/goakt/log"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Handler is used to handle event and state consumed from the event store
type Handler func(ctx context.Context, persistenceID string, event *anypb.Any, state *anypb.Any, offset uint64) error

// Projection defines the projection
type Projection struct {
	// Name specifies the projection Name
	name string
	// Logger specifies the logger
	logger log.Logger
	// Handler specifies the projection handler
	handler Handler
	// JournalStore specifies the journal store for reading events
	eventsStore eventstore.EventsStore
	// OffsetStore specifies the offset store to commit offsets
	offsetsStore offsetstore.OffsetStore
	// Specifies the recovery setting
	recovery *Recovery
	// stop signal
	stopSignal chan struct{}
	isStarted  *atomic.Bool
}

// New create an instance of Projection given the name of the projection, the handler and the offsets store
func New(name string, handler Handler,
	eventsStore eventstore.EventsStore,
	offsetStore offsetstore.OffsetStore,
	recovery *Recovery,
	logger log.Logger) *Projection {
	return &Projection{
		handler:      handler,
		offsetsStore: offsetStore,
		name:         name,
		eventsStore:  eventsStore,
		logger:       logger,
		recovery:     recovery,
		stopSignal:   make(chan struct{}, 1),
		isStarted:    atomic.NewBool(false),
	}
}

// Start starts the projection
func (p *Projection) Start(ctx context.Context) error {
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "PreStart")
	defer span.End()

	if p.isStarted.Load() {
		return nil
	}

	// connect to the offset store
	if p.offsetsStore == nil {
		return errors.New("offsets store is not defined")
	}

	// call the connect method of the journal store
	if err := p.offsetsStore.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to the offsets store: %v", err)
	}

	// connect to the events store
	if p.eventsStore == nil {
		return errors.New("events store is not defined")
	}

	// call the connect method of the journal store
	if err := p.eventsStore.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to the events store: %v", err)
	}

	// we will ping the stores 5 times to see whether there have started successfully or not.
	// The operation will be done in an exponential backoff mechanism with an initial delay of a second and a maximum delay of a second.
	// Once the retries have completed and still not connected we fail the start process of the projection.
	const (
		maxRetries   = 5
		initialDelay = time.Second
		maxDelay     = time.Second
	)
	// create a new instance of retrier that will try a maximum of five times, with
	// an initial delay of 100 ms and a maximum delay of 1 second
	retrier := retry.NewRetrier(maxRetries, initialDelay, maxDelay)
	err := retrier.RunContext(ctx, func(ctx context.Context) error {
		// we ping both stores in parallel
		g, ctx := errgroup.WithContext(ctx)
		// ping the journal store
		g.Go(func() error {
			return p.eventsStore.Ping(ctx)
		})
		// ping the offset store
		g.Go(func() error {
			return p.offsetsStore.Ping(ctx)
		})
		// return the result of operations
		return g.Wait()
	})

	if err != nil {
		return errors.Wrap(err, "failed to start the projection")
	}

	// set the started status
	p.isStarted.Store(true)

	// start processing
	go p.processingLoop(ctx)

	return nil
}

// Stop stops the projection
func (p *Projection) Stop(ctx context.Context) error {
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "PostStop")
	defer span.End()

	// check whether it is stopped or not
	if !p.isStarted.Load() {
		return nil
	}

	// send the stop
	close(p.stopSignal)

	// disconnect the events store
	if err := p.eventsStore.Disconnect(ctx); err != nil {
		return fmt.Errorf("failed to disconnect the events store: %v", err)
	}

	// disconnect the offset store
	if err := p.offsetsStore.Disconnect(ctx); err != nil {
		return fmt.Errorf("failed to disconnect the offsets store: %v", err)
	}

	// set the started status to false
	p.isStarted.Store(false)
	return nil
}

// processingLoop is a loop that continuously runs to process events persisted onto the journal store until the projection is stopped
func (p *Projection) processingLoop(ctx context.Context) {
	for {
		select {
		case <-p.stopSignal:
			return
		default:
			// let us fetch all the persistence persistenceIDs
			// TODO: use cursor pagination to avoid memory outage
			persistenceIDs, err := p.eventsStore.PersistenceIDs(ctx)
			// handle the error
			if err != nil {
				// log the error
				p.logger.Error(errors.Wrap(err, "failed to fetch the list of persistence IDs"))
				// here we stop the projection
				err := p.Stop(ctx)
				// handle the error
				if err != nil {
					// log the error
					p.logger.Error(err)
					return
				}
				return
			}

			// with a simple parallelism we process all persistence IDs
			// TODO: break the work with workers that can fail without failing the entire projection
			g, ctx := errgroup.WithContext(ctx)
			idsChan := make(chan string, 1)

			// let us push the id into the channel
			g.Go(func() error {
				defer close(idsChan)
				// let us push the persistence id into the channel
				for _, persistenceID := range persistenceIDs {
					select {
					case idsChan <- persistenceID:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			})

			// Start a fixed number of goroutines process the persistenceIDs.
			for i := 0; i < 20; i++ {
				g.Go(func() error {
					for persistenceID := range idsChan {
						select {
						case <-ctx.Done():
							return ctx.Err()
						default:
							return p.doProcess(ctx, persistenceID)
						}
					}
					return nil
				})
			}

			// wait for all the processing to be done
			if err := g.Wait(); err != nil {
				// log the error
				p.logger.Error(err)
				switch p.recovery.RecoveryStrategy() {
				case egopb.ProjectionRecoveryStrategy_FAIL,
					egopb.ProjectionRecoveryStrategy_RETRY_AND_FAIL:
					// here we stop the projection
					err := p.Stop(ctx)
					// handle the error
					if err != nil {
						// log the error
						p.logger.Error(err)
						return
					}
					return
				default:
					// pass
				}
			}
		}
	}
}

// doProcess processes all events of a given persistent entity and hand them over to the handler
func (p *Projection) doProcess(ctx context.Context, persistenceID string) error {
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "HandlePersistenceID")
	defer span.End()

	if !p.isStarted.Load() {
		return nil
	}

	// get the latest offset persisted for the persistence id
	offset, err := p.offsetsStore.GetCurrentOffset(ctx, offsetstore.NewProjectionID(p.name, persistenceID))
	if err != nil {
		return err
	}

	// define the fromSequenceNumber
	fromSequenceNumber := offset.GetCurrentOffset() + 1

	// fetch events
	events, err := p.eventsStore.ReplayEvents(ctx, persistenceID, fromSequenceNumber, math.MaxUint64, math.MaxUint64)
	if err != nil {
		return err
	}

	// grab the length of events
	length := len(events)

	// there is nothing to process
	if length == 0 {
		return nil
	}

	// define a variable that hold the number of events successfully processed
	// iterate the events
	for i := 0; i < length; i++ {
		// grab the envelope
		envelope := events[i]
		// grab the data to pass to the projection handler
		state := envelope.GetResultingState()
		event := envelope.GetEvent()
		seqNr := envelope.GetSequenceNumber()

		// send the request to the handler based upon the recovery strategy in place
		switch p.recovery.RecoveryStrategy() {
		case egopb.ProjectionRecoveryStrategy_FAIL:
			// send the data to the handler. In case of error we log the error and fail the projection
			if err := p.handler(ctx, persistenceID, event, state, seqNr); err != nil {
				p.logger.Error(errors.Wrapf(err, "failed to process event for persistence id=%s, sequence=%d", persistenceID, seqNr))
				return err
			}

		case egopb.ProjectionRecoveryStrategy_RETRY_AND_FAIL:
			// grab the max retries amd delay
			retries := p.recovery.Retries()
			delay := p.recovery.RetryDelay()
			// create a new exponential backoff that will try a maximum of retries times, with
			// an initial delay of 100 ms and a maximum delay
			backoff := retry.NewRetrier(int(retries), 100*time.Millisecond, delay)
			// pass the data to the projection handler
			if err := backoff.Run(func() error {
				// handle the projection handler error
				if err := p.handler(ctx, persistenceID, event, state, seqNr); err != nil {
					p.logger.Error(errors.Wrapf(err, "failed to process event for persistence id=%s, sequence=%d", persistenceID, seqNr))
					return err
				}
				return nil
			}); err != nil {
				// because we fail we return the error without committing the offset
				return err
			}

		case egopb.ProjectionRecoveryStrategy_RETRY_AND_SKIP:
			// grab the max retries amd delay
			retries := p.recovery.Retries()
			delay := p.recovery.RetryDelay()
			// create a new exponential backoff that will try a maximum of retries times, with
			// an initial delay of 100 ms and a maximum delay
			backoff := retry.NewRetrier(int(retries), 100*time.Millisecond, delay)
			// pass the data to the projection handler
			if err := backoff.Run(func() error {
				if err := p.handler(ctx, persistenceID, event, state, seqNr); err != nil {
					return err
				}
				return nil
			}); err != nil {
				// here we just log the error, but we skip the event and commit the offset
				p.logger.Error(errors.Wrapf(err, "failed to process event for persistence id=%s, sequence=%d", persistenceID, seqNr))
			}

		case egopb.ProjectionRecoveryStrategy_SKIP:
			// send the data to the handler. In case of error we just log the error and skip the event by committing the offset
			if err := p.handler(ctx, persistenceID, event, state, seqNr); err != nil {
				p.logger.Error(errors.Wrapf(err, "failed to process event for persistence id=%s, sequence=%d", persistenceID, seqNr))
			}
		}
		// the envelope has been successfully processed
		// here we commit the offset to the offset store and continue the next event
		offset = &egopb.Offset{
			PersistenceId:  persistenceID,
			ProjectionName: p.name,
			CurrentOffset:  seqNr,
			Timestamp:      timestamppb.Now().AsTime().UnixMilli(),
		}
		// write the given offset and return any possible error
		if err := p.offsetsStore.WriteOffset(ctx, offset); err != nil {
			return errors.Wrapf(err, "failed to persist offset for persistence id=%s", persistenceID)
		}
	}

	return nil
}
