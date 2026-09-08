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

package eventstream

import (
	"sync"

	"github.com/google/uuid"
	"go.uber.org/atomic"

	"github.com/tochemey/ego/v4/internal/queue"
)

// Subscriber defines the Subscriber Interface
type Subscriber interface {
	ID() string
	Active() bool
	Topics() []string
	Iterator() chan *Message
	// Ready returns a channel that receives a signal whenever new messages
	// are available to drain via Iterator, or when the subscriber shuts down.
	// Block on Ready instead of calling Iterator in a tight loop: Iterator
	// returns a closed snapshot channel, so polling it while the queue is
	// empty busy-spins a CPU core.
	Ready() <-chan struct{}
	Shutdown()
	// Dropped returns how many messages were discarded because the
	// subscriber's queue was at capacity. It is always zero for an
	// unbounded subscriber.
	Dropped() uint64
	signal(message *Message)
	subscribe(topic string)
	unsubscribe(topic string)
}

// subscriber defines the subscriber
type subscriber struct {
	// id defines the subscriber id
	id string
	// sem represents a lock
	sem sync.Mutex
	// messages of the subscriber
	messages *queue.Queue
	// topics define the topic the subscriber subscribed to
	topics map[string]bool
	// states whether the given subscriber is active or not
	active *atomic.Bool
	// notify wakes consumers blocked on Ready when a message is enqueued
	// or the subscriber shuts down. Capacity one: a pending wake-up already
	// guarantees the next Iterator drain sees every enqueued message.
	notify chan struct{}
	// capacity caps the number of undelivered messages; zero means unbounded
	capacity int
	// pending counts the messages admitted to the queue and not yet drained.
	// Admission reserves a slot atomically, so the concurrent signals of a
	// fan-out cannot overshoot the capacity.
	pending *atomic.Int64
	// dropped counts the messages discarded because the queue was full
	dropped *atomic.Uint64
}

var _ Subscriber = &subscriber{}

// newSubscriber creates an instance of a stream consumer
// and returns the consumer id and its reference.
// The type of messages the consumer will consume is past as type
// parameter
func newSubscriber() *subscriber {
	// create the consumer id
	id := uuid.NewString()
	return &subscriber{
		id:       id,
		sem:      sync.Mutex{},
		messages: queue.NewQueue(),
		topics:   make(map[string]bool),
		active:   atomic.NewBool(true),
		notify:   make(chan struct{}, 1),
		pending:  atomic.NewInt64(0),
		dropped:  atomic.NewUint64(0),
	}
}

// Dropped returns the number of messages discarded because the queue was full
func (x *subscriber) Dropped() uint64 {
	return x.dropped.Load()
}

// admit reserves a queue slot for one message and reports whether it may be
// enqueued. An unbounded subscriber always admits.
func (x *subscriber) admit() bool {
	if x.capacity <= 0 {
		return true
	}

	if x.pending.Inc() > int64(x.capacity) {
		x.pending.Dec()
		return false
	}

	return true
}

// release gives back the slot of one drained message on a bounded subscriber.
func (x *subscriber) release() {
	if x.capacity > 0 {
		x.pending.Dec()
	}
}

// ID return consumer id
func (x *subscriber) ID() string {
	return x.id
}

// Active checks whether the consumer is active
func (x *subscriber) Active() bool {
	return x.active.Load()
}

// Topics returns the list of topics the consumer has subscribed to
func (x *subscriber) Topics() []string {
	// acquire the lock
	x.sem.Lock()
	// release the lock once done
	defer x.sem.Unlock()
	var topics []string
	for topic := range x.topics {
		topics = append(topics, topic)
	}
	return topics
}

// Shutdown shutdowns the consumer
func (x *subscriber) Shutdown() {
	x.active.Store(false)
	// wake any consumer blocked on Ready so it can observe the shutdown
	select {
	case x.notify <- struct{}{}:
	default:
	}
}

// Ready returns a channel that receives a signal whenever new messages are
// available to drain via Iterator, or when the subscriber shuts down.
func (x *subscriber) Ready() <-chan struct{} {
	return x.notify
}

func (x *subscriber) Iterator() chan *Message {
	// Drain the queue into a slice first, then build the channel with the
	// exact right capacity. Sampling Length() for the make call and then
	// re-checking it in the loop creates a race: if a message is enqueued
	// between the two reads, we try to send into a 0-capacity channel while
	// no one is yet reading from it, deadlocking the caller.
	var msgs []*Message
	for x.active.Load() {
		msg := x.messages.Dequeue()
		if msg == nil {
			break
		}
		x.release()
		msgs = append(msgs, msg.(*Message))
	}
	out := make(chan *Message, len(msgs))
	for _, m := range msgs {
		out <- m
	}
	close(out)
	return out
}

// signal is used to push a message to the subscriber
func (x *subscriber) signal(message *Message) {
	// only receive message when active
	if x.active.Load() {
		// a bounded subscriber discards what it cannot hold instead of
		// growing without limit behind a slow consumer
		if !x.admit() {
			x.dropped.Inc()
			return
		}

		x.messages.Enqueue(message)
		// wake a consumer blocked on Ready. Dropping the send when the
		// buffer is full is safe: the pending wake-up's drain will pick
		// this message up too.
		select {
		case x.notify <- struct{}{}:
		default:
		}
	}
}

// subscribe subscribes the subscriber to a given topic
func (x *subscriber) subscribe(topic string) {
	// acquire the lock
	x.sem.Lock()
	// set the topic
	x.topics[topic] = true
	// release the lock
	x.sem.Unlock()
}

// unsubscribe unsubscribes the subscriber from the give topic
func (x *subscriber) unsubscribe(topic string) {
	// acquire the lock
	x.sem.Lock()
	// remove the topic from the consumer topics
	delete(x.topics, topic)
	// release the lock
	x.sem.Unlock()
}
