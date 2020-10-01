/*
Package stream provides a publish/subscribe system for events produced by changes
to the state store.
*/
package stream

import "fmt"

// Topic is an identifier that partitions events. A subscription will only receive
// events which match the Topic.
type Topic fmt.Stringer

// Event is a structure with identifiers and a payload. Events are Published to
// EventPublisher and returned to Subscribers.
type Event struct {
	Topic   Topic
	Key     string
	Index   uint64
	Payload interface{}
}

// IsEndOfSnapshot returns true if this is a framing event that indicates the
// snapshot has completed. Future events from Subscription.Next will be
// change events.
func (e Event) IsEndOfSnapshot() bool {
	return e.Payload == endOfSnapshot{}
}

// IsEndOfEmptySnapshot returns true if this is a framing event that indicates
// there is no snapshot. Future events from Subscription.Next will be
// change events.
func (e Event) IsEndOfEmptySnapshot() bool {
	return e.Payload == endOfEmptySnapshot{}
}

func (e Event) Slice() []Event {
	return []Event{e}
}

func (e Event) Empty() bool {
	return false
}

func (e Event) First() Event {
	return e
}

type endOfSnapshot struct{}

type endOfEmptySnapshot struct{}

type closeSubscriptionPayload struct {
	tokensSecretIDs []string
}

// NewCloseSubscriptionEvent returns a special Event that is handled by the
// stream package, and is never sent to subscribers. EventProcessor handles
// these events, and closes any subscriptions which were created using a token
// which matches any of the tokenSecretIDs.
//
// tokenSecretIDs may contain duplicate IDs.
func NewCloseSubscriptionEvent(tokenSecretIDs []string) Event {
	return Event{Payload: closeSubscriptionPayload{tokensSecretIDs: tokenSecretIDs}}
}

// EventBatch is a slice of Events. It is used to wrap a slice of that it
// implements the Events interface.
type EventBatch []Event

func (b EventBatch) Slice() []Event {
	return b
}

func (b EventBatch) Empty() bool {
	return len(b) == 0
}

func (b EventBatch) First() Event {
	if len(b) == 0 {
		return Event{}
	}
	return b[0]
}

// Events is an interface for types which can be represented as a slice of events.
// It exists as a convenient interface for receiving either a single Event or
// a slice of []Event.
// Both Event and EventBatch implement this interface.
type Events interface {
	Slice() []Event
	Empty() bool
	First() Event
}
