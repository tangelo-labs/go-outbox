package outbox

import (
	"context"
	"database/sql"
	"time"
)

// Event is an event that is stored in the event store.
type Event struct {
	// ID uniquely identifies the event within the event store.
	ID string

	// Type is the event type. e.g. "orders.created".
	Type string

	// Payload is the event data itself encoded as a slice of bytes which can
	// be safely transferred over the network.
	Payload []byte

	// Metadata any arbitrary metadata that can be attached to the event. For
	// example, topic name which can be used by PubSub dispatchers.
	Metadata map[string]string
}

// DispatchFunc is a function that is called for each event that is retrieved
// from the event store that has not been dispatched yet. If the function
// returns no error, then the event is marked as dispatched.
//
// The main purpose of this function is to dispatch the events to an external
// message broker system such as Kafka or RabbitMQ.
type DispatchFunc func(ctx context.Context, event Event) error

// EventStore is responsible for storing and retrieving events.
type EventStore interface {
	// SaveTx saves an event to the event store within the given transaction.
	SaveTx(ctx context.Context, tx *sql.Tx, event Event) error

	// SaveAllTx similar to SaveTx, but saves multiple events.
	SaveAllTx(ctx context.Context, tx *sql.Tx, events ...Event) error

	// DispatchPendingTx retrieves events that have not been dispatched yet
	// and calls the given dispatching function for each one within an SQL
	// transaction. If the function returns no error, then the event is marked
	// as dispatched. Otherwise, it will be skipped and retried on the next
	// call to this method.
	//
	// This is the method that should be used to dispatch events to an external
	// message broker system in a transactional manner with "at-least-once"
	// delivery guarantee.
	//
	// The batchSize parameter specifies the maximum number of events that
	// should be retrieved from the event store at once. Please note that using
	// a large batch size may result in transaction timeouts as the transaction
	// will be open for a longer period of time while the events are being
	// dispatched.
	DispatchPendingTx(ctx context.Context, batchSize uint16, fn DispatchFunc) error

	// Purge deletes all dispatched events that are older than the given
	// duration, and returns the number of deleted events.
	Purge(ctx context.Context, olderTan time.Duration) (int64, error)
}
