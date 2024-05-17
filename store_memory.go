package outbox

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

var _ EventStore = (*InMemoryStore)(nil)

// InMemoryStore is an in-memory event store for testing purposes only.
type InMemoryStore struct {
	events map[string]*inMemEvent
	mu     sync.RWMutex
	tx     sync.Mutex
}

type inMemEvent struct {
	inner        Event
	createdAt    time.Time
	dispatchedAt time.Time
}

// NewMemoryStore builds a new in-memory event store for testing purposes only,
// use with caution as SQL transaction handling is not supported which is the
// whole purpose of the outbox pattern.
func NewMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		events: make(map[string]*inMemEvent),
	}
}

// Size returns the number of events in the store.
func (m *InMemoryStore) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.events)
}

// SaveTx saves an event to the store.
func (m *InMemoryStore) SaveTx(ctx context.Context, _ *sql.Tx, event Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.events[event.ID]; ok {
		m.events[event.ID].inner = event

		return nil
	}

	m.events[event.ID] = &inMemEvent{
		inner:     event,
		createdAt: time.Now(),
	}

	return nil
}

// SaveAllTx saves multiple events to the store.
func (m *InMemoryStore) SaveAllTx(ctx context.Context, _ *sql.Tx, events ...Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, event := range events {
		if _, ok := m.events[event.ID]; ok {
			m.events[event.ID].inner = event

			continue
		}

		m.events[event.ID] = &inMemEvent{
			inner:     event,
			createdAt: time.Now(),
		}
	}

	return nil
}

// Purge removes old dispatched events.
func (m *InMemoryStore) Purge(_ context.Context, olderTan time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	d := int64(0)

	for id, event := range m.events {
		if !event.dispatchedAt.IsZero() && event.createdAt.Before(time.Now().Add(-olderTan)) {
			d++
			delete(m.events, id)
		}
	}

	return d, nil
}

// DispatchPendingTx dispatches pending events.
func (m *InMemoryStore) DispatchPendingTx(ctx context.Context, batchSize uint16, fn DispatchFunc) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.tx.Lock()
	defer m.tx.Unlock()

	c := 0

	for _, event := range m.events {
		if event.dispatchedAt.IsZero() {
			if c >= int(batchSize) {
				break
			}

			c++

			if err := fn(ctx, event.inner); err != nil {
				return err
			}

			event.dispatchedAt = time.Now()
		}
	}

	return nil
}
