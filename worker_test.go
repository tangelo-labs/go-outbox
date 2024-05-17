package outbox_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Tangelogames/appocalypse/pkg/outbox"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/require"
)

func TestWorker_Start(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	t.Run("GIVEN an event store with 3 unpublished events AND a worker instance with throughput=1, cleanupInterval=10ms, retention=1ms", func(t *testing.T) {
		events := []outbox.Event{
			{
				ID:      gofakeit.UUID(),
				Type:    gofakeit.AnimalType(),
				Payload: []byte(gofakeit.Sentence(10)),
				Metadata: map[string]string{
					gofakeit.Color(): gofakeit.Color(),
				},
			},
			{
				ID:      gofakeit.UUID(),
				Type:    gofakeit.AnimalType(),
				Payload: []byte(gofakeit.Sentence(10)),
				Metadata: map[string]string{
					gofakeit.Color(): gofakeit.Color(),
				},
			},
			{
				ID:      gofakeit.UUID(),
				Type:    gofakeit.AnimalType(),
				Payload: []byte(gofakeit.Sentence(10)),
				Metadata: map[string]string{
					gofakeit.Color(): gofakeit.Color(),
				},
			},
		}

		store := &storeSpy{InMemoryStore: outbox.NewMemoryStore()}
		sErr := store.SaveAllTx(ctx, nil, events...)

		require.NoError(t, sErr)

		w := outbox.NewWorker(
			1*time.Second,
			100*time.Millisecond,
			10*time.Millisecond,
			time.Millisecond,
			1,
		)

		t.Run("WHEN the worker is started with a fake dispatcher", func(t *testing.T) {
			dispatchedEvents := &queue{}

			d := func(ctx context.Context, event outbox.Event) error {
				dispatchedEvents.add(event)

				return nil
			}

			require.NoError(t, w.Start(ctx, store, d))

			t.Run("THEN eventually all events are dispatched AND store is invoked at least 3 times AND purge is invoked at least once AND event store is now empty", func(t *testing.T) {
				require.Eventually(t, func() bool {
					return len(dispatchedEvents.ids()) == len(events) && store.getDispatchPendingTxCalls() >= 3 && store.getPurgeCalls() > 0
				}, 5*time.Second, 100*time.Millisecond)

				require.Contains(t, dispatchedEvents.ids(), events[0].ID)
				require.Contains(t, dispatchedEvents.ids(), events[1].ID)
				require.Contains(t, dispatchedEvents.ids(), events[2].ID)
				require.Zero(t, store.Size())
			})
		})
	})
}

type queue struct {
	events []outbox.Event
	mu     sync.Mutex
}

func (q *queue) add(event outbox.Event) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.events = append(q.events, event)
}

func (q *queue) ids() []string {
	q.mu.Lock()
	defer q.mu.Unlock()

	ids := make([]string, len(q.events))

	for i, event := range q.events {
		ids[i] = event.ID
	}

	return ids
}

type storeSpy struct {
	*outbox.InMemoryStore

	purgeCalls             int
	dispatchPendingTxCalls int
	mu                     sync.Mutex
}

func (s *storeSpy) getDispatchPendingTxCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.dispatchPendingTxCalls
}

func (s *storeSpy) getPurgeCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.purgeCalls
}

func (s *storeSpy) DispatchPendingTx(ctx context.Context, batchSize uint16, fn outbox.DispatchFunc) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.dispatchPendingTxCalls++

	return s.InMemoryStore.DispatchPendingTx(ctx, batchSize, fn)
}

func (s *storeSpy) Purge(ctx context.Context, olderTan time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.purgeCalls++

	return s.InMemoryStore.Purge(ctx, olderTan)
}
