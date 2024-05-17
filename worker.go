package outbox

import (
	"context"
	"sync"
	"time"
)

// Worker is responsible for periodically retrieving pending events from an
// event store dispatching them.
type Worker interface {
	// Start starts running the worker until the given context is cancelled.
	// If worker is already running this method should noop.
	Start(ctx context.Context, store EventStore, dispatcher DispatchFunc) error

	// Done returns a channel that is closed when the worker is stopped.
	Done() <-chan struct{}
}

type worker struct {
	dispatchTimeout time.Duration
	pullInterval    time.Duration
	cleanInterval   time.Duration
	retentionTime   time.Duration
	throughput      uint16

	running bool
	done    chan struct{}
	mu      sync.Mutex
}

// NewWorker builds a new worker instance.
//
//   - dispatchTimeout is the maximum time allowed for a single dispatch roundtrip.
//   - pullInterval determines how often the worker will check for pending events.
//   - cleanInterval determines how often the worker will clean up old events.
//   - retentionTime determines how long DISPATCHED events are kept in the event
//     store before they are deleted.
//   - throughput is the maximum number of events that can be dispatched in a
//     single pull operation.
func NewWorker(
	dispatchTimeout time.Duration,
	pullInterval time.Duration,
	cleanInterval time.Duration,
	retentionTime time.Duration,
	throughput uint16,
) Worker {
	return &worker{
		dispatchTimeout: dispatchTimeout,
		pullInterval:    pullInterval,
		cleanInterval:   cleanInterval,
		retentionTime:   retentionTime,
		throughput:      throughput,
	}
}

func (wk *worker) Start(ctx context.Context, store EventStore, dispatcher DispatchFunc) error {
	wk.mu.Lock()
	defer wk.mu.Unlock()

	if wk.running {
		return nil
	}

	wk.running = true
	wk.done = make(chan struct{})

	go func() {
		done := ctx.Done()
		pull := time.NewTicker(wk.pullInterval)
		clean := time.NewTicker(wk.cleanInterval)

		for {
			select {
			case <-done:
				wk.mu.Lock()
				wk.running = false
				wk.mu.Unlock()
				close(wk.done)

				return
			case <-clean.C:
				if _, err := store.Purge(ctx, wk.retentionTime); err != nil {
					// TODO: monitor purge error
					println(err.Error())
				}
			case <-pull.C:
				wk.roundtrip(ctx, store, dispatcher)
			}
		}
	}()

	return nil
}

func (wk *worker) Done() <-chan struct{} {
	return wk.done
}

func (wk *worker) roundtrip(ctx context.Context, store EventStore, dispatcher DispatchFunc) {
	dCtx, cancel := context.WithTimeout(ctx, wk.dispatchTimeout)
	err := store.DispatchPendingTx(dCtx, wk.throughput, dispatcher)
	cancel()

	if err != nil {
		// TODO: monitor dispatch error
		println(err.Error())
	}
}
