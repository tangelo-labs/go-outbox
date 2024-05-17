package mysql_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/require"
	"github.com/tangelo-labs/go-domain"
	"github.com/tangelo-labs/go-domain/events"
	"github.com/tangelo-labs/go-outbox"
	"github.com/tangelo-labs/go-outbox/pkg/transport/events/causation"
	"github.com/tangelo-labs/go-outbox/provider/mysql"
)

func TestMysql(t *testing.T) {
	ctx, cancelFunc := context.WithTimeout(causation.AddToContext(context.Background(), "causation_id_1"), 20*time.Second)
	defer cancelFunc()

	t.Run("GIVEN a mysql repo", func(t *testing.T) {
		outboxTableName := fmt.Sprintf("outbox_%s", gofakeit.UUID())
		orderTableName := fmt.Sprintf("orders_%s", gofakeit.UUID())

		gormDB := setup(t, outboxTableName, orderTableName)
		sqlDB, err := gormDB.DB()

		require.NoError(t, err)

		defer func() {
			gormDB.Migrator().DropTable(outboxTableName)
			gormDB.Migrator().DropTable(orderTableName)
		}()

		repo := &ordersRepo{
			db:        sqlDB,
			outbox:    mysql.NewStore(sqlDB, outboxTableName),
			tableName: orderTableName,
		}

		t.Run("WHEN an order is created", func(t *testing.T) {
			orderID := domain.NewID()
			eventID := domain.NewID()

			recorder := events.BaseRecorder{}
			recorder.Record(whatEverEvent{ID: eventID})

			err = repo.Create(ctx, &order{
				ID:           orderID,
				Amount:       100,
				BaseRecorder: recorder,
			})

			t.Run("THEN no error is returned", func(t *testing.T) {
				require.NoError(t, err)
			})

			t.Run("AND the causation is stored along with the event in event store", func(t *testing.T) {
				dispatchedEvents := &queue{}

				d := func(ctx context.Context, event outbox.Event) error {
					dispatchedEvents.add(event)

					return nil
				}

				require.NoError(t, repo.outbox.DispatchPendingTx(ctx, 10, d))
				require.EqualValues(t, dispatchedEvents.events[0].Metadata["causation-id"], "causation_id_1")
			})
		})

		t.Run("WHEN another order is created", func(t *testing.T) {
			eventID := domain.NewID()
			recorder := events.BaseRecorder{}
			recorder.Record(whatEverEvent{ID: eventID})

			err = repo.Create(ctx, &order{
				ID:           domain.NewID(),
				Amount:       100,
				BaseRecorder: recorder,
			})
			require.NoError(t, err)

			t.Run("THEN it is processed too", func(t *testing.T) {
				var processed outbox.Event
				require.NoError(t, repo.outbox.DispatchPendingTx(ctx, 1, func(ctx context.Context, event outbox.Event) error {
					processed = event

					return nil
				}))

				payload := whatEverEvent{}
				require.NoError(t, json.Unmarshal(processed.Payload, &payload))

				require.EqualValues(t, eventID.String(), payload.ID.String())

				t.Run("AND attempts field is increased", func(t *testing.T) {
					row := sqlDB.QueryRowContext(ctx, fmt.Sprintf("SELECT attempts FROM `%s` WHERE id = ?", outboxTableName), processed.ID)
					attempts := 0
					require.NoError(t, row.Err())
					require.NoError(t, row.Scan(&attempts))
					require.EqualValues(t, 1, attempts)
				})
			})
		})
	})
}

type ordersRepo struct {
	db        *sql.DB
	outbox    outbox.EventStore
	tableName string
}

type order struct {
	ID     domain.ID
	Amount int64
	events.BaseRecorder
}

func (r *ordersRepo) Create(ctx context.Context, order *order) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	_, eErr := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO `%s` (id, amount) VALUES (?, ?)", r.tableName), order.ID, order.Amount)
	if eErr != nil {
		_ = tx.Rollback()

		return eErr
	}

	events, err := prepareEvents(ctx, "orders", order)
	if err != nil {
		_ = tx.Rollback()

		return err
	}

	if sErr := r.outbox.SaveAllTx(ctx, tx, events...); sErr != nil {
		_ = tx.Rollback()

		return sErr
	}

	return tx.Commit()
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
