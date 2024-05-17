package outbox_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Tangelogames/appocalypse/pkg/outbox"
	"github.com/Tangelogames/appocalypse/pkg/transport/events/causation"
	"github.com/Tangelogames/appocalypse/pkg/transport/events/correlation"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/require"
	"github.com/tangelo-labs/go-domain"
	"github.com/tangelo-labs/go-domain/events"
	"github.com/tangelo-labs/go-dotenv"
	"gorm.io/datatypes"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestGorm(t *testing.T) {
	ctx, cancelFunc := context.WithTimeout(causation.AddToContext(context.Background(), "causation_id_1"), 20*time.Second)
	defer cancelFunc()

	t.Run("GIVEN a gorm repo", func(t *testing.T) {
		outboxTableName := fmt.Sprintf("outbox_%s", gofakeit.UUID())
		orderTableName := fmt.Sprintf("orders_%s", gofakeit.UUID())

		gormDB := setup(t, outboxTableName, orderTableName)
		sqlDB, err := gormDB.DB()

		require.NoError(t, err)

		defer func() {
			gormDB.Migrator().DropTable(outboxTableName)
			gormDB.Migrator().DropTable(orderTableName)
		}()

		repo := &ordersGormRepo{
			db:        gormDB,
			outbox:    outbox.NewMySQLStore(sqlDB, outboxTableName),
			tableName: orderTableName,
		}

		t.Run("WHEN an order is created", func(t *testing.T) {
			orderID := domain.NewID()
			eventID := domain.NewID()

			recorder := events.BaseRecorder{}
			recorder.Record(whatEverEvent{ID: eventID})

			err := repo.Create(ctx, &order{
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
	})
}

type environment struct {
	DatabaseDSN string `env:"DATABASE_OP01"`
}

type mysqlEvent struct {
	ID           string `gorm:"column:id;primaryKey;size:32"`
	EventName    string `gorm:"column:event_name;size:255"`
	Payload      datatypes.JSON
	Metadata     datatypes.JSON
	CreatedAt    time.Time
	DispatchedAt time.Time
	Attempts     int32 `gorm:"column:attempts;default:0"`
}

type whatEverEvent struct {
	ID domain.ID
}

type ordersGormRepo struct {
	db        *gorm.DB
	outbox    outbox.EventStore
	tableName string
}

func (r *ordersGormRepo) Create(ctx context.Context, order *order) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if sErr := tx.Table(r.tableName).Save(order).Error; sErr != nil {
			return sErr
		}

		events, err := prepareEvents(ctx, "orders", order)
		if err != nil {
			return err
		}

		if sErr := r.outbox.SaveAllTx(ctx, tx.Statement.ConnPool.(*sql.Tx), events...); sErr != nil {
			_ = tx.Rollback()

			return sErr
		}

		return nil
	})
}

func prepareEvents(ctx context.Context, topic string, recorder events.Recorder) ([]outbox.Event, error) {
	var events []outbox.Event

	causationID := causation.FromContext(ctx)
	correlationID := correlation.FromContext(ctx)

	for _, event := range recorder.Changes() {
		binaryEvent, err := encodeEvent(event)
		if err != nil {
			return nil, err
		}

		events = append(events, outbox.Event{
			ID:      domain.NewID().String(),
			Type:    fmt.Sprintf("%T", event),
			Payload: binaryEvent,
			Metadata: map[string]string{
				"topic":          topic,
				"causation-id":   causationID,
				"correlation-id": correlationID,
			},
		})
	}

	return events, nil
}

func encodeEvent(event events.Event) ([]byte, error) {
	return json.Marshal(event)
}

func setup(t *testing.T, outboxTableName, orderTableName string) *gorm.DB {
	envVars := environment{}
	require.NoError(t, dotenv.LoadAndParse(&envVars))

	gormDB, err := gorm.Open(mysql.Open(envVars.DatabaseDSN))
	require.NoError(t, err)

	require.NoError(t, gormDB.Migrator().DropTable(&mysqlEvent{}))
	require.NoError(t, gormDB.Migrator().DropTable(&order{}))

	require.NoError(t, gormDB.AutoMigrate(&mysqlEvent{}))
	require.NoError(t, gormDB.Migrator().RenameTable(&mysqlEvent{}, outboxTableName))
	require.NoError(t, gormDB.AutoMigrate(&order{}))
	require.NoError(t, gormDB.Migrator().RenameTable(&order{}, orderTableName))

	return gormDB
}
