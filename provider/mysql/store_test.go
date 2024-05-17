package mysql_test

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tangelogames/appocalypse/pkg/outbox"
	"github.com/stretchr/testify/require"
)

func TestMySQLSave(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))

	require.NoError(t, err)
	store := outbox.NewMySQLStore(db, "outbox")

	mock.ExpectBegin()
	tx, bErr := db.Begin()
	require.NoError(t, bErr)

	event := outbox.Event{
		ID:      gofakeit.UUID(),
		Type:    gofakeit.AnimalType(),
		Payload: []byte(gofakeit.Sentence(10)),
		Metadata: map[string]string{
			gofakeit.Color(): gofakeit.Color(),
		},
	}

	mock.ExpectExec("INSERT INTO `outbox` (id, event_name, payload, metadata, created_at, attempts) VALUES (?, ?, ?, ?, ?, 0)").
		WithArgs(
			event.ID,
			event.Type,
			event.Payload,
			metaToJSON(event.Metadata),
			anyTime{},
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	sErr := store.SaveTx(ctx, tx, event)

	require.NoError(t, sErr)
}

type anyTime struct{}

func (a anyTime) Match(v driver.Value) bool {
	_, ok := v.(time.Time)

	return ok
}

func metaToJSON(meta map[string]string) []byte {
	b, _ := json.Marshal(meta)

	return b
}
