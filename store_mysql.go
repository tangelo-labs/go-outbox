package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

type mysqlStore struct {
	tableName     string
	onDispatchErr func(Event, error)
	db            *sql.DB
}

// NewMySQLStore builds event store that uses MySQL to access the outbox table.
func NewMySQLStore(db *sql.DB, tableName string) EventStore {
	return &mysqlStore{
		tableName: tableName,
		onDispatchErr: func(event Event, err error) {
			// TODO: remove this and make it configurable
			fmt.Printf("error dispatching event %#v: %s\n", event, err.Error())
		},
		db: db,
	}
}

func (g *mysqlStore) SaveTx(ctx context.Context, tx *sql.Tx, event Event) error {
	metaJSON, mErr := json.Marshal(event.Metadata)
	if mErr != nil {
		return mErr
	}

	_, err := tx.
		ExecContext(ctx,
			fmt.Sprintf("INSERT INTO `%s` (id, event_name, payload, metadata, created_at, attempts) VALUES (?, ?, ?, ?, ?, 0)", g.tableName),
			event.ID,
			event.Type,
			event.Payload,
			metaJSON,
			time.Now(),
		)

	return err
}

func (g *mysqlStore) SaveAllTx(ctx context.Context, tx *sql.Tx, events ...Event) error {
	stmt, pErr := tx.PrepareContext(ctx,
		fmt.Sprintf("INSERT INTO `%s` (id, event_name, payload, metadata, created_at) VALUES (?, ?, ?, ?, ?)", g.tableName),
	)

	if pErr != nil {
		return pErr
	}

	for _, event := range events {
		metaJSON, mErr := json.Marshal(event.Metadata)
		if mErr != nil {
			return mErr
		}

		if _, eErr := tx.Stmt(stmt).ExecContext(ctx, event.ID, event.Type, event.Payload, metaJSON, time.Now()); eErr != nil {
			return eErr
		}
	}

	return nil
}

func (g *mysqlStore) Purge(ctx context.Context, olderTan time.Duration) (int64, error) {
	res, err := g.db.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM `%s` WHERE dispatched_at IS NOT NULL AND dispatched_at < ?", g.tableName),
		time.Now().Add(-olderTan),
	)

	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

func (g *mysqlStore) DispatchPendingTx(ctx context.Context, batchSize uint16, fn DispatchFunc) error {
	tx, err := g.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("%w: failed to start transaction", err)
	}

	rows, err := tx.QueryContext(ctx,
		fmt.Sprintf("SELECT id, event_name, payload, metadata FROM `%s` WHERE dispatched_at IS NULL ORDER BY attempts ASC LIMIT ? FOR UPDATE", g.tableName),
		batchSize,
	)

	if err != nil {
		if rErr := tx.Rollback(); rErr != nil {
			return fmt.Errorf("%w: %w: failed to rollback on query error", err, rErr)
		}

		return fmt.Errorf("%w: failed to query for pending events", err)
	}

	forDispatch := make([]Event, 0)
	var eventIDs []string

	for rows.Next() {
		var event Event
		var metaJSON []byte

		if sErr := rows.Scan(&event.ID, &event.Type, &event.Payload, &metaJSON); sErr != nil {
			if rErr := tx.Rollback(); rErr != nil {
				return fmt.Errorf("%w: %w: failed to rollback on scanning row error", sErr, rErr)
			}

			return fmt.Errorf("%w: failed to scan row", sErr)
		}

		if uErr := json.Unmarshal(metaJSON, &event.Metadata); uErr != nil {
			if rErr := tx.Rollback(); rErr != nil {
				return fmt.Errorf("%w: %w: failed to rollback on unmarshalling metadata error", uErr, rErr)
			}

			return fmt.Errorf("%w: failed to unmarshal metadata", uErr)
		}

		forDispatch = append(forDispatch, event)
		eventIDs = append(eventIDs, event.ID)
	}

	if len(forDispatch) == 0 {
		if errRB := tx.Rollback(); err != nil {
			return fmt.Errorf("%w: failed to rollback transaction when no events to dispatch", errRB)
		}

		return nil
	}

	updtAttemptsQ, updtAttemptsArgs, errIN := sqlx.In(fmt.Sprintf("UPDATE `%s` SET attempts = attempts + 1 WHERE id IN (?)", g.tableName), eventIDs)
	if errIN != nil {
		if errRB := tx.Rollback(); errRB != nil {
			return fmt.Errorf("%w: %w: failed to rollback on building update attempts query error", errIN, errRB)
		}

		return fmt.Errorf("%w: failed to build update attempts query", errIN)
	}

	if _, errU := tx.ExecContext(ctx, updtAttemptsQ, updtAttemptsArgs...); errU != nil {
		if errRB := tx.Rollback(); errRB != nil {
			return fmt.Errorf("%w: %w: failed to rollback on update attempts error", errU, errRB)
		}

		return fmt.Errorf("%w: failed when updating attempts", errU)
	}

	if cErr := rows.Close(); cErr != nil {
		if rErr := tx.Rollback(); rErr != nil {
			return fmt.Errorf("%w: %w: failed to rollback on closing rows error", cErr, rErr)
		}

		return fmt.Errorf("%w: failed to close rows", cErr)
	}

	for _, event := range forDispatch {
		if dErr := fn(ctx, event); dErr != nil {
			if g.onDispatchErr != nil {
				g.onDispatchErr(event, dErr)
			}

			// do not mark as delivered if dispatch failed, just skip it
			continue
		}

		stm := fmt.Sprintf("UPDATE `%s` SET dispatched_at = ? WHERE id = ?", g.tableName)
		now := time.Now()

		if _, eErr := tx.ExecContext(ctx, stm, now, event.ID); eErr != nil {
			if g.onDispatchErr != nil {
				g.onDispatchErr(event, eErr)
			}

			// omit error and keep trying other events
			continue
		}
	}

	return tx.Commit()
}
