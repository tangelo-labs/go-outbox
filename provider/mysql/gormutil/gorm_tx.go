package gormutil

import (
	"database/sql"
	"errors"

	"gorm.io/gorm"
)

// ExtractSQLTx attempts to extract the standard database/sql transaction (*sql.Tx)
// from a GORM transaction (*gorm.DB).
//
// Rationale
// GORM manages transactions via *gorm.DB, but some libraries or implementations
// (for example the SQL outbox store) expect a *sql.Tx. Internally GORM exposes
// the current connection/transaction pool at tx.Statement.ConnPool. When
// running inside db.Transaction(func(tx *gorm.DB) error { ... }), the
// ConnPool is typically a *sql.Tx and can be type-asserted.
//
// Usage
//
//	err := gormDB.Transaction(func(tx *gorm.DB) error {
//	    sqlTx, err := gormutil.ExtractSQLTx(tx)
//	    if err != nil {
//	        return err
//	    }
//
//	    // pass sqlTx to a store that expects *sql.Tx
//	    return store.SaveTx(ctx, sqlTx, event)
//	})
//
// Behavior
//   - If tx is nil, or tx.Statement is nil, or tx.Statement.ConnPool is nil,
//     the function returns a descriptive error.
//   - If ConnPool is not a *sql.Tx, the function returns an error indicating
//     the unexpected type.
//
// Security note
// Performing type assertions on library internals can depend on the
// implementation details of the library. This helper centralizes the
// assertion and returns clear errors if the state is not as expected.
func ExtractSQLTx(tx *gorm.DB) (*sql.Tx, error) {
	if tx == nil {
		return nil, errors.New("gorm: transaction is nil")
	}

	stmt := tx.Statement
	if stmt == nil {
		return nil, errors.New("gorm: transaction statement is nil")
	}

	if stmt.ConnPool == nil {
		return nil, errors.New("gorm: statement ConnPool is nil")
	}

	if sqlTx, ok := stmt.ConnPool.(*sql.Tx); ok {
		return sqlTx, nil
	}

	return nil, errors.New("gorm: statement ConnPool is not *sql.Tx")
}
