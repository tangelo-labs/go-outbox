package gormutil

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestExtractSQLTx_Errors(t *testing.T) {
	// nil tx
	_, err := ExtractSQLTx(nil)
	require.Error(t, err)

	// tx with nil Statement
	tx := &gorm.DB{}
	_, err = ExtractSQLTx(tx)
	require.Error(t, err)

	// tx with Statement but nil ConnPool
	tx = &gorm.DB{Statement: &gorm.Statement{}}
	_, err = ExtractSQLTx(tx)
	require.Error(t, err)

	// tx with ConnPool of wrong type
	stmt := &gorm.Statement{}
	stmt.ConnPool = &sql.DB{} // not *sql.Tx
	tx = &gorm.DB{Statement: stmt}
	_, err = ExtractSQLTx(tx)
	require.Error(t, err)
}
