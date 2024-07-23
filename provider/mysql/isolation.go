package mysql

import "database/sql"

// IsolationLevel is a type that represents the isolation level of a transaction.
type IsolationLevel sql.IsolationLevel

// Allowed isolation levels.
const (
	ReadCommittedIsolation = IsolationLevel(sql.LevelReadCommitted)
	SerializableIsolation  = IsolationLevel(sql.LevelSerializable)
)
