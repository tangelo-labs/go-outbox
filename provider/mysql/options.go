package mysql

import "database/sql"

// Option is a functional option that can be used to configure the store.
type Option func(*store)

// WithIsolationLevel sets the isolation level for the store.
func WithIsolationLevel(level IsolationLevel) Option {
	return func(s *store) {
		s.isolationLevel = sql.IsolationLevel(level)
	}
}
