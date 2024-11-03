package columbus

import (
	"context"
	"database/sql"
)

// SqlInterface is the database sql interface used by Mapper methods, SubQuery and RowPostProcessor
//
// it supports only context methods common between sql.DB and sql.Tx
type SqlInterface interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}
