package columbus

import (
	"context"
	"database/sql"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSubQuery_execute(t *testing.T) {
	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	sq := &SubQuery{
		PropertyName: "test",
		Query:        `SELECT * FROM test_table WHERE id = ?`,
		ArgColumns:   []string{"parent_id"},
	}
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("name"))
	err = sq.execute(context.Background(), db, row)
	require.NoError(t, err)
	require.NotNil(t, row["test"])
	require.Equal(t, 1, len(row["test"].([]map[string]any)))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSubQuery_execute_NoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	sq := &SubQuery{
		PropertyName: "test",
		Query:        `SELECT * FROM test_table WHERE id = ?`,
		ArgColumns:   []string{"parent_id"},
		EmptyNil:     true,
	}
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	err = sq.execute(context.Background(), db, row)
	require.NoError(t, err)
	require.Nil(t, row["test"])
}

func TestSubQuery_execute_ArgsErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	sq := &SubQuery{
		PropertyName: "test",
		Query:        `SELECT * FROM test_table WHERE id = ?`,
		ArgColumns:   []string{"parent_id"},
	}
	row := map[string]any{
		"no_parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("name"))
	err = sq.execute(context.Background(), db, row)
	require.Error(t, err)
	require.Equal(t, "sub-query arg property 'parent_id' does not exist", err.Error())
}

func TestSubQuery_execute_SqlErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	sq := &SubQuery{
		PropertyName: "test",
		Query:        `SELECT * FROM test_table WHERE id = ?`,
		ArgColumns:   []string{"parent_id"},
	}
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnError(errors.New("foo"))
	err = sq.execute(context.Background(), db, row)
	require.Error(t, err)
}

func TestSubQuery_execute_AsObject(t *testing.T) {
	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	sq := &SubQuery{
		PropertyName: "test",
		Query:        `SELECT * FROM test_table WHERE id = ?`,
		ArgColumns:   []string{"parent_id"},
		AsObject:     true,
	}
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("name"))
	err = sq.execute(context.Background(), db, row)
	require.NoError(t, err)
	require.NotNil(t, row["test"].(map[string]any))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSubQuery_execute_AsObject_EmptyNil(t *testing.T) {
	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	sq := &SubQuery{
		PropertyName: "test",
		Query:        `SELECT * FROM test_table WHERE id = ?`,
		ArgColumns:   []string{"parent_id"},
		AsObject:     true,
		EmptyNil:     true,
	}
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("name"))
	err = sq.execute(context.Background(), db, row)
	require.NoError(t, err)
	require.Nil(t, row["test"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSubQuery_execute_AsObject_ErrNoRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	sq := &SubQuery{
		PropertyName: "test",
		Query:        `SELECT * FROM test_table WHERE id = ?`,
		ArgColumns:   []string{"parent_id"},
		AsObject:     true,
		ErrNoRow:     true,
	}
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("name"))
	err = sq.execute(context.Background(), db, row)
	require.NoError(t, err)
	require.NotNil(t, row["test"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSubQuery_execute_AsObject_ErrNoRow_Errors(t *testing.T) {
	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	sq := &SubQuery{
		PropertyName: "test",
		Query:        `SELECT * FROM test_table WHERE id = ?`,
		ArgColumns:   []string{"parent_id"},
		AsObject:     true,
		ErrNoRow:     true,
	}
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	err = sq.execute(context.Background(), db, row)
	require.Error(t, err)
	require.Equal(t, err, sql.ErrNoRows)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSubQuery_execute_AsObject_SqlErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	sq := &SubQuery{
		PropertyName: "test",
		Query:        `SELECT * FROM test_table WHERE id = ?`,
		ArgColumns:   []string{"parent_id"},
		AsObject:     true,
	}
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnError(errors.New("foo"))
	err = sq.execute(context.Background(), db, row)
	require.Error(t, err)
}
