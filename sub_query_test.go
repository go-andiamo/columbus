package columbus

import (
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

	sq := NewSubQuery("test",
		`SELECT * FROM test_table WHERE id = ?`,
		[]string{"parent_id"},
		nil, false)
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("name"))
	err = sq.Execute(ctx, db, row)
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

	sq := NewSubQuery(
		"test",
		`SELECT * FROM test_table WHERE id = ?`,
		[]string{"parent_id"},
		nil, true)
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	err = sq.Execute(ctx, db, row)
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

	sq := NewSubQuery(
		"test",
		`SELECT * FROM test_table WHERE id = ?`,
		[]string{"parent_id"},
		nil, false)
	row := map[string]any{
		"no_parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("name"))
	err = sq.Execute(ctx, db, row)
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

	sq := NewSubQuery(
		"test",
		`SELECT * FROM test_table WHERE id = ?`,
		[]string{"parent_id"},
		nil, false)
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnError(errors.New("foo"))
	err = sq.Execute(ctx, db, row)
	require.Error(t, err)
}

func TestSubQuery_execute_AsObject(t *testing.T) {
	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	sq := NewObjectSubQuery(
		"test",
		`SELECT * FROM test_table WHERE id = ?`,
		[]string{"parent_id"},
		nil, false, false)
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("name"))
	err = sq.Execute(ctx, db, row)
	require.NoError(t, err)
	require.NotNil(t, row["test"].(map[string]any))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSubQuery_execute_AsObject_SqlError(t *testing.T) {
	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	sq := NewObjectSubQuery(
		"test",
		`SELECT * FROM test_table WHERE id = ?`,
		[]string{"parent_id"},
		nil, false, false)
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnError(errors.New("foo"))
	err = sq.Execute(ctx, db, row)
	require.Error(t, err)
}

func TestSubQuery_execute_AsObject_EmptyNil(t *testing.T) {
	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	sq := NewObjectSubQuery(
		"test",
		`SELECT * FROM test_table WHERE id = ?`,
		[]string{"parent_id"},
		nil, true, false)
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("name"))
	err = sq.Execute(ctx, db, row)
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

	sq := NewObjectSubQuery(
		"test",
		`SELECT * FROM test_table WHERE id = ?`,
		[]string{"parent_id"},
		nil, false, true)
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("name"))
	err = sq.Execute(ctx, db, row)
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

	sq := NewObjectSubQuery(
		"test",
		`SELECT * FROM test_table WHERE id = ?`,
		[]string{"parent_id"},
		nil, false, true)
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	err = sq.Execute(ctx, db, row)
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

	sq := NewObjectSubQuery(
		"test",
		`SELECT * FROM test_table WHERE id = ?`,
		[]string{"parent_id"},
		nil, false, true)
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnError(errors.New("foo"))
	err = sq.Execute(ctx, db, row)
	require.Error(t, err)
}
