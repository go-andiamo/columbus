package columbus

import (
	"database/sql"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewSubQuery_Execute(t *testing.T) {
	db, mock, err := sqlmock.New()
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

func TestNewSubQuery_Execute_Twice(t *testing.T) {
	db, mock, err := sqlmock.New()
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

	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("name"))
	err = sq.Execute(ctx, db, row)
	require.NoError(t, err)
	require.NotNil(t, row["test"])
	require.Equal(t, 1, len(row["test"].([]map[string]any)))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestNewSubQuery_Execute_NoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
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

func TestNewSubQuery_Execute_ArgsErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
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

func TestNewSubQuery_Execute_SqlErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
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

func TestNewObjectSubQuery_Execute(t *testing.T) {
	db, mock, err := sqlmock.New()
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

func TestNewObjectSubQuery_Execute_SqlError(t *testing.T) {
	db, mock, err := sqlmock.New()
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

func TestNewObjectSubQuery_Execute_EmptyNil(t *testing.T) {
	db, mock, err := sqlmock.New()
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
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{}))
	err = sq.Execute(ctx, db, row)
	require.NoError(t, err)
	require.Nil(t, row["test"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestNewObjectSubQuery_Execute_ErrNoRow(t *testing.T) {
	db, mock, err := sqlmock.New()
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

func TestNewObjectSubQuery_Execute_ErrNoRow_Errors(t *testing.T) {
	db, mock, err := sqlmock.New()
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

func TestNewObjectSubQuery_Execute_SqlErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
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

func TestNewObjectSubQuery_Execute_ArgsErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
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
		"no_parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("name"))
	err = sq.Execute(ctx, db, row)
	require.Error(t, err)
	require.Equal(t, "sub-query arg property 'parent_id' does not exist", err.Error())
}

func TestNewObjectSubQuery_Execute_ErrNoRows_ArgsErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
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
		"no_parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("name"))
	err = sq.Execute(ctx, db, row)
	require.Error(t, err)
	require.Equal(t, "sub-query arg property 'parent_id' does not exist", err.Error())
}

func TestNewMergeSubQuery_Execute(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	sq := NewMergeSubQuery(`SELECT * FROM test_table WHERE id = ?`,
		[]string{"parent_id"},
		nil, false)
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("foo"))
	err = sq.Execute(ctx, db, row)
	require.NoError(t, err)
	require.Equal(t, 2, len(row))
	require.Equal(t, row["id"], "foo")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestNewMergeSubQuery_Execute_NoOverwrite(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	sq := NewMergeSubQuery(`SELECT * FROM test_table WHERE id = ?`,
		[]string{"parent_id"},
		nil, true)
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnRows(sqlmock.NewRows([]string{"id", "parent_id"}).AddRow("foo", int64(17)))
	err = sq.Execute(ctx, db, row)
	require.NoError(t, err)
	require.Equal(t, 2, len(row))
	require.Equal(t, row["id"], "foo")
	require.Equal(t, int64(16), row["parent_id"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestNewMergeSubQuery_Execute_SqlErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	sq := NewMergeSubQuery(`SELECT * FROM test_table WHERE id = ?`,
		[]string{"parent_id"},
		nil, false)
	row := map[string]any{
		"parent_id": int64(16),
	}
	mock.ExpectQuery("").WithArgs(int64(16)).WillReturnError(errors.New("foo"))
	err = sq.Execute(ctx, db, row)
	require.Error(t, err)
}

func TestNewMergeSubQuery_Execute_ArgsErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	sq := NewMergeSubQuery(`SELECT * FROM test_table WHERE id = ?`,
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
