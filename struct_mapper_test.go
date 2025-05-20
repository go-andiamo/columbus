package columbus

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reflect"
	"strings"
	"testing"
	"time"
)

type testStruct struct {
	unexported string
	Foo        string `db:"foo"`
	Bar        string `db:"bar"`
}

type testPostProcessor[T testStruct] struct{}

func (m *testPostProcessor[T]) PostProcess(ctx context.Context, sqli SqlInterface, row *testStruct) error {
	row.Foo = strings.ToLower(row.Foo)
	return nil
}

var _ StructPostProcessor[testStruct] = &testPostProcessor[testStruct]{}

type testErrorPostProcessor[T testStruct] struct{}

func (m *testErrorPostProcessor[T]) PostProcess(ctx context.Context, sqli SqlInterface, row *testStruct) error {
	return errors.New("fooey")
}

var _ StructPostProcessor[testStruct] = &testErrorPostProcessor[testStruct]{}

type testColumnNamer struct{}

func (t *testColumnNamer) ColumnName(structType reflect.Type, fld reflect.StructField) (string, bool) {
	return "", false
}

var _ FieldColumnNamer = &testColumnNamer{}

func TestNewStructMapper(t *testing.T) {
	sm, err := NewStructMapper[testStruct](`foo,bar`,
		Query("FROM table"),
		ErrorOnUnknownColumns(true), ErrorOnUnMappedColumns(true),
		UseTagName("db"),
		&testPostProcessor[testStruct]{},
		&testColumnNamer{},
	)
	require.NoError(t, err)
	require.NotNil(t, sm)
	raw := sm.(*structMapper[testStruct])
	assert.Equal(t, "foo,bar", raw.cols)
	assert.Equal(t, Query("SELECT foo,bar FROM table"), *raw.defaultQuery)
	assert.False(t, raw.mapped)
	assert.Nil(t, raw.mapError)
	assert.True(t, raw.errorOnUnknownColumns)
	assert.True(t, raw.errorOnUnMappedColumns)
	assert.Equal(t, "db", raw.useTagName)
	assert.Len(t, raw.fieldColumnNamers, 2)
	assert.Len(t, raw.postProcessors, 1)
}

func TestMustNewStructMapper(t *testing.T) {
	sm := MustNewStructMapper[testStruct](`foo,bar`,
		Query("FROM table"),
		ErrorOnUnknownColumns(true), ErrorOnUnMappedColumns(true),
		UseTagName("db"),
		&testPostProcessor[testStruct]{},
		&testColumnNamer{},
	)
	require.NotNil(t, sm)
	raw := sm.(*structMapper[testStruct])
	assert.Equal(t, "foo,bar", raw.cols)
	assert.Equal(t, Query("SELECT foo,bar FROM table"), *raw.defaultQuery)
	assert.False(t, raw.mapped)
	assert.Nil(t, raw.mapError)
	assert.True(t, raw.errorOnUnknownColumns)
	assert.True(t, raw.errorOnUnMappedColumns)
	assert.Equal(t, "db", raw.useTagName)
	assert.Len(t, raw.fieldColumnNamers, 2)
	assert.Len(t, raw.postProcessors, 1)
}

func TestNewStructMapper_Errors(t *testing.T) {
	_, err := NewStructMapper[testStruct](`foo,bar`,
		Query("FROM table"), Query("FROM table"))
	require.Error(t, err)
	assert.Equal(t, "cannot use multiple default queries", err.Error())

	_, err = NewStructMapper[testStruct](`foo,bar`,
		"not a valid option")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown option type:")

	_, err = NewStructMapper[testStruct](`foo,bar`,
		Query(" ,extra_col FROM table"))
	require.Error(t, err)
	assert.Equal(t, "cannot forge extra columns using Query", err.Error())

	_, err = NewStructMapper[string](`foo,bar`)
	require.Error(t, err)
	assert.Equal(t, "StructMapper can only be used with struct types", err.Error())

	type badStruct struct {
		Foo string `sql:"foo"`
		Bar string `sql:"foo"`
	}
	_, err = NewStructMapper[badStruct](`foo,bar`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate column mapping")

	require.Panics(t, func() {
		_ = MustNewStructMapper[badStruct](`foo,bar`)
	})
}

func TestStructMapper_rowMapOptions(t *testing.T) {
	sm := MustNewStructMapper[testStruct](`foo,bar`,
		Query("FROM table"),
		&testPostProcessor[testStruct]{})
	require.NotNil(t, sm)
	raw := sm.(*structMapper[testStruct])
	query, postProcessors, limiter, err := raw.rowMapOptions([]any{
		Query("FROM table2"), AddClause("WHERE id = ?"),
		&testPostProcessor[testStruct]{},
		defaultLimiter,
	})
	require.NoError(t, err)
	assert.Equal(t, "SELECT foo,bar FROM table2 WHERE id = ?", query)
	assert.Len(t, postProcessors, 2)
	assert.NotNil(t, limiter)
}

func TestStructMapper_rowMapOptions_Errors(t *testing.T) {
	sm := MustNewStructMapper[testStruct](`foo,bar`)
	require.NotNil(t, sm)
	raw := sm.(*structMapper[testStruct])
	_, _, _, err := raw.rowMapOptions([]any{
		AddClause("WHERE id = ?"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "add clause must have a query set")

	_, _, _, err = raw.rowMapOptions([]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no default query")

	_, _, _, err = raw.rowMapOptions([]any{
		Query(" ,extra_col FROM table"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot forge extra columns using Query")

	_, _, _, err = raw.rowMapOptions([]any{
		"not a valid option",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown option type:")
}

func TestStructMapper_Rows(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"foo", "bar"}).
		AddRow("FOO value", "bar value").
		AddRow("FOO value 2", "bar value 2"))

	sm, err := NewStructMapper[testStruct](`foo,bar`,
		Query("FROM table"),
		ErrorOnUnknownColumns(true), ErrorOnUnMappedColumns(true),
		UseTagName("db"),
		&testPostProcessor[testStruct]{},
		&testColumnNamer{},
	)
	require.NoError(t, err)
	require.NotNil(t, sm)
	rows, err := sm.Rows(context.Background(), db, nil)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
	assert.Equal(t, "foo value", rows[0].Foo)
	assert.Equal(t, "bar value", rows[0].Bar)
	assert.Equal(t, "foo value 2", rows[1].Foo)
	assert.Equal(t, "bar value 2", rows[1].Bar)
}

func TestStructMapper_Rows_Repeated(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"foo", "bar"}).
		AddRow("FOO value", "bar value").
		AddRow("FOO value 2", "bar value 2"))

	sm, err := NewStructMapper[testStruct](`foo,bar`,
		Query("FROM table"),
		ErrorOnUnknownColumns(true), ErrorOnUnMappedColumns(true),
		UseTagName("db"),
		&testPostProcessor[testStruct]{},
		&testColumnNamer{},
	)
	require.NoError(t, err)
	require.NotNil(t, sm)
	rows, err := sm.Rows(context.Background(), db, nil)
	require.NoError(t, err)
	assert.Len(t, rows, 2)

	db2, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"foo", "bar"}).
		AddRow("FOO value", "bar value").
		AddRow("FOO value 2", "bar value 2"))

	rows, err = sm.Rows(context.Background(), db2, nil)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
}

func TestStructMapper_Rows_ComplexStruct(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"foo", "bar", "baz"}).
		AddRow("foo value", "bar value", "baz value").
		AddRow("foo value 2", "bar value 2", "baz value 2"))

	type Embedded struct {
		Foo string `sql:"foo"`
	}
	type subStruct struct {
		Bar string `sql:"bar"`
	}
	type complexStruct struct {
		Baz string `sql:"baz"`
		Embedded
		Sub subStruct
	}
	sm, err := NewStructMapper[complexStruct](`foo,bar,baz`,
		Query("FROM table"),
	)
	require.NoError(t, err)
	require.NotNil(t, sm)
	rows, err := sm.Rows(context.Background(), db, nil)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
	assert.Equal(t, "foo value", rows[0].Foo)
	assert.Equal(t, "baz value", rows[0].Baz)
	assert.Equal(t, "bar value", rows[0].Sub.Bar)
	assert.Equal(t, "foo value 2", rows[1].Foo)
	assert.Equal(t, "baz value 2", rows[1].Baz)
	assert.Equal(t, "bar value 2", rows[1].Sub.Bar)
}

func TestStructMapper_Rows_UnknownColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"foo", "bar"}).
		AddRow("foo value", "bar value").
		AddRow("foo value 2", "bar value 2"))

	type badStruct struct {
		Baz string `sql:"baz"`
	}
	sm, err := NewStructMapper[badStruct](`foo,bar`,
		Query("FROM table"),
		ErrorOnUnknownColumns(true),
	)
	require.NoError(t, err)
	require.NotNil(t, sm)
	_, err = sm.Rows(context.Background(), db, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown column")
}

func TestStructMapper_Rows_UnmappedColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"foo", "bar"}).
		AddRow("foo value", "bar value").
		AddRow("foo value 2", "bar value 2"))

	type badStruct struct {
		Foo string `sql:"foo"`
	}
	sm, err := NewStructMapper[badStruct](`foo,bar`,
		Query("FROM table"),
		ErrorOnUnMappedColumns(true),
	)
	require.NoError(t, err)
	require.NotNil(t, sm)
	_, err = sm.Rows(context.Background(), db, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmapped column")
}

func TestStructMapper_Rows_Limited(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"foo", "bar"}).
		AddRow("FOO value", "bar value").
		AddRow("FOO value 2", "bar value 2"))

	sm, err := NewStructMapper[testStruct](`foo,bar`,
		Query("FROM table"),
		ErrorOnUnknownColumns(true), ErrorOnUnMappedColumns(true),
		UseTagName("db"),
		&testPostProcessor[testStruct]{},
		&testColumnNamer{},
	)
	require.NoError(t, err)
	require.NotNil(t, sm)
	rows, err := sm.Rows(context.Background(), db, nil, &testLimiter{1})
	require.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "foo value", rows[0].Foo)
	assert.Equal(t, "bar value", rows[0].Bar)
}

func TestStructMapper_Rows_ErrorPostProcessor(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"foo", "bar"}).
		AddRow("FOO value", "bar value"))

	sm, err := NewStructMapper[testStruct](`foo,bar`,
		Query("FROM table"),
		&testErrorPostProcessor[testStruct]{},
	)
	require.NoError(t, err)
	require.NotNil(t, sm)
	_, err = sm.Rows(context.Background(), db, nil)
	require.Error(t, err)
}

func TestStructMapper_Iterate(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"foo", "bar"}).
		AddRow("FOO value", "bar value").
		AddRow("FOO value 2", "bar value 2"))

	sm, err := NewStructMapper[testStruct](`foo,bar`,
		Query("FROM table"),
		UseTagName("db"),
	)
	require.NoError(t, err)
	require.NotNil(t, sm)

	called := 0
	err = sm.Iterate(ctx, db, nil, func(row testStruct) (cont bool, err error) {
		called++
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, called)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStructMapper_Iterate_ErrorPostProcessor(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"foo", "bar"}).
		AddRow("FOO value", "bar value").
		AddRow("FOO value 2", "bar value 2"))

	sm, err := NewStructMapper[testStruct](`foo,bar`,
		Query("FROM table"),
		UseTagName("db"),
		&testErrorPostProcessor[testStruct]{},
	)
	require.NoError(t, err)
	require.NotNil(t, sm)

	called := 0
	err = sm.Iterate(ctx, db, nil, func(row testStruct) (cont bool, err error) {
		called++
		return true, nil
	})
	require.Error(t, err)
	require.Equal(t, 0, called)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStructMapper_FirstRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"foo", "bar"}).
		AddRow("FOO value", "bar value").
		AddRow("FOO value 2", "bar value 2"))

	sm, err := NewStructMapper[testStruct](`foo,bar`,
		Query("FROM table"),
		ErrorOnUnknownColumns(true), ErrorOnUnMappedColumns(true),
		UseTagName("db"),
		&testPostProcessor[testStruct]{},
		&testColumnNamer{},
	)
	require.NoError(t, err)
	require.NotNil(t, sm)
	row, err := sm.FirstRow(context.Background(), db, nil)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, "foo value", row.Foo)
	assert.Equal(t, "bar value", row.Bar)
}

func TestStructMapper_FirstRow_NoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"foo", "bar"}))

	sm, err := NewStructMapper[testStruct](`foo,bar`,
		Query("FROM table"),
		ErrorOnUnknownColumns(true), ErrorOnUnMappedColumns(true),
		UseTagName("db"),
		&testPostProcessor[testStruct]{},
		&testColumnNamer{},
	)
	require.NoError(t, err)
	require.NotNil(t, sm)
	row, err := sm.FirstRow(context.Background(), db, nil)
	require.NoError(t, err)
	require.Nil(t, row)
}

func TestStructMapper_FirstRow_ErrorPostProcessor(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"foo", "bar"}).
		AddRow("FOO value", "bar value").
		AddRow("FOO value 2", "bar value 2"))

	sm, err := NewStructMapper[testStruct](`foo,bar`,
		Query("FROM table"),
		ErrorOnUnknownColumns(true), ErrorOnUnMappedColumns(true),
		UseTagName("db"),
		&testErrorPostProcessor[testStruct]{},
	)
	require.NoError(t, err)
	require.NotNil(t, sm)
	_, err = sm.FirstRow(context.Background(), db, nil)
	require.Error(t, err)
}

func TestStructMapper_ExactlyOneRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"foo", "bar"}).
		AddRow("FOO value", "bar value").
		AddRow("FOO value 2", "bar value 2"))

	sm, err := NewStructMapper[testStruct](`foo,bar`,
		Query("FROM table"),
		ErrorOnUnknownColumns(true), ErrorOnUnMappedColumns(true),
		UseTagName("db"),
		&testPostProcessor[testStruct]{},
		&testColumnNamer{},
	)
	require.NoError(t, err)
	require.NotNil(t, sm)
	row, err := sm.ExactlyOneRow(context.Background(), db, nil)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, "foo value", row.Foo)
	assert.Equal(t, "bar value", row.Bar)
}

func TestStructMapper_ExactlyOneRow_NoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"foo", "bar"}))

	sm, err := NewStructMapper[testStruct](`foo,bar`,
		Query("FROM table"),
		ErrorOnUnknownColumns(true), ErrorOnUnMappedColumns(true),
		UseTagName("db"),
		&testPostProcessor[testStruct]{},
		&testColumnNamer{},
	)
	require.NoError(t, err)
	require.NotNil(t, sm)
	_, err = sm.ExactlyOneRow(context.Background(), db, nil)
	require.Error(t, err)
	assert.Equal(t, sql.ErrNoRows, err)
}

func TestStructMapper_ExactlyOneRow_ErrorPostProcessor(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"foo", "bar"}).
		AddRow("FOO value", "bar value").
		AddRow("FOO value 2", "bar value 2"))

	sm, err := NewStructMapper[testStruct](`foo,bar`,
		Query("FROM table"),
		ErrorOnUnknownColumns(true), ErrorOnUnMappedColumns(true),
		UseTagName("db"),
		&testErrorPostProcessor[testStruct]{},
	)
	require.NoError(t, err)
	require.NotNil(t, sm)
	_, err = sm.ExactlyOneRow(context.Background(), db, nil)
	require.Error(t, err)
}

func TestIsScannable(t *testing.T) {
	type testStruct struct{}
	testCases := []struct {
		value  any
		expect bool
	}{
		{
			value:  time.Now(),
			expect: true,
		},
		{
			value:  sql.NullString{},
			expect: true,
		},
		{
			value:  sql.NullInt16{},
			expect: true,
		},
		{
			value:  sql.NullInt32{},
			expect: true,
		},
		{
			value:  sql.NullInt64{},
			expect: true,
		},
		{
			value:  sql.NullFloat64{},
			expect: true,
		},
		{
			value:  sql.NullBool{},
			expect: true,
		},
		{
			value:  decimal.NewFromInt(0),
			expect: true,
		},
		{
			value: &testStruct{},
		},
		{
			value: testStruct{},
		},
		{
			value:  "",
			expect: true,
		},
		{
			value:  0,
			expect: true,
		},
		{
			value: nil,
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("[%d]", i+1), func(t *testing.T) {
			is := isScannable(reflect.TypeOf(tc.value))
			if tc.expect {
				assert.True(t, is)
			} else {
				assert.False(t, is)
			}
		})
	}
}

func TestWalkStruct(t *testing.T) {
	type EmbeddedStruct struct {
		Foo string `sql:"foo"`
	}
	type SubStruct struct {
		Foo string `sql:"foo"`
	}
	type testStructOk struct {
		private  string
		Unmapped *string
		EmbeddedStruct
	}
	tc := testStructOk{}
	rt := reflect.TypeOf(tc)
	err := walkStruct([]FieldColumnNamer{&defaultFieldColumnNamer{tagName: sqlTag}}, rt, make(map[string]struct{}))
	require.NoError(t, err)

	type testStructBad struct {
		Sub SubStruct
		EmbeddedStruct
	}
	tc2 := testStructBad{}
	rt = reflect.TypeOf(tc2)
	err = walkStruct([]FieldColumnNamer{&defaultFieldColumnNamer{tagName: sqlTag}}, rt, make(map[string]struct{}))
	require.Error(t, err)
}
