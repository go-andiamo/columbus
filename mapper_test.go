package columbus

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

var ctx = context.Background()

func TestNewMapper(t *testing.T) {
	m, err := NewMapper("a,b,c")
	require.NoError(t, err)
	require.NotNil(t, m)
	mt := m.(*mapper)
	require.Equal(t, "a,b,c", mt.cols)

	m, err = NewMapper([]string{"a", "b", "c"})
	require.NoError(t, err)
	require.NotNil(t, m)
	mt = m.(*mapper)
	require.Equal(t, "a,b,c", mt.cols)
}

func TestMustNewMapper(t *testing.T) {
	require.Panics(t, func() {
		_ = MustNewMapper("a,b,c", nil, "not a valid option")
	})
	require.NotPanics(t, func() {
		_ = MustNewMapper("a,b,c", nil)
	})
}

func TestNewMapper_WithOptions(t *testing.T) {
	_, err := NewMapper("a,b,c")
	require.NoError(t, err)

	m, err := NewMapper("a,b,c", Mappings{"a": {}, "b": {}})
	require.NoError(t, err)
	mt := m.(*mapper)
	require.Equal(t, 2, len(mt.mappings))

	drpp := &dummyRowPostProcessor{}
	m, err = NewMapper("a,b,c", drpp)
	require.NoError(t, err)
	mt = m.(*mapper)
	require.Equal(t, 1, len(mt.rowPostProcessors))

	sq := NewSubQuery("", "", nil, nil, false)
	m, err = NewMapper("a,b,c", sq)
	require.NoError(t, err)
	mt = m.(*mapper)
	require.Equal(t, 1, len(mt.rowSubQueries))

	q := Query(`FROM table WHERE id = ?`)
	m, err = NewMapper("a,b,c", q)
	require.NoError(t, err)
	mt = m.(*mapper)
	require.NotNil(t, mt.defaultQuery)

	m, err = NewMapper("a,b,c")
	require.NoError(t, err)
	mt = m.(*mapper)
	require.True(t, mt.useDecimals)
	m, err = NewMapper("a,b,c", UseDecimals(false))
	require.NoError(t, err)
	mt = m.(*mapper)
	require.False(t, mt.useDecimals)

	_, err = NewMapper("a,b,c", "not a valid option")
	require.Error(t, err)
	require.Equal(t, "unknown option type: string", err.Error())
}

func TestNewMapper_WithOptions_ErrorsWithMultipleDefaultQueries(t *testing.T) {
	q := Query(`FROM table WHERE id = ?`)
	_, err := NewMapper("a,b,c", q)
	require.NoError(t, err)
	_, err = NewMapper("a,b,c", q, q)
	require.Error(t, err)
	require.Equal(t, "cannot use multiple default queries", err.Error())
}

func TestMapper_rowMapOptions_query(t *testing.T) {
	m, err := newMapper("a,b,c")
	require.NoError(t, err)
	require.Nil(t, m.defaultQuery)
	_, _, _, _, _, _, err = m.rowMapOptions()
	require.Error(t, err)
	require.Equal(t, "no default query", err.Error())

	m, err = newMapper("a,b,c", Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)
	require.NotNil(t, m.defaultQuery)
	q, _, _, _, _, _, err := m.rowMapOptions()
	require.NoError(t, err)
	require.Equal(t, "SELECT a,b,c FROM table WHERE id = ?", q)

	useQuery := Query(`FROM other_table WHERE other_id = ?`)
	q, _, _, _, _, _, err = m.rowMapOptions(useQuery)
	require.NoError(t, err)
	require.Equal(t, "SELECT a,b,c FROM other_table WHERE other_id = ?", q)

	addClause := AddClause(`ORDER BY id`)
	q, _, _, _, _, _, err = m.rowMapOptions(addClause)
	require.NoError(t, err)
	require.Equal(t, "SELECT a,b,c FROM table WHERE id = ? ORDER BY id", q)

	q, _, _, _, _, _, err = m.rowMapOptions(useQuery, addClause)
	require.NoError(t, err)
	require.Equal(t, "SELECT a,b,c FROM other_table WHERE other_id = ? ORDER BY id", q)

	m, err = newMapper("a,b,c")
	require.NoError(t, err)
	_, _, _, _, _, _, err = m.rowMapOptions(addClause)
	require.Error(t, err)
	require.Equal(t, "add clause must have a query set", err.Error())
}

func TestMapper_rowMapOptions_mappings(t *testing.T) {
	m, err := newMapper("a,b,c", Mappings{
		"a": {
			PropertyName: "aaa",
		},
	}, Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)
	require.NotNil(t, m.defaultQuery)
	_, mappings, _, _, _, _, err := m.rowMapOptions()
	require.NoError(t, err)
	require.Equal(t, 1, len(mappings))

	_, mappings, _, _, _, _, err = m.rowMapOptions(Mappings{
		"b": Mapping{},
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(mappings))

	_, mappings, _, _, _, _, err = m.rowMapOptions(Mappings{"a": Mapping{}}, Mappings{"b": Mapping{}})
	require.NoError(t, err)
	require.Equal(t, 2, len(mappings))
}

func TestMapper_rowMapOptions_postProcesses(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)
	require.NotNil(t, m.defaultQuery)
	_, _, postProcesses, _, _, _, err := m.rowMapOptions()
	require.NoError(t, err)
	require.Empty(t, postProcesses)

	_, _, postProcesses, _, _, _, err = m.rowMapOptions(&dummyRowPostProcessor{})
	require.NoError(t, err)
	require.Equal(t, 1, len(postProcesses))

	_, _, postProcesses, _, _, _, err = m.rowMapOptions(&dummyRowPostProcessor{}, &dummyRowPostProcessor{})
	require.NoError(t, err)
	require.Equal(t, 2, len(postProcesses))
}

func TestMapper_rowMapOptions_subQueries(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)
	require.NotNil(t, m.defaultQuery)
	_, _, _, subQueries, _, _, err := m.rowMapOptions()
	require.NoError(t, err)
	require.Empty(t, subQueries)

	sq1 := NewSubQuery("", "", nil, nil, false)
	sq2 := NewObjectSubQuery("", "", nil, nil, false, true)
	_, _, _, subQueries, _, _, err = m.rowMapOptions(sq1, sq2)
	require.NoError(t, err)
	require.Equal(t, 2, len(subQueries))
}

func TestMapper_rowMapOptions_excludeProperties(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)
	require.NotNil(t, m.defaultQuery)
	_, _, _, _, exclusions, _, err := m.rowMapOptions()
	require.NoError(t, err)
	require.Empty(t, exclusions)

	_, _, _, _, exclusions, _, err = m.rowMapOptions(AllowedProperties{"a": nil})
	require.NoError(t, err)
	require.Equal(t, 1, len(exclusions))

	_, _, _, _, exclusions, _, err = m.rowMapOptions(AllowedProperties{"a": nil}, AllowedProperties{"b": nil})
	require.NoError(t, err)
	require.Equal(t, 2, len(exclusions))

	_, _, _, _, exclusions, _, err = m.rowMapOptions(PropertyExclusions{AllowedProperties{"a": nil}, AllowedProperties{"b": nil}})
	require.NoError(t, err)
	require.Equal(t, 2, len(exclusions))

	excfn := func(property string, path []string) bool { return false }
	_, _, _, _, exclusions, _, err = m.rowMapOptions(excfn)
	require.NoError(t, err)
	require.Equal(t, 1, len(exclusions))
}

func TestMapper_rowMapOptions_limiter(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)
	require.NotNil(t, m.defaultQuery)
	_, _, _, _, _, limiter, err := m.rowMapOptions()
	require.NoError(t, err)
	require.NotNil(t, limiter)
	require.IsType(t, &nullLimiter{}, limiter)

	opt := &testLimiter{2}
	_, _, _, _, _, limiter, err = m.rowMapOptions(opt)
	require.NoError(t, err)
	require.NotNil(t, limiter)
	require.IsType(t, &testLimiter{}, limiter)
}

type testLimiter struct {
	limit int
}

func (n *testLimiter) LimitReached(rowCount int) bool {
	return rowCount > n.limit
}

func TestMapper_Rows(t *testing.T) {
	m, err := newMapper("a", Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value").AddRow("a value 2"))

	rows, err := m.Rows(ctx, db, nil)
	require.NoError(t, err)
	require.Len(t, rows, 2)
}

func TestMapper_Rows_Limited(t *testing.T) {
	m, err := newMapper("a", Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value").AddRow("a value 2"))

	rows, err := m.Rows(ctx, db, nil, &testLimiter{1})
	require.NoError(t, err)
	require.Len(t, rows, 1)
}

func TestMapper_Rows_SqlErrors(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnError(errors.New("foo"))

	_, err = m.Rows(ctx, db, nil)
	require.Error(t, err)
	require.Equal(t, "foo", err.Error())
}

func TestMapper_Rows_MapRowErrors(t *testing.T) {
	m, err := newMapper("a", Mappings{
		"a": {
			PostProcess: func(ctx context.Context, sqli SqlInterface, row map[string]any, value any) (bool, any, error) {
				return false, nil, errors.New("foo")
			},
		},
	}, Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value"))

	_, err = m.Rows(ctx, db, nil)
	require.Error(t, err)
	require.Equal(t, "foo", err.Error())
}

func TestMapper_FirstRow_SqlErrors(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnError(errors.New("foo"))

	_, err = m.FirstRow(ctx, db, nil)
	require.Error(t, err)
	require.Equal(t, "foo", err.Error())
}

func TestMapper_ExactlyOneRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	m, err := newMapper("a", Query(`FROM table`))
	require.NoError(t, err)

	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a", "b", "c"}).AddRow(
		"a value",
		int64(16),
		float64(16)))

	row, err := m.ExactlyOneRow(ctx, db, nil)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NotNil(t, row)

	assert.Equal(t, "a value", row["a"])
	assert.Equal(t, int64(16), row["b"])
	assert.Equal(t, float64(16), row["c"])
}

func TestMapper_ExactlyOneRow_SqlErrors(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnError(errors.New("foo"))

	_, err = m.ExactlyOneRow(ctx, db, nil)
	require.Error(t, err)
	require.Equal(t, "foo", err.Error())
}

func TestMapper_Rows_OptionsErrors(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)

	_, err = m.Rows(ctx, nil, nil, "not a valid option")
	require.Error(t, err)
	require.Equal(t, "unknown option type: string", err.Error())
}

func TestMapper_FirstRow_OptionsErrors(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)

	_, err = m.FirstRow(ctx, nil, nil, "not a valid option")
	require.Error(t, err)
	require.Equal(t, "unknown option type: string", err.Error())
}

func TestMapper_ExactlyOneRow_OptionsErrors(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)

	_, err = m.ExactlyOneRow(ctx, nil, nil, "not a valid option")
	require.Error(t, err)
	require.Equal(t, "unknown option type: string", err.Error())
}

func TestMapper_FirstRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	m, err := newMapper("a", Query(`FROM table`))
	require.NoError(t, err)

	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a", "b", "c"}).AddRow(
		"a value",
		int64(16),
		float64(16)))

	row, err := m.FirstRow(ctx, db, nil)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NotNil(t, row)

	assert.Equal(t, "a value", row["a"])
	assert.Equal(t, int64(16), row["b"])
	assert.Equal(t, float64(16), row["c"])
}

func TestMapper_FirstRow_CalledTwice(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	m, err := newMapper("a", Query(`FROM table`))
	require.NoError(t, err)

	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value"))
	row, err := m.FirstRow(ctx, db, nil)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NotNil(t, row)

	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value"))
	row, err = m.FirstRow(ctx, db, nil)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NotNil(t, row)
	assert.Equal(t, "a value", row["a"])
}

func TestMapper_PropertyExclusions(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	m, err := newMapper("a,b,c", Query(`FROM table`))
	require.NoError(t, err)

	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a", "b", "c"}).AddRow("a value", "b value", "c value"))
	row, err := m.FirstRow(ctx, db, nil)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NotNil(t, row)
	require.Equal(t, 3, len(row))
	require.True(t, hasProperties(row, "a", "b", "c"))

	allow := AllowedProperties{"a": nil, "b": nil, "cxxx": nil}
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a", "b", "c"}).AddRow("a value", "b value", "c value"))
	row, err = m.FirstRow(ctx, db, nil, allow)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NotNil(t, row)
	require.Equal(t, 2, len(row))
	require.True(t, hasProperties(row, "a", "b"))

	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a", "b", "c"}).AddRow("a value", "b value", "c value"))
	row, err = m.FirstRow(ctx, db, nil, allow, Mappings{
		"c": {PropertyName: "cxxx"},
	})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NotNil(t, row)
	require.Equal(t, 3, len(row))
	require.True(t, hasProperties(row, "a", "b", "cxxx"))
}

func TestMapper_Mapping_PropertyName(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	m, err := newMapper("a", Mappings{
		"a": {
			PropertyName: "foo",
		},
	}, Query(`FROM table`))
	require.NoError(t, err)

	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value"))

	row, err := m.FirstRow(ctx, db, nil)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NotNil(t, row)

	assert.Equal(t, 1, len(row))
	assert.Equal(t, "a value", row["foo"])
}

func TestMapper_Mapping_OmitNull(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	m, err := newMapper("a", Mappings{
		"a": {
			OmitNull: true,
		},
	}, Query(`FROM table`))
	require.NoError(t, err)

	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow(nil))

	row, err := m.FirstRow(ctx, db, nil)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NotNil(t, row)

	assert.Equal(t, 0, len(row))
}

func TestMapper_Mapping_NullDefault(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	m, err := newMapper("a", Mappings{
		"a": {
			NullDefault: "foo",
		},
	}, Query(`FROM table`))
	require.NoError(t, err)

	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow(nil))

	row, err := m.FirstRow(ctx, db, nil)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NotNil(t, row)

	assert.Equal(t, 1, len(row))
	assert.Equal(t, "foo", row["a"])
}

func TestMapper_Mapping_Path(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	m, err := newMapper("a,b", Mappings{
		"a": {
			Path: []string{"x", "y"},
		},
		"b": {
			Path: []string{"x", "y"},
		},
	}, Query(`FROM table`))
	require.NoError(t, err)

	mock.ExpectQuery("SELECT a,b FROM table").WillReturnRows(sqlmock.NewRows([]string{"a", "b"}).AddRow("foo", "bar"))

	row, err := m.FirstRow(ctx, db, nil)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NotNil(t, row)

	assert.Equal(t, 1, len(row))
	obj := row["x"].(map[string]any)
	assert.Equal(t, 1, len(obj))
	obj = obj["y"].(map[string]any)
	assert.Equal(t, 2, len(obj))
	assert.True(t, hasProperties(obj, "a", "b"))
	assert.Equal(t, "foo", obj["a"])
	assert.Equal(t, "bar", obj["b"])
}

func TestMapper_Mapping_PostProcess(t *testing.T) {
	m, err := newMapper("a", Mappings{
		"a": {
			PostProcess: func(ctx context.Context, sqli SqlInterface, row map[string]any, value any) (bool, any, error) {
				return true, "replaced value", nil
			},
		},
	}, Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value"))

	row, err := m.FirstRow(ctx, db, nil)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NotNil(t, row)
	assert.Equal(t, "replaced value", row["a"])
}

func TestMapper_SubQuery(t *testing.T) {
	m, err := newMapper("a",
		Query(`FROM table`),
		NewSubQuery("foo", `SELECT b FROM sub_table WHERE a = ?`, []string{"a"}, nil, false),
	)
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("SELECT a FROM table").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value"))
	mock.ExpectQuery("SELECT b FROM sub_table WHERE a = ?").WithArgs("a value").WillReturnRows(sqlmock.NewRows([]string{"b"}).AddRow("b value"))

	row, err := m.FirstRow(ctx, db, nil)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NotNil(t, row)
	assert.Equal(t, 2, len(row))
	assert.Equal(t, "a value", row["a"])
	assert.True(t, hasProperties(row, "a", "foo"))
	subs := row["foo"].([]map[string]any)
	assert.Equal(t, 1, len(subs))
	sub := subs[0]
	assert.Equal(t, "b value", sub["b"])
}

func TestMapper_SubQuery_Excluded(t *testing.T) {
	m, err := newMapper("a",
		Query(`FROM table`),
		NewSubQuery("foo", `SELECT b FROM sub_table WHERE a = ?`, []string{"a"}, nil, false),
	)
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("SELECT a FROM table").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value"))
	//mock.ExpectQuery("SELECT b FROM sub_table WHERE a = ?").WithArgs("a value").WillReturnRows(sqlmock.NewRows([]string{"b"}).AddRow("b value"))

	excluder := AllowedProperties{"a": nil}
	row, err := m.FirstRow(ctx, db, nil, excluder)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NotNil(t, row)
	assert.Equal(t, 1, len(row))
	assert.Equal(t, "a value", row["a"])
}

func TestMapper_SubQuery_SqlErrors(t *testing.T) {
	m, err := newMapper("a",
		Query(`FROM table`),
		NewSubQuery("foo", `SELECT b FROM sub_table WHERE a = ?`, []string{"a"}, nil, false),
	)
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("SELECT a FROM table").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value"))
	mock.ExpectQuery("SELECT b FROM sub_table WHERE a = ?").WithArgs("a value").WillReturnError(errors.New("fooey"))

	_, err = m.FirstRow(ctx, db, nil)
	require.Error(t, err)
	require.Equal(t, "fooey", err.Error())
}

type testRowPostProcessor struct {
	propertyName string
}

var _ RowPostProcessor = &testRowPostProcessor{}

func (rp *testRowPostProcessor) PostProcess(ctx context.Context, sqli SqlInterface, row map[string]any) error {
	const query = `SELECT b FROM sub_table WHERE a = ?`
	rows, err := sqli.QueryContext(ctx, query, row["a"])
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	row[rp.propertyName] = true
	return nil
}

func (rp *testRowPostProcessor) ProvidesProperty() string {
	return rp.propertyName
}

func TestMapper_RowPostProcessor(t *testing.T) {
	m, err := newMapper("a",
		Query(`FROM table`),
		&testRowPostProcessor{propertyName: "foo"},
	)
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("SELECT a FROM table").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value"))
	mock.ExpectQuery("SELECT b FROM sub_table WHERE a = ?").WithArgs("a value").WillReturnRows(sqlmock.NewRows([]string{"b"}).AddRow("b value"))

	row, err := m.FirstRow(ctx, db, nil)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NotNil(t, row)
	assert.Equal(t, 2, len(row))
	assert.True(t, hasProperties(row, "a", "foo"))
	assert.Equal(t, "a value", row["a"])
	assert.True(t, row["foo"].(bool))
}

func TestMapper_RowPostProcessor_Excluded(t *testing.T) {
	m, err := newMapper("a",
		Query(`FROM table`),
		&testRowPostProcessor{propertyName: "foo"},
	)
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("SELECT a FROM table").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value"))
	//mock.ExpectQuery("SELECT b FROM sub_table WHERE a = ?").WithArgs("a value").WillReturnRows(sqlmock.NewRows([]string{"b"}).AddRow("b value"))

	excluder := AllowedProperties{"a": nil}
	row, err := m.FirstRow(ctx, db, nil, excluder)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NotNil(t, row)
	assert.Equal(t, 1, len(row))
	assert.Equal(t, "a value", row["a"])
}

func TestMapper_RowPostProcessor_SqlErrors(t *testing.T) {
	m, err := newMapper("a",
		Query(`FROM table`),
		&testRowPostProcessor{propertyName: "foo"},
	)
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("SELECT a FROM table").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value"))
	mock.ExpectQuery("SELECT b FROM sub_table WHERE a = ?").WithArgs("a value").WillReturnError(errors.New("fooey"))

	_, err = m.FirstRow(ctx, db, nil)
	require.Error(t, err)
	require.Equal(t, "fooey", err.Error())
}

func TestMapper_WriteRows(t *testing.T) {
	m, err := newMapper("a", Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value").AddRow("a value 2"))

	w := bytes.NewBuffer(nil)
	err = m.WriteRows(ctx, w, db, nil)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.Equal(t, "[{\"a\":\"a value\"}\n,{\"a\":\"a value 2\"}\n]", w.String())
}

func TestMapper_WriteRows_Limited(t *testing.T) {
	m, err := newMapper("a", Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value").AddRow("a value 2"))

	w := bytes.NewBuffer(nil)
	err = m.WriteRows(ctx, w, db, nil, &testLimiter{1})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.Equal(t, "[{\"a\":\"a value\"}\n]", w.String())
}

func TestMapper_WriteRows_SqlErrors(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnError(errors.New("foo"))

	err = m.WriteRows(ctx, nil, db, nil)
	require.Error(t, err)
	require.Equal(t, "foo", err.Error())
}

func TestMapper_WriteRows_OptionsErrors(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)

	err = m.WriteRows(ctx, nil, nil, nil, "not a valid option")
	require.Error(t, err)
	require.Equal(t, "unknown option type: string", err.Error())
}

func TestMapper_WriteFirstRow(t *testing.T) {
	m, err := newMapper("a", Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value").AddRow("a value 2"))

	w := bytes.NewBuffer(nil)
	err = m.WriteFirstRow(ctx, w, db, nil)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.Equal(t, "{\"a\":\"a value\"}\n", w.String())
}

func TestMapper_WriteFirstRow_SqlErrors(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnError(errors.New("foo"))

	err = m.WriteFirstRow(ctx, nil, db, nil)
	require.Error(t, err)
	require.Equal(t, "foo", err.Error())
}

func TestMapper_WriteFirstRow_OptionsErrors(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)

	err = m.WriteFirstRow(ctx, nil, nil, nil, "not a valid option")
	require.Error(t, err)
	require.Equal(t, "unknown option type: string", err.Error())
}

func TestMapper_WriteExactlyOneRow(t *testing.T) {
	m, err := newMapper("a", Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value").AddRow("a value 2"))

	w := bytes.NewBuffer(nil)
	err = m.WriteExactlyOneRow(ctx, w, db, nil)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	require.Equal(t, "{\"a\":\"a value\"}\n", w.String())
}

func TestMapper_WriteExactlyOneRow_ErrorNoRow(t *testing.T) {
	m, err := newMapper("a", Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a"}))

	w := bytes.NewBuffer(nil)
	err = m.WriteExactlyOneRow(ctx, w, db, nil)
	require.Error(t, err)
	require.Equal(t, sql.ErrNoRows, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMapper_WriteExactlyOneRow_SqlErrors(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnError(errors.New("foo"))

	err = m.WriteExactlyOneRow(ctx, nil, db, nil)
	require.Error(t, err)
	require.Equal(t, "foo", err.Error())
}

func TestMapper_WriteExactlyOneRow_OptionsErrors(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)

	err = m.WriteExactlyOneRow(ctx, nil, nil, nil, "not a valid option")
	require.Error(t, err)
	require.Equal(t, "unknown option type: string", err.Error())
}

func TestMapper_Iterate(t *testing.T) {
	m, err := newMapper("a", Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value").AddRow("a value 2"))

	called := 0
	err = m.Iterate(ctx, db, nil, func(row map[string]any) (cont bool, err error) {
		called++
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, called)
}

func TestMapper_Iterate_OptionsErrors(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)

	err = m.Iterate(ctx, nil, nil, nil, "not a valid option")
	require.Error(t, err)
	require.Equal(t, "unknown option type: string", err.Error())
}

func TestMapper_Iterate_SqlErrors(t *testing.T) {
	m, err := newMapper("a,b,c", Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnError(errors.New("foo"))

	err = m.Iterate(ctx, db, nil, nil)
	require.Error(t, err)
	require.Equal(t, "foo", err.Error())
}

func TestMapper_Iterate_MapRowErrors(t *testing.T) {
	m, err := newMapper("a", Mappings{
		"a": {
			PostProcess: func(ctx context.Context, sqli SqlInterface, row map[string]any, value any) (bool, any, error) {
				return false, nil, errors.New("foo")
			},
		},
	}, Query(`FROM table`))
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	_ = mock
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("a value"))

	err = m.Iterate(ctx, db, nil, func(row map[string]any) (cont bool, err error) {
		return false, err
	})
	require.Error(t, err)
	require.Equal(t, "foo", err.Error())
}

func TestMapper_Extend(t *testing.T) {
	m, err := NewMapper("a",
		Mappings{"a": {Path: []string{"sub_obj"}}},
		Query(`FROM table`),
		&dummyRowPostProcessor{},
		NewSubQuery("x", `SELECT * FROM other`, nil, nil, false),
	)
	require.NoError(t, err)
	require.NotNil(t, m)
	mt := m.(*mapper)
	require.Equal(t, "SELECT a FROM table", string(*mt.defaultQuery))
	require.Equal(t, "a", mt.cols)
	require.Equal(t, 1, len(mt.rowPostProcessors))
	require.Equal(t, 1, len(mt.rowSubQueries))
	require.Equal(t, 1, len(mt.mappings))

	m2, err := m.Extend([]string{"b", "c"},
		Mappings{"b": {Path: []string{"sub_obj"}}},
		Query(`FROM other_table`),
		UseDecimals(false),
		&dummyRowPostProcessor{},
		NewSubQuery("x", `SELECT * FROM other`, nil, nil, false),
	)
	require.NoError(t, err)
	require.NotNil(t, m2)
	m2t := m2.(*mapper)
	require.Equal(t, m2, m2t)
	require.Equal(t, "SELECT a,b,c FROM other_table", string(*m2t.defaultQuery))
	require.Equal(t, "a,b,c", m2t.cols)
	require.Equal(t, 2, len(m2t.rowPostProcessors))
	require.Equal(t, 2, len(m2t.rowSubQueries))
	require.Equal(t, 2, len(m2t.mappings))
	require.False(t, m2t.useDecimals)

	_, err = m.Extend(nil, nil, Query(`FROM other_table`), Query(`FROM other_table`))
	require.Error(t, err)

	m, err = NewMapper("")
	require.NoError(t, err)
	require.NotNil(t, m)
	m2, err = m.Extend([]string{"a", "b"}, nil)
	require.NoError(t, err)
	require.NotNil(t, m2)
	m2t = m2.(*mapper)
	require.Equal(t, m2, m2t)
	require.Equal(t, "a,b", m2t.cols)
}

func hasProperties(obj map[string]any, keys ...string) bool {
	for _, key := range keys {
		if _, ok := obj[key]; !ok {
			return false
		}
	}
	return true
}
