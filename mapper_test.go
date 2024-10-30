package columbus

import (
	"context"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

var ctx = context.Background()

func TestNewMapper(t *testing.T) {
	m, err := NewMapper("a,b,c", nil)
	require.NoError(t, err)
	require.NotNil(t, m)
	mt := m.(*mapper)
	require.Equal(t, "a,b,c", mt.cols)

	m, err = NewMapper([]string{"a", "b", "c"}, nil)
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
	_, err := NewMapper("a,b,c", nil, nil)
	require.NoError(t, err)

	drpp := &dummyRowPostProcessor{}
	m, err := NewMapper("a,b,c", nil, drpp)
	require.NoError(t, err)
	mt := m.(*mapper)
	require.Equal(t, 1, len(mt.rowPostProcessors))

	sq := NewSubQuery("", "", nil, nil, false)
	m, err = NewMapper("a,b,c", nil, sq)
	require.NoError(t, err)
	mt = m.(*mapper)
	require.Equal(t, 1, len(mt.rowSubQueries))

	q := Query(`FROM table WHERE id = ?`)
	m, err = NewMapper("a,b,c", nil, q)
	require.NoError(t, err)
	mt = m.(*mapper)
	require.NotNil(t, mt.defaultQuery)

	_, err = NewMapper("a,b,c", nil, "not a valid option")
	require.Error(t, err)
	require.Equal(t, "unknown option type: string", err.Error())
}

func TestNewMapper_WithOptions_ErrorsWithMultipleDefaultQueries(t *testing.T) {
	q := Query(`FROM table WHERE id = ?`)
	_, err := NewMapper("a,b,c", nil, q)
	require.NoError(t, err)
	_, err = NewMapper("a,b,c", nil, q, q)
	require.Error(t, err)
	require.Equal(t, "cannot use multiple default queries", err.Error())
}

func TestMapper_rowMapOptions_query(t *testing.T) {
	m, err := newMapper("a,b,c", nil, nil)
	require.NoError(t, err)
	require.Nil(t, m.defaultQuery)
	_, _, _, _, _, err = m.rowMapOptions()
	require.Error(t, err)
	require.Equal(t, "no default query", err.Error())

	m, err = newMapper("a,b,c", nil, Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)
	require.NotNil(t, m.defaultQuery)
	q, _, _, _, _, err := m.rowMapOptions()
	require.NoError(t, err)
	require.Equal(t, "SELECT a,b,c FROM table WHERE id = ?", q)

	useQuery := Query(`FROM other_table WHERE other_id = ?`)
	q, _, _, _, _, err = m.rowMapOptions(useQuery)
	require.NoError(t, err)
	require.Equal(t, "SELECT a,b,c FROM other_table WHERE other_id = ?", q)

	addClause := AddClause(`ORDER BY id`)
	q, _, _, _, _, err = m.rowMapOptions(addClause)
	require.NoError(t, err)
	require.Equal(t, "SELECT a,b,c FROM table WHERE id = ? ORDER BY id", q)

	q, _, _, _, _, err = m.rowMapOptions(useQuery, addClause)
	require.NoError(t, err)
	require.Equal(t, "SELECT a,b,c FROM other_table WHERE other_id = ? ORDER BY id", q)

	m, err = newMapper("a,b,c", nil, nil)
	require.NoError(t, err)
	_, _, _, _, _, err = m.rowMapOptions(addClause)
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
	_, mappings, _, _, _, err := m.rowMapOptions()
	require.NoError(t, err)
	require.Equal(t, 1, len(mappings))

	_, mappings, _, _, _, err = m.rowMapOptions(Mappings{
		"b": Mapping{},
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(mappings))

	_, mappings, _, _, _, err = m.rowMapOptions(Mappings{"a": Mapping{}}, Mappings{"b": Mapping{}})
	require.NoError(t, err)
	require.Equal(t, 2, len(mappings))
}

func TestMapper_rowMapOptions_postProcesses(t *testing.T) {
	m, err := newMapper("a,b,c", nil, Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)
	require.NotNil(t, m.defaultQuery)
	_, _, postProcesses, _, _, err := m.rowMapOptions()
	require.NoError(t, err)
	require.Empty(t, postProcesses)

	_, _, postProcesses, _, _, err = m.rowMapOptions(&dummyRowPostProcessor{})
	require.NoError(t, err)
	require.Equal(t, 1, len(postProcesses))

	_, _, postProcesses, _, _, err = m.rowMapOptions(&dummyRowPostProcessor{}, &dummyRowPostProcessor{})
	require.NoError(t, err)
	require.Equal(t, 2, len(postProcesses))
}

func TestMapper_rowMapOptions_subQueries(t *testing.T) {
	m, err := newMapper("a,b,c", nil, Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)
	require.NotNil(t, m.defaultQuery)
	_, _, _, subQueries, _, err := m.rowMapOptions()
	require.NoError(t, err)
	require.Empty(t, subQueries)

	sq1 := NewSubQuery("", "", nil, nil, false)
	sq2 := NewObjectSubQuery("", "", nil, nil, false, true)
	_, _, _, subQueries, _, err = m.rowMapOptions(sq1, sq2)
	require.NoError(t, err)
	require.Equal(t, 2, len(subQueries))
}

func TestMapper_rowMapOptions_excludeProperties(t *testing.T) {
	m, err := newMapper("a,b,c", nil, Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)
	require.NotNil(t, m.defaultQuery)
	_, _, _, _, exclusions, err := m.rowMapOptions()
	require.NoError(t, err)
	require.Empty(t, exclusions)

	_, _, _, _, exclusions, err = m.rowMapOptions(ExcludeProperties{"a": nil})
	require.NoError(t, err)
	require.Equal(t, 1, len(exclusions))

	_, _, _, _, exclusions, err = m.rowMapOptions(ExcludeProperties{"a": nil}, ExcludeProperties{"b": nil})
	require.NoError(t, err)
	require.Equal(t, 2, len(exclusions))
}

func TestMapper_Rows_SqlErrors(t *testing.T) {
	m, err := newMapper("a,b,c", nil, Query(`FROM table`))
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
			PostProcess: func(ctx context.Context, sqli SqlInterface, row map[string]any, value any) (any, error) {
				return nil, errors.New("foo")
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
	m, err := newMapper("a,b,c", nil, Query(`FROM table`))
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

func TestMapper_ExactlyOneRow_SqlErrors(t *testing.T) {
	m, err := newMapper("a,b,c", nil, Query(`FROM table`))
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
	m, err := newMapper("a,b,c", nil, Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)

	_, err = m.Rows(ctx, nil, nil, "not a valid option")
	require.Error(t, err)
	require.Equal(t, "unknown option type: string", err.Error())
}

func TestMapper_FirstRow_OptionsErrors(t *testing.T) {
	m, err := newMapper("a,b,c", nil, Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)

	_, err = m.FirstRow(ctx, nil, nil, "not a valid option")
	require.Error(t, err)
	require.Equal(t, "unknown option type: string", err.Error())
}

func TestMapper_ExactlyOneRow_OptionsErrors(t *testing.T) {
	m, err := newMapper("a,b,c", nil, Query(`FROM table WHERE id = ?`))
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

	m, err := newMapper("a", nil, Query(`FROM table`))
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

	m, err := newMapper("a", nil, Query(`FROM table`))
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
	assert.Equal(t, "foo", obj["a"])
	assert.Equal(t, "bar", obj["b"])
}

func TestMapper_Mapping_PostProcess(t *testing.T) {
	m, err := newMapper("a", Mappings{
		"a": {
			PostProcess: func(ctx context.Context, sqli SqlInterface, row map[string]any, value any) (any, error) {
				return "replaced value", nil
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
	m, err := newMapper("a", nil,
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
	subs := row["foo"].([]map[string]any)
	assert.Equal(t, 1, len(subs))
	sub := subs[0]
	assert.Equal(t, "b value", sub["b"])
}

func TestMapper_SubQuery_SqlErrors(t *testing.T) {
	m, err := newMapper("a", nil,
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
