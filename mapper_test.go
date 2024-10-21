package columbus

import (
	"context"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
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
	m, err := newMapper("a,b,c", nil, Query(`FROM table WHERE id = ?`))
	require.NoError(t, err)
	require.NotNil(t, m.defaultQuery)
	_, mappings, _, _, _, err := m.rowMapOptions()
	require.NoError(t, err)
	require.Empty(t, mappings)

	_, mappings, _, _, _, err = m.rowMapOptions(Mappings{"a": Mapping{}})
	require.NoError(t, err)
	require.Equal(t, 1, len(mappings))

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
