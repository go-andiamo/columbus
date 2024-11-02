package columbus

import (
	"database/sql"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestNewColumnsInfo(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"a", "b", "c"}).AddRow(
		"a value",
		int64(16),
		float64(16)))
	rows, err := db.QueryContext(ctx, `SELECT a,b,c FROM table`)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	defer func() {
		_ = rows.Close()
	}()

	info, err := newColumnsInfo(rows, nil)
	require.NoError(t, err)
	require.NotNil(t, info)
}

func TestColumnsInfo_Reader_MappingScanner(t *testing.T) {
	ci := &columnsInfo{
		count: 1,
		names: []string{"a"},
		mappings: Mappings{
			"a": {
				Scanner: func(src any) (value any, err error) {
					return src, nil
				},
			},
		},
	}
	r := ci.reader()
	require.NotNil(t, r)
	require.Equal(t, 1, r.count)
	require.Equal(t, 1, len(r.names))
	require.Equal(t, 1, len(r.scanArgs))
	require.Equal(t, 1, len(r.values))
	require.IsType(t, &customColumnScanner{}, r.scanArgs[0])

	s := r.scanArgs[0].(sql.Scanner)
	err := s.Scan("foo")
	require.NoError(t, err)
	require.Equal(t, "foo", r.values[0])
}

func TestColumnsInfo_Reader_Json(t *testing.T) {
	ci := &columnsInfo{
		count:   1,
		names:   []string{"a"},
		dbTypes: []string{"JSON"},
	}
	r := ci.reader()
	require.NotNil(t, r)
	require.Equal(t, 1, r.count)
	require.Equal(t, 1, len(r.names))
	require.Equal(t, 1, len(r.scanArgs))
	require.Equal(t, 1, len(r.values))
	require.IsType(t, &jsonColumnScanner{}, r.scanArgs[0])

	s := r.scanArgs[0].(sql.Scanner)
	err := s.Scan(`{"foo":"bar"}`)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"foo": "bar"}, r.values[0])
	err = s.Scan(`{not valid json}`)
	require.Error(t, err)
	err = s.Scan([]byte(`["foo"]`))
	require.NoError(t, err)
	require.Equal(t, []any{"foo"}, r.values[0])
	err = s.Scan([]byte(`[not valid json]`))
	require.Error(t, err)
	err = s.Scan(nil)
	require.NoError(t, err)
	require.Equal(t, nil, r.values[0])

}

func TestColumnsInfo_Reader_Decimal(t *testing.T) {
	ci := &columnsInfo{
		count:   1,
		names:   []string{"a"},
		dbTypes: []string{"DECIMAL"},
	}
	r := ci.reader()
	require.NotNil(t, r)
	require.Equal(t, 1, r.count)
	require.Equal(t, 1, len(r.names))
	require.Equal(t, 1, len(r.scanArgs))
	require.Equal(t, 1, len(r.values))
	require.IsType(t, &decimalColumnScanner{}, r.scanArgs[0])

	s := r.scanArgs[0].(sql.Scanner)
	err := s.Scan(16.1)
	require.NoError(t, err)
	require.Equal(t, "16.1", r.values[0].(decimal.Decimal).String())
	err = s.Scan(float32(20.5))
	require.NoError(t, err)
	require.Equal(t, "20.5", r.values[0].(decimal.Decimal).String())
	err = s.Scan(int64(20))
	require.NoError(t, err)
	require.Equal(t, "20", r.values[0].(decimal.Decimal).String())
	err = s.Scan(`30.5`)
	require.NoError(t, err)
	require.Equal(t, "30.5", r.values[0].(decimal.Decimal).String())
	err = s.Scan(`"40.5"`)
	require.NoError(t, err)
	require.Equal(t, "40.5", r.values[0].(decimal.Decimal).String())
	err = s.Scan([]byte(`50.5`))
	require.NoError(t, err)
	require.Equal(t, "50.5", r.values[0].(decimal.Decimal).String())
	err = s.Scan([]byte(`"60.5"`))
	require.NoError(t, err)
	require.Equal(t, "60.5", r.values[0].(decimal.Decimal).String())
	err = s.Scan(nil)
	require.NoError(t, err)
	require.Nil(t, r.values[0])
}

func TestColumnsInfo_Reader_String(t *testing.T) {
	ci := &columnsInfo{
		count:     1,
		names:     []string{"a"},
		dbTypes:   []string{""},
		scanTypes: []reflect.Type{reflect.TypeOf(sql.NullString{})},
	}
	r := ci.reader()
	require.NotNil(t, r)
	require.Equal(t, 1, r.count)
	require.Equal(t, 1, len(r.names))
	require.Equal(t, 1, len(r.scanArgs))
	require.Equal(t, 1, len(r.values))
	require.IsType(t, &stringColumnScanner{}, r.scanArgs[0])

	s := r.scanArgs[0].(sql.Scanner)
	err := s.Scan("foo")
	require.NoError(t, err)
	require.Equal(t, "foo", r.values[0])
	err = s.Scan([]byte("bar"))
	require.NoError(t, err)
	require.Equal(t, "bar", r.values[0])
}

func TestColumnsInfo_Reader_Float(t *testing.T) {
	ci := &columnsInfo{
		count:     1,
		names:     []string{"a"},
		dbTypes:   []string{""},
		scanTypes: []reflect.Type{reflect.TypeOf(1.0)},
	}
	r := ci.reader()
	require.NotNil(t, r)
	require.Equal(t, 1, r.count)
	require.Equal(t, 1, len(r.names))
	require.Equal(t, 1, len(r.scanArgs))
	require.Equal(t, 1, len(r.values))
	require.IsType(t, &decimalColumnScanner{}, r.scanArgs[0])
}

func TestColumnsInfo_Reader_Raw(t *testing.T) {
	ci := &columnsInfo{
		count:     1,
		names:     []string{"a"},
		dbTypes:   []string{""},
		scanTypes: []reflect.Type{reflect.TypeOf(1)},
	}
	r := ci.reader()
	require.NotNil(t, r)
	require.Equal(t, 1, r.count)
	require.Equal(t, 1, len(r.names))
	require.Equal(t, 1, len(r.scanArgs))
	require.Equal(t, 1, len(r.values))
	require.IsType(t, &rawColumnScanner{}, r.scanArgs[0])

	s := r.scanArgs[0].(sql.Scanner)
	err := s.Scan(16)
	require.NoError(t, err)
	require.Equal(t, 16, r.values[0])
}
