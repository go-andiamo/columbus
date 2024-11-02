package columbus

import (
	"database/sql"
	"encoding/json"
	"github.com/shopspring/decimal"
	"reflect"
	"strings"
)

type columnsInfo struct {
	count     int
	names     []string
	scanTypes []reflect.Type
	dbTypes   []string
	mappings  Mappings
}

type columnsReader struct {
	count    int
	names    []string
	values   []any
	scanArgs []any
}

func newColumnsInfo(rows *sql.Rows, mappings Mappings) (result *columnsInfo, err error) {
	var cts []*sql.ColumnType
	if cts, err = rows.ColumnTypes(); err == nil {
		count := len(cts)
		result = &columnsInfo{
			count:     count,
			names:     make([]string, count),
			scanTypes: make([]reflect.Type, count),
			dbTypes:   make([]string, count),
			mappings:  mappings,
		}
		for i, ct := range cts {
			result.names[i] = ct.Name()
			result.scanTypes[i] = ct.ScanType()
			result.dbTypes[i] = ct.DatabaseTypeName()
		}
	}
	return result, err
}

func (ci *columnsInfo) reader() *columnsReader {
	r := &columnsReader{
		count:    ci.count,
		values:   make([]any, ci.count),
		scanArgs: make([]any, ci.count),
		names:    ci.names,
	}
	for i := 0; i < ci.count; i++ {
		r.scanArgs[i] = ci.buildScanner(r, i)
	}
	return r
}

// ColumnScanner is a func that can be used by Mapping to read the value of a column
type ColumnScanner func(src any) (value any, err error)

func (ci *columnsInfo) buildScanner(cr *columnsReader, index int) sql.Scanner {
	if m, ok := ci.mappings[ci.names[index]]; ok && m.Scanner != nil {
		return &customColumnScanner{
			columns: cr,
			index:   index,
			scanner: m.Scanner,
		}
	}
	switch ci.dbTypes[index] {
	case "JSON", "JSONB":
		return &jsonColumnScanner{
			columns: cr,
			index:   index,
		}
	case "DECIMAL", "FLOAT", "DOUBLE":
		return &decimalColumnScanner{
			columns: cr,
			index:   index,
		}
	}
	v := reflect.New(ci.scanTypes[index]).Interface()
	switch v.(type) {
	case *string, string, *sql.NullString:
		return &stringColumnScanner{
			columns: cr,
			index:   index,
		}
	case *float32, *float64, float32, float64, *sql.NullFloat64:
		return &decimalColumnScanner{
			columns: cr,
			index:   index,
		}
	}
	return &rawColumnScanner{
		columns: cr,
		index:   index,
	}
}

type customColumnScanner struct {
	columns *columnsReader
	index   int
	scanner ColumnScanner
}

func (c *customColumnScanner) Scan(src any) error {
	v, err := c.scanner(src)
	if err == nil {
		c.columns.values[c.index] = v
	}
	return err
}

type rawColumnScanner struct {
	columns *columnsReader
	index   int
}

func (c *rawColumnScanner) Scan(src any) error {
	c.columns.values[c.index] = src
	return nil
}

type stringColumnScanner struct {
	columns *columnsReader
	index   int
}

func (c *stringColumnScanner) Scan(src any) error {
	switch v := src.(type) {
	case []byte:
		c.columns.values[c.index] = string(v)
	default:
		c.columns.values[c.index] = v
	}
	return nil
}

type decimalColumnScanner struct {
	columns *columnsReader
	index   int
}

func (c *decimalColumnScanner) Scan(src any) error {
	var err error
	switch v := src.(type) {
	case float32:
		c.columns.values[c.index] = decimal.NewFromFloat(float64(v))
	case float64:
		c.columns.values[c.index] = decimal.NewFromFloat(v)
	case int64:
		c.columns.values[c.index] = decimal.New(v, 0)
	case []byte:
		if len(v) > 2 && v[0] == '"' && v[len(v)-1] == '"' {
			c.columns.values[c.index], err = decimal.NewFromString(string(v[1 : len(v)-1]))
		} else {
			c.columns.values[c.index], err = decimal.NewFromString(string(v))
		}
	case string:
		if len(v) > 2 && strings.HasPrefix(v, `"`) && strings.HasSuffix(v, `"`) {
			c.columns.values[c.index], err = decimal.NewFromString(v[1 : len(v)-1])
		} else {
			c.columns.values[c.index], err = decimal.NewFromString(v)
		}
	default:
		c.columns.values[c.index] = src
	}
	return err
}

type jsonColumnScanner struct {
	columns *columnsReader
	index   int
}

func (c *jsonColumnScanner) Scan(src any) error {
	var err error
	switch data := src.(type) {
	case []byte:
		var v any
		if err = json.Unmarshal(data, &v); err == nil {
			c.columns.values[c.index] = v
		}
	case string:
		var v any
		if err = json.Unmarshal([]byte(data), &v); err == nil {
			c.columns.values[c.index] = v
		}
	default:
		c.columns.values[c.index] = src
	}
	return err
}
