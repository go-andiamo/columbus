package columbus

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/shopspring/decimal"
	"maps"
	"reflect"
	"strconv"
	"strings"
)

// ColumnScanner is a func that can be used by Mapping to read the value of a column
type ColumnScanner func(src any) (value any, err error)

// DbTypeScanners is a map of column type scanners that can be used as an option for NewMapper / MustNewMapper
//
// the key is the database native column type (e.g. "JSON", "JSONB", "UUID" etc.) and the value is a func that performs the scan
type DbTypeScanners map[string]ColumnScanner

// DefaultDbTypeScanners is the global default DbTypeScanners used by mappers
//
// Each new mapper uses these, but keys can be overridden by passing a DbTypeScanners as an option to NewMapper / MustNewMapper
var DefaultDbTypeScanners DbTypeScanners = nil

func defaultDbTypeScanners() (result DbTypeScanners) {
	if result = maps.Clone(DefaultDbTypeScanners); result == nil {
		result = make(DbTypeScanners)
	}
	return result
}

// BoolColumn is a ColumnScanner that can be used by Mapping.Scanner to convert a column to a boolean property
//
// Particularly useful for MySql which only supports BOOL columns as TINYINT
func BoolColumn(src any) (any, error) {
	switch v := src.(type) {
	case bool:
		return v, nil
	case int64:
		return v != 0, nil
	case float64:
		return v != 0, nil
	case []byte:
		return strconv.ParseBool(string(v))
	case string:
		return strconv.ParseBool(v)
	case nil:
		return false, nil
	}
	return nil, fmt.Errorf("type %T is not a bool", src)
}

type columnsInfo struct {
	count          int
	names          []string
	scanTypes      []reflect.Type
	dbTypes        []string
	mappings       Mappings
	useDecimals    bool
	dbTypeScanners DbTypeScanners
}

type columnsReader struct {
	count    int
	names    []string
	values   []any
	scanArgs []any
}

func newColumnsInfo(rows *sql.Rows, useDecimals bool, mappings Mappings, scanners DbTypeScanners) (result *columnsInfo, err error) {
	var cts []*sql.ColumnType
	if cts, err = rows.ColumnTypes(); err == nil {
		count := len(cts)
		result = &columnsInfo{
			count:          count,
			names:          make([]string, count),
			scanTypes:      make([]reflect.Type, count),
			dbTypes:        make([]string, count),
			mappings:       mappings,
			useDecimals:    useDecimals,
			dbTypeScanners: scanners,
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

func (ci *columnsInfo) buildScanner(cr *columnsReader, index int) sql.Scanner {
	if m, ok := ci.mappings[ci.names[index]]; ok && m.Scanner != nil {
		return &customColumnScanner{
			columns: cr,
			index:   index,
			scanner: m.Scanner,
		}
	} else if st, ok := ci.dbTypeScanners[ci.dbTypes[index]]; ok && st != nil {
		return &customColumnScanner{
			columns: cr,
			index:   index,
			scanner: st,
		}
	}
	switch ci.dbTypes[index] {
	case "JSON", "JSONB":
		return &jsonColumnScanner{
			columns: cr,
			index:   index,
		}
	case "DECIMAL", "FLOAT", "DOUBLE", "NUMERIC":
		if ci.useDecimals {
			return &decimalColumnScanner{
				columns: cr,
				index:   index,
			}
		}
	default:
		if ci.useDecimals && strings.HasPrefix(ci.dbTypes[index], "FLOAT") {
			return &decimalColumnScanner{
				columns: cr,
				index:   index,
			}
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
		if ci.useDecimals {
			return &decimalColumnScanner{
				columns: cr,
				index:   index,
			}
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
