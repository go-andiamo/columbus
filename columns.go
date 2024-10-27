package columbus

import (
	"database/sql"
	"reflect"
)

type columnsInfo struct {
	count     int
	names     []string
	scanTypes []reflect.Type
}

type columnsReader struct {
	count    int
	names    []string
	values   []any
	scanArgs []any
}

func newColumnsInfo(rows *sql.Rows) (result *columnsInfo, err error) {
	var cts []*sql.ColumnType
	if cts, err = rows.ColumnTypes(); err == nil {
		count := len(cts)
		result = &columnsInfo{
			count:     count,
			names:     make([]string, count),
			scanTypes: make([]reflect.Type, count),
		}
		for i, ct := range cts {
			result.names[i] = ct.Name()
			result.scanTypes[i] = ct.ScanType()
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
	for i, st := range ci.scanTypes {
		sv := reflect.New(st)
		v := sv.Elem().Interface()
		r.values[i] = v
		r.scanArgs[i] = &r.values[i]
	}
	return r
}
