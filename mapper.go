package columbus

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
)

type Mapper interface {
	Rows(ctx context.Context, sqli SqlInterface, args []any, options ...any) ([]map[string]any, error)
	FirstRow(ctx context.Context, sqli SqlInterface, args []any, options ...any) (map[string]any, error)
	ExactlyOneRow(ctx context.Context, sqli SqlInterface, args []any, options ...any) (map[string]any, error)
}

// NewMapper creates a new row mapper
func NewMapper[T string | []string](cols T, mappings Mappings, options ...any) (Mapper, error) {
	return newMapper(cols, mappings, options...)
}

// MustNewMapper is the same as NewMapper, except it panics on error
func MustNewMapper[T string | []string](cols T, mappings Mappings, options ...any) Mapper {
	m, err := NewMapper[T](cols, mappings, options...)
	if err != nil {
		panic(err)
	}
	return m
}

func newMapper(cols any, mappings Mappings, options ...any) (*mapper, error) {
	result := &mapper{
		mappings: mappings,
	}
	switch ct := cols.(type) {
	case string:
		result.cols = ct
	case []string:
		result.cols = strings.Join(ct, ",")
	}
	if err := result.addOptions(options...); err != nil {
		return nil, err
	}
	return result, nil
}

type mapper struct {
	mutex             sync.RWMutex
	cols              string
	columnsInfo       *columnsInfo
	mappings          Mappings
	rowPostProcessors []RowPostProcessor
	rowSubQueries     []SubQuery
	defaultQuery      *Query
	// subQuery is set by parent sub-query
	subQuery SubQuery
}

func (m *mapper) Rows(ctx context.Context, sqli SqlInterface, args []any, options ...any) (result []map[string]any, err error) {
	query, mappings, postProcesses, subQueries, exclusions, err := m.rowMapOptions(options...)
	if err != nil {
		return nil, err
	}
	rows, err := sqli.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var colsReader *columnsReader
	if colsReader, err = m.mapColumns(rows); err == nil {
		result = make([]map[string]any, 0)
		var row map[string]any
		for rows.Next() {
			if row, err = m.mapRow(rows, colsReader, mappings, postProcesses, subQueries, exclusions); err == nil {
				result = append(result, row)
			} else {
				return nil, err
			}
		}
	}
	return result, err
}

func (m *mapper) FirstRow(ctx context.Context, sqli SqlInterface, args []any, options ...any) (result map[string]any, err error) {
	query, mappings, postProcesses, subQueries, exclusions, err := m.rowMapOptions(options...)
	if err != nil {
		return nil, err
	}
	rows, err := sqli.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	if rows.Next() {
		var colsReader *columnsReader
		if colsReader, err = m.mapColumns(rows); err == nil {
			result, err = m.mapRow(rows, colsReader, mappings, postProcesses, subQueries, exclusions)
		}
	}
	return result, err
}

func (m *mapper) ExactlyOneRow(ctx context.Context, sqli SqlInterface, args []any, options ...any) (result map[string]any, err error) {
	query, mappings, postProcesses, subQueries, exclusions, err := m.rowMapOptions(options...)
	if err != nil {
		return nil, err
	}
	rows, err := sqli.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	err = sql.ErrNoRows
	if rows.Next() {
		var colsReader *columnsReader
		if colsReader, err = m.mapColumns(rows); err == nil {
			result, err = m.mapRow(rows, colsReader, mappings, postProcesses, subQueries, exclusions)
		}
	}
	return result, err
}

func (m *mapper) rowMapOptions(options ...any) (query string, mappings Mappings, postProcesses []RowPostProcessor, subQueries []SubQuery, exclusions []PropertyExclusions, err error) {
	mappings = m.mappings
	mappingsCopied := false
	exclusions = make([]PropertyExclusions, 0)
	querySet := false
	if m.defaultQuery != nil {
		querySet = true
		query = string(*m.defaultQuery)
	} else if m.subQuery != nil {
		querySet = true
		query = m.subQuery.Query()
	}
	for _, o := range options {
		if o != nil {
			switch option := o.(type) {
			case Query:
				querySet = true
				query = "SELECT " + m.cols + " " + string(option)
			case AddClause:
				if !querySet {
					err = errors.New("add clause must have a query set")
					return
				}
				query += " " + string(option)
			case Mappings:
				if !mappingsCopied {
					mappingsCopied = true
					mappings = m.copyMappings()
				}
				for k, v := range option {
					mappings[k] = v
				}
			case PropertyExclusions:
				exclusions = append(exclusions, option)
			case RowPostProcessor:
				postProcesses = append(postProcesses, option)
			case SubQuery:
				subQueries = append(subQueries, option)
			default:
				return "", nil, nil, nil, nil, fmt.Errorf("unknown option type: %T", o)
			}
		}
	}
	if !querySet {
		err = errors.New("no default query")
	}
	return query, mappings, postProcesses, subQueries, exclusions, err
}

func (m *mapper) copyMappings() Mappings {
	result := make(Mappings, len(m.mappings))
	for k, v := range m.mappings {
		result[k] = v
	}
	return result
}

func (m *mapper) addOptions(options ...any) error {
	for _, o := range options {
		if o != nil {
			switch option := o.(type) {
			case RowPostProcessor:
				m.rowPostProcessors = append(m.rowPostProcessors, option)
			case SubQuery:
				m.rowSubQueries = append(m.rowSubQueries, option)
			case Query:
				if m.defaultQuery != nil {
					return errors.New("cannot use multiple default queries")
				}
				qStr := Query("SELECT " + m.cols + " " + string(option))
				m.defaultQuery = &qStr
			default:
				return fmt.Errorf("unknown option type: %T", o)
			}
		}
	}
	return nil
}

func (m *mapper) mapColumns(rows *sql.Rows) (cr *columnsReader, err error) {
	m.mutex.RLock()
	if m.columnsInfo != nil {
		m.mutex.RUnlock()
		return m.columnsInfo.reader(), nil
	}
	m.mutex.RUnlock()
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.columnsInfo, err = newColumnsInfo(rows)
	return m.columnsInfo.reader(), err
}

func (m *mapper) mapRow(rows *sql.Rows, cols *columnsReader, mappings Mappings, postProcesses []RowPostProcessor, subQueries []SubQuery, exclusions []PropertyExclusions) (row map[string]any, err error) {
	if err = rows.Scan(cols.scanArgs...); err == nil {
		row = make(map[string]any, cols.count)
		for i, n := range cols.names {
			row[n] = cols.values[i]
		}
	}
	return row, err
}
