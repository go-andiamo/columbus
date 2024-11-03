package columbus

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

type Mapper interface {
	Rows(ctx context.Context, sqli SqlInterface, args []any, options ...any) ([]map[string]any, error)
	FirstRow(ctx context.Context, sqli SqlInterface, args []any, options ...any) (map[string]any, error)
	ExactlyOneRow(ctx context.Context, sqli SqlInterface, args []any, options ...any) (map[string]any, error)
	WriteRows(ctx context.Context, writer io.Writer, sqli SqlInterface, args []any, options ...any) error
	WriteFirstRow(ctx context.Context, writer io.Writer, sqli SqlInterface, args []any, options ...any) error
	WriteExactlyOneRow(ctx context.Context, writer io.Writer, sqli SqlInterface, args []any, options ...any) error
	Iterate(ctx context.Context, sqli SqlInterface, args []any, handler func(row map[string]any) (cont bool, err error), options ...any) error
}

// UseDecimals is an option that determines whether float/numeric/decimal columns should be mapped as decimal.Decimal properties
//
// by default, Mapper will convert float/numeric/decimal columns to decimal.Decimal
type UseDecimals bool

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
		mappings:    mappings,
		useDecimals: true,
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
	useDecimals       bool
	// subQuery is set by parent sub-query
	subQuery SubQuery
	subPath  []string
}

var _ Mapper = (*mapper)(nil)

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
	if colsReader, err = m.mapColumns(rows, mappings); err == nil {
		result = make([]map[string]any, 0)
		var row map[string]any
		for rows.Next() {
			if row, err = m.mapRow(ctx, sqli, rows, colsReader, mappings, postProcesses, subQueries, exclusions); err == nil {
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
		if colsReader, err = m.mapColumns(rows, mappings); err == nil {
			result, err = m.mapRow(ctx, sqli, rows, colsReader, mappings, postProcesses, subQueries, exclusions)
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
		if colsReader, err = m.mapColumns(rows, mappings); err == nil {
			result, err = m.mapRow(ctx, sqli, rows, colsReader, mappings, postProcesses, subQueries, exclusions)
		}
	}
	return result, err
}

func (m *mapper) WriteRows(ctx context.Context, writer io.Writer, sqli SqlInterface, args []any, options ...any) (err error) {
	query, mappings, postProcesses, subQueries, exclusions, err := m.rowMapOptions(options...)
	if err != nil {
		return err
	}
	rows, err := sqli.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	var colsReader *columnsReader
	if colsReader, err = m.mapColumns(rows, mappings); err == nil {
		var row map[string]any
		if _, err = writer.Write([]byte("[")); err == nil {
			jw := json.NewEncoder(writer)
			first := true
			for rows.Next() && err == nil {
				if row, err = m.mapRow(ctx, sqli, rows, colsReader, mappings, postProcesses, subQueries, exclusions); err == nil {
					if !first {
						_, err = writer.Write([]byte(","))
					}
					if err == nil {
						err = jw.Encode(row)
						first = false
					}
				}
			}
		}
		_, err = writer.Write([]byte("]"))
	}
	return err
}

func (m *mapper) WriteFirstRow(ctx context.Context, writer io.Writer, sqli SqlInterface, args []any, options ...any) (err error) {
	query, mappings, postProcesses, subQueries, exclusions, err := m.rowMapOptions(options...)
	if err != nil {
		return err
	}
	rows, err := sqli.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	if rows.Next() {
		var colsReader *columnsReader
		if colsReader, err = m.mapColumns(rows, mappings); err == nil {
			var row map[string]any
			if row, err = m.mapRow(ctx, sqli, rows, colsReader, mappings, postProcesses, subQueries, exclusions); err == nil {
				err = json.NewEncoder(writer).Encode(row)
			}
		}
	}
	return err
}

func (m *mapper) WriteExactlyOneRow(ctx context.Context, writer io.Writer, sqli SqlInterface, args []any, options ...any) (err error) {
	query, mappings, postProcesses, subQueries, exclusions, err := m.rowMapOptions(options...)
	if err != nil {
		return err
	}
	rows, err := sqli.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	err = sql.ErrNoRows
	if rows.Next() {
		var colsReader *columnsReader
		if colsReader, err = m.mapColumns(rows, mappings); err == nil {
			var row map[string]any
			if row, err = m.mapRow(ctx, sqli, rows, colsReader, mappings, postProcesses, subQueries, exclusions); err == nil {
				err = json.NewEncoder(writer).Encode(row)
			}
		}
	}
	return err
}

func (m *mapper) Iterate(ctx context.Context, sqli SqlInterface, args []any, handler func(row map[string]any) (cont bool, err error), options ...any) (err error) {
	query, mappings, postProcesses, subQueries, exclusions, err := m.rowMapOptions(options...)
	if err != nil {
		return err
	}
	rows, err := sqli.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	var colsReader *columnsReader
	if colsReader, err = m.mapColumns(rows, mappings); err == nil {
		var row map[string]any
		cont := true
		for rows.Next() && cont && err == nil {
			if row, err = m.mapRow(ctx, sqli, rows, colsReader, mappings, postProcesses, subQueries, exclusions); err == nil {
				cont, err = handler(row)
			}
		}
	}
	return err
}

func (m *mapper) rowMapOptions(options ...any) (query string, mappings Mappings, postProcesses []RowPostProcessor, subQueries []SubQuery, exclusions PropertyExclusions, err error) {
	mappings = m.mappings
	mappingsCopied := false
	exclusions = make([]PropertyExcluder, 0)
	querySet := false
	subQueries = append(subQueries, m.rowSubQueries...)
	postProcesses = append(postProcesses, m.rowPostProcessors...)
	if m.defaultQuery != nil {
		querySet = true
		query = string(*m.defaultQuery)
	} else if m.subQuery != nil {
		querySet = true
		query = m.subQuery.getQuery()
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
				exclusions = append(exclusions, option...)
			case PropertyExcluder:
				exclusions = append(exclusions, option)
			case RowPostProcessor:
				postProcesses = append(postProcesses, option)
			case SubQuery:
				subQueries = append(subQueries, option)
			default:
				if excf, ok := o.(func(string, []string) bool); ok {
					exclusions = append(exclusions, ConditionalExclude(excf))
				} else {
					return "", nil, nil, nil, nil, fmt.Errorf("unknown option type: %T", o)
				}
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
			case UseDecimals:
				m.useDecimals = bool(option)
			default:
				return fmt.Errorf("unknown option type: %T", o)
			}
		}
	}
	return nil
}

func (m *mapper) mapColumns(rows *sql.Rows, mappings Mappings) (cr *columnsReader, err error) {
	m.mutex.RLock()
	if m.columnsInfo != nil {
		m.mutex.RUnlock()
		return m.columnsInfo.reader(), nil
	}
	m.mutex.RUnlock()
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.columnsInfo, err = newColumnsInfo(rows, m.useDecimals, mappings)
	return m.columnsInfo.reader(), err
}

func (m *mapper) mapRow(ctx context.Context, sqli SqlInterface, rows *sql.Rows, cols *columnsReader, mappings Mappings, postProcesses []RowPostProcessor, subQueries []SubQuery, exclusions PropertyExclusions) (row map[string]any, err error) {
	if err = rows.Scan(cols.scanArgs...); err == nil {
		row = make(map[string]any, cols.count)
		for i, name := range cols.names {
			value := cols.values[i]
			useObject := row
			var mapping *Mapping
			excluded := false
			if mp, ok := mappings[name]; ok {
				mapping = &mp
				if value == nil {
					if mapping.OmitNull {
						continue
					} else if mapping.NullDefault != nil {
						value = mapping.NullDefault
					}
				}
				if mapping.PropertyName != "" {
					name = mapping.PropertyName
				}
				if excluded = exclusions.Exclude(name, append(m.subPath, mapping.Path...)); !excluded {
					for _, path := range mapping.Path {
						found := false
						if existing, ok := useObject[path]; ok {
							if obj, ok := existing.(map[string]any); ok {
								found = true
								useObject = obj
							}
						}
						if !found {
							obj := map[string]any{}
							useObject[path] = obj
							useObject = obj

						}
					}
				}
			} else {
				excluded = exclusions.Exclude(name, m.subPath)
			}
			if !excluded {
				useObject[name] = value
				if mapping != nil {
					if mapping.PostProcess != nil {
						if replace, replaceValue, err := mapping.PostProcess(ctx, sqli, row, value); err != nil {
							return nil, err
						} else if replace {
							useObject[name] = replaceValue
						}
					}
				}
			}
		}
		for _, sq := range subQueries {
			if sq != nil && (sq.ProvidesProperty() == "" || !exclusions.Exclude(sq.ProvidesProperty(), nil)) {
				if err = sq.Execute(ctx, sqli, row, exclusions); err != nil {
					return nil, err
				}
			}
		}
		for _, rp := range postProcesses {
			if rp != nil && (rp.ProvidesProperty() == "" || !exclusions.Exclude(rp.ProvidesProperty(), nil)) {
				if err = rp.PostProcess(ctx, sqli, row); err != nil {
					return nil, err
				}
			}
		}
	}
	return row, err
}
