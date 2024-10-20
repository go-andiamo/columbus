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
	mappings          Mappings
	rowPostProcessors []RowPostProcessor
	rowSubQueries     []SubQuery
	defaultQuery      *Query
	subQuery          *SubQuery
}

func (m *mapper) Rows(ctx context.Context, sqli SqlInterface, args []any, options ...any) ([]map[string]any, error) {
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
	result := make([]map[string]any, 0)
	for rows.Next() {
		if r, err := m.mapRow(rows, mappings, postProcesses, subQueries, exclusions); err == nil {
			result = append(result, r)
		} else {
			return nil, err
		}
	}
	return result, nil
}

func (m *mapper) FirstRow(ctx context.Context, sqli SqlInterface, args []any, options ...any) (map[string]any, error) {
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
		if r, err := m.mapRow(rows, mappings, postProcesses, subQueries, exclusions); err == nil {
			return r, nil
		} else {
			return nil, err
		}
	}
	return nil, nil
}

func (m *mapper) ExactlyOneRow(ctx context.Context, sqli SqlInterface, args []any, options ...any) (map[string]any, error) {
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
		if r, err := m.mapRow(rows, mappings, postProcesses, subQueries, exclusions); err == nil {
			return r, nil
		} else {
			return nil, err
		}
	}
	return nil, sql.ErrNoRows
}

func (m *mapper) rowMapOptions(options ...any) (query string, mappings Mappings, postProcesses []RowPostProcessor, subQueries []SubQuery, exclusions ExcludeProperties, err error) {
	mappings = Mappings{}
	exclusions = ExcludeProperties{}
	querySet := false
	if m.defaultQuery != nil {
		querySet = true
		query = string(*m.defaultQuery)
	} else if m.subQuery != nil {
		querySet = true
		query = m.subQuery.Query
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
				for k, v := range option {
					mappings[k] = v
				}
			case ExcludeProperties:
				for k, v := range option {
					exclusions[k] = v
				}
			case RowPostProcessor:
				postProcesses = append(postProcesses, option)
			case SubQuery:
				subQueries = append(subQueries, option)
			case *SubQuery:
				subQueries = append(subQueries, *option)
			}
		}
	}
	if !querySet {
		err = errors.New("no default query")
	}
	return query, mappings, postProcesses, subQueries, exclusions, err
}

func (m *mapper) mapRow(rows *sql.Rows, addMappings Mappings, addPostProcesses []RowPostProcessor, addSubQueries []SubQuery, exclusions ExcludeProperties) (map[string]any, error) {
	//TODO implement me
	return map[string]any{}, nil
}

func (m *mapper) addOptions(options ...any) error {
	for _, o := range options {
		if o != nil {
			switch option := o.(type) {
			case RowPostProcessor:
				m.rowPostProcessors = append(m.rowPostProcessors, option)
			case SubQuery:
				m.rowSubQueries = append(m.rowSubQueries, option)
			case *SubQuery:
				m.rowSubQueries = append(m.rowSubQueries, *option)
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
