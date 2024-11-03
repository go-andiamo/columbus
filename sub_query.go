package columbus

import (
	"context"
	"fmt"
	"sync"
)

type SubQuery interface {
	Execute(ctx context.Context, sqli SqlInterface, row map[string]any, exclusions PropertyExclusions) error
	ProvidesProperty() string
	getQuery() string
}

// NewSubQuery creates a new sub-query that creates an array property in the mapped row
func NewSubQuery(propertyName string, query string, argColumns []string, mappings Mappings, emptyNil bool) SubQuery {
	return &sliceSubQuery{subQuery{
		propertyName: propertyName,
		query:        query,
		argColumns:   argColumns,
		mappings:     mappings,
		emptyNil:     emptyNil,
	}}
}

// NewObjectSubQuery creates a new sub-query that creates an object property in the mapped row
func NewObjectSubQuery(propertyName string, query string, argColumns []string, mappings Mappings, emptyNil bool, errNoRow bool) SubQuery {
	if errNoRow {
		return &exactObjectSubQuery{subQuery{
			propertyName: propertyName,
			query:        query,
			argColumns:   argColumns,
			mappings:     mappings,
		}}
	}
	return &objectSubQuery{subQuery{
		propertyName: propertyName,
		query:        query,
		argColumns:   argColumns,
		mappings:     mappings,
		emptyNil:     emptyNil,
	}}
}

// NewMergeSubQuery creates a new sub-query that reads an object for the mapped row and merges the properties from
// that object into the mapped row
func NewMergeSubQuery(query string, argColumns []string, mappings Mappings, noOverwrite bool) SubQuery {
	return &mergeSubQuery{
		noOverwrite: noOverwrite,
		subQuery: subQuery{
			query:      query,
			argColumns: argColumns,
			mappings:   mappings,
		}}
}

type subQuery struct {
	mutex  sync.RWMutex
	mapper *mapper
	// propertyName is the property name to use in the row object
	propertyName string
	// query is the SQL query to use - it should contain the same number of '?' arg markers as the length of argColumns
	query string
	// argColumns is the columns to use as args for the sub-query
	argColumns []string
	// emptyNil indicates if the result is empty then the resultant property is nil
	emptyNil bool
	// mappings is any column mappings used by the sub-query
	mappings Mappings
}

func (sq *subQuery) getQuery() string {
	return sq.query
}

func (sq *subQuery) ProvidesProperty() string {
	return sq.propertyName
}

type sliceSubQuery struct {
	subQuery
}

var _ SubQuery = &sliceSubQuery{}

func (sq *sliceSubQuery) Execute(ctx context.Context, sqli SqlInterface, row map[string]any, exclusions PropertyExclusions) error {
	rm := sq.rowMapper(sq)
	args, err := sq.getArgs(row)
	if err != nil {
		return err
	}
	if rows, err := rm.Rows(ctx, sqli, args, exclusions); err != nil {
		return err
	} else if sq.emptyNil && (rows == nil || len(rows) == 0) {
		row[sq.propertyName] = nil
	} else {
		row[sq.propertyName] = rows
	}
	return nil
}

type objectSubQuery struct {
	subQuery
}

var _ SubQuery = &objectSubQuery{}

func (sq *objectSubQuery) Execute(ctx context.Context, sqli SqlInterface, row map[string]any, exclusions PropertyExclusions) error {
	rm := sq.rowMapper(sq)
	args, err := sq.getArgs(row)
	if err != nil {
		return err
	}
	if obj, err := rm.FirstRow(ctx, sqli, args, exclusions); err != nil {
		return err
	} else if sq.emptyNil && (obj == nil || len(obj) == 0) {
		row[sq.propertyName] = nil
	} else {
		row[sq.propertyName] = obj
	}
	return nil
}

type exactObjectSubQuery struct {
	subQuery
}

var _ SubQuery = &exactObjectSubQuery{}

func (sq *exactObjectSubQuery) Execute(ctx context.Context, sqli SqlInterface, row map[string]any, exclusions PropertyExclusions) error {
	rm := sq.rowMapper(sq)
	args, err := sq.getArgs(row)
	if err != nil {
		return err
	}
	if obj, err := rm.ExactlyOneRow(ctx, sqli, args, exclusions); err != nil {
		return err
	} else {
		row[sq.propertyName] = obj
	}
	return nil
}

type mergeSubQuery struct {
	noOverwrite bool
	subQuery
}

var _ SubQuery = &mergeSubQuery{}

func (sq *mergeSubQuery) Execute(ctx context.Context, sqli SqlInterface, row map[string]any, exclusions PropertyExclusions) error {
	rm := sq.rowMapper(sq)
	args, err := sq.getArgs(row)
	if err != nil {
		return err
	}
	if obj, err := rm.FirstRow(ctx, sqli, args, exclusions); err != nil {
		return err
	} else if sq.noOverwrite {
		for k, v := range obj {
			if _, ok := row[k]; !ok {
				row[k] = v
			}
		}
	} else {
		for k, v := range obj {
			row[k] = v
		}
	}
	return nil
}

func (sq *subQuery) getArgs(row map[string]any) ([]any, error) {
	result := make([]any, 0, len(sq.argColumns))
	for _, arg := range sq.argColumns {
		if v, ok := row[arg]; ok {
			result = append(result, v)
		} else {
			return nil, fmt.Errorf("sub-query arg property '%s' does not exist", arg)
		}
	}
	return result, nil
}

func (sq *subQuery) rowMapper(asq SubQuery) *mapper {
	sq.mutex.RLock()
	if sq.mapper != nil {
		sq.mutex.RUnlock()
		return sq.mapper
	}
	sq.mutex.RUnlock()
	sq.mutex.Lock()
	defer sq.mutex.Unlock()
	sq.mapper, _ = newMapper(nil, sq.mappings)
	sq.mapper.subQuery = asq
	if sq.propertyName != "" {
		sq.mapper.subPath = []string{sq.propertyName}
	}
	return sq.mapper
}
