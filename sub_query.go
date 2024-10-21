package columbus

import (
	"context"
	"fmt"
)

type SubQuery interface {
	Execute(ctx context.Context, sqli SqlInterface, row map[string]any) error
}

var _ SubQuery = &subQuery{}

func NewSubQuery(propertyName string, query string, argColumns []string, mappings Mappings, emptyNil bool) SubQuery {
	return &subQuery{
		propertyName: propertyName,
		query:        query,
		argColumns:   argColumns,
		mappings:     mappings,
		emptyNil:     emptyNil,
	}
}

func NewObjectSubQuery(propertyName string, query string, argColumns []string, mappings Mappings, emptyNil bool, errNoRow bool) SubQuery {
	return &subQuery{
		propertyName: propertyName,
		query:        query,
		argColumns:   argColumns,
		mappings:     mappings,
		asObject:     true,
		emptyNil:     emptyNil,
		errNoRow:     !emptyNil && errNoRow,
	}
}

type subQuery struct {
	// propertyName is the property name to use in the row object
	propertyName string
	// query is the SQL query to use - it should contain the same number of '?' arg markers as the length of argColumns
	query string
	// argColumns is the columns to use as args for the sub-query
	argColumns []string
	// asObject indicates the result of the query is placed into the row object as an object
	// (as opposed to a slice/json array)
	asObject bool
	// errNotFound indicates that, when retrieving as an object, should error when sub-query finds no rows
	errNoRow bool
	// emptyNil indicates if the result is empty then the resultant property is nil
	emptyNil bool
	// mappings is any column mappings used by the sub-query
	mappings Mappings
}

func (sq *subQuery) Execute(ctx context.Context, sqli SqlInterface, row map[string]any) error {
	rm, _ := newMapper(nil, sq.mappings)
	rm.subQuery = sq
	args, err := sq.getArgs(row)
	if err != nil {
		return err
	}
	if sq.asObject && sq.errNoRow {
		if obj, err := rm.ExactlyOneRow(ctx, sqli, args); err != nil {
			return err
		} else {
			row[sq.propertyName] = obj
		}
	} else if sq.asObject {
		if obj, err := rm.FirstRow(ctx, sqli, args); err != nil {
			return err
		} else if sq.emptyNil && (obj == nil || len(obj) == 0) {
			row[sq.propertyName] = nil
		} else {
			row[sq.propertyName] = obj
		}
	} else {
		if rows, err := rm.Rows(ctx, sqli, args); err != nil {
			return err
		} else if sq.emptyNil && (rows == nil || len(rows) == 0) {
			row[sq.propertyName] = nil
		} else {
			row[sq.propertyName] = rows
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
