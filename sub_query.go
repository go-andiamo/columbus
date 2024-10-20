package columbus

import (
	"context"
	"fmt"
)

type SubQuery struct {
	// PropertyName is the property name to use in the row object
	PropertyName string
	// ArgColumns is the columns to use as args for the sub-query
	ArgColumns []string
	// Query is the SQL query to use - it should contain the same number of '?' arg markers as the length of ArgColumns
	Query string
	// AsObject indicates the result of the query is placed into the row object as an object
	// (as opposed to a slice/json array)
	AsObject bool
	// ErrNotFound indicates that, when retrieving as an object, should error when sub-query finds no rows
	ErrNoRow bool
	// EmptyNil indicates if the result is empty then the resultant property is nil
	EmptyNil bool
	// Mappings is any column mappings used by the sub-query
	Mappings Mappings
}

func (sq *SubQuery) execute(ctx context.Context, sqli SqlInterface, row map[string]any) error {
	rm, _ := newMapper(nil, sq.Mappings)
	rm.subQuery = sq
	args, err := sq.getArgs(row)
	if err != nil {
		return err
	}
	if sq.AsObject && sq.ErrNoRow {
		if obj, err := rm.ExactlyOneRow(ctx, sqli, args); err != nil {
			return err
		} else {
			row[sq.PropertyName] = obj
		}
	} else if sq.AsObject {
		if obj, err := rm.FirstRow(ctx, sqli, args); err != nil {
			return err
		} else if sq.EmptyNil && (obj == nil || len(obj) == 0) {
			row[sq.PropertyName] = nil
		} else {
			row[sq.PropertyName] = obj
		}
	} else {
		if rows, err := rm.Rows(ctx, sqli, args); err != nil {
			return err
		} else if sq.EmptyNil && (rows == nil || len(rows) == 0) {
			row[sq.PropertyName] = nil
		} else {
			row[sq.PropertyName] = rows
		}
	}
	return nil
}

func (sq *SubQuery) getArgs(row map[string]any) ([]any, error) {
	result := make([]any, 0, len(sq.ArgColumns))
	for _, arg := range sq.ArgColumns {
		if v, ok := row[arg]; ok {
			result = append(result, v)
		} else {
			return nil, fmt.Errorf("sub-query arg property '%s' does not exist", arg)
		}
	}
	return result, nil
}
