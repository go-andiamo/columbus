package columbus

import "context"

type SubQuery struct {
	// PropertyName is the property name to use in the row object
	PropertyName string
	// ArgColumns is the columns to use as args for the sub-query
	ArgColumns []string
	// query is the SQL query to use - it should contain the same number of '?' arg markers as the length of ArgColumns
	Query string
	// asObject indicates the result of the query is placed into the row object as an object
	// (as opposed to a slice/json array)
	AsObject bool
	// emptyNull indicates if AsObject and object is empty then the resultant property is null
	EmptyNull bool
	// columnMappings is any column mappings used by the sub-query
	Mappings Mappings
}

func (sq *SubQuery) execute(ctx context.Context, sqli SqlInterface, row map[string]any) error {
	//TODO
	return nil
}
