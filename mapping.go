package columbus

import "context"

type PostProcess func(ctx context.Context, sqli SqlInterface, row map[string]any, value any) (replaceValue any, err error)

type Mapping struct {
	// PropertyName is the property name to use (if not an empty string) - overrides the column name
	PropertyName string
	// Path is the path to use for the property
	// allows flat columns to be re-structured into row as object properties
	Path []string
	// OmitNull indicates that if the column is null then the property is not added to the row (this is not overridden by specifying a value for NullDefault)
	OmitNull bool
	// NullDefault is the value to use when the column is null
	NullDefault any
	// PostProcess is an optional post-process function to be run on the property
	PostProcess PostProcess
	// Scanner is an optional ColumnScanner function that reads the value from the database column
	Scanner ColumnScanner
}

// Mappings is a map of Mapping by column name
type Mappings map[string]Mapping
