package columbus

import (
	"context"
)

// RowPostProcessor is an interface that can be passed as an option to Mapper
//
// Any RowPostProcessor(s) passed to Mapper are executed after the row is mapped
type RowPostProcessor interface {
	// PostProcess executes the RowPostProcessor
	PostProcess(ctx context.Context, sqli SqlInterface, row map[string]any) error
	// ProvidesProperty indicates that the RowPostProcessor provides a specific named property
	// which is used to check if the property should be excluded
	//
	// if returns a property name and that property is excluded - the RowPostProcessor is not executed
	//
	// if returns an empty string - the RowPostProcessor is always executed
	ProvidesProperty() string
}

type RowPostProcessorFunc func(ctx context.Context, sqli SqlInterface, row map[string]any) error

var _ RowPostProcessor = (RowPostProcessorFunc)(nil)

func (f RowPostProcessorFunc) PostProcess(ctx context.Context, sqli SqlInterface, row map[string]any) error {
	return f(ctx, sqli, row)
}

func (f RowPostProcessorFunc) ProvidesProperty() string {
	return ""
}
