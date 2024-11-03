package columbus

import (
	"context"
)

// RowPostProcessor is an interface that can be passed as an option to Mapper
//
// Any RowPostProcessor(s) passed to Mapper are executed after the row is mapped
type RowPostProcessor interface {
	PostProcess(ctx context.Context, sqli SqlInterface, row map[string]any) error
	ProvidesProperty() string
}
