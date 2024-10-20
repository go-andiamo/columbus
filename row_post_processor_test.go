package columbus

import "context"

type dummyRowPostProcessor struct {
	err error
}

var _ RowPostProcessor = &dummyRowPostProcessor{}

func (d *dummyRowPostProcessor) PostProcess(ctx context.Context, sqli SqlInterface, row map[string]any) error {
	return d.err
}

func (d *dummyRowPostProcessor) ProvidesProperty() string {
	return ""
}
