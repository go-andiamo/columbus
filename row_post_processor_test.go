package columbus

import "context"

type dummyRowPostProcessor struct {
	err error
}

func (d *dummyRowPostProcessor) PostProcess(ctx context.Context, sqli SqlInterface, row map[string]any) error {
	return d.err
}
