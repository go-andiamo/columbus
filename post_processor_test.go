package columbus

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRowPostProcessorFunc(t *testing.T) {
	fn := RowPostProcessorFunc(func(ctx context.Context, sqli SqlInterface, row map[string]any) error {
		row["foo"] = true
		return nil
	})
	require.Equal(t, "", fn.ProvidesProperty())
	row := map[string]any{}
	err := fn.PostProcess(context.Background(), nil, row)
	require.NoError(t, err)
	require.Equal(t, true, row["foo"])

	m, err := NewMapper("", fn)
	require.NoError(t, err)
	require.NotNil(t, m)
	mt := m.(*mapper)
	require.Equal(t, 1, len(mt.rowPostProcessors))
}

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
