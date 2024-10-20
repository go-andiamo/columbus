package columbus

import (
	"context"
)

type RowPostProcessor interface {
	PostProcess(ctx context.Context, sqli SqlInterface, row map[string]any) error
}
