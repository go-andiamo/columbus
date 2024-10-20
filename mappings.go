package columbus

import "context"

type PostProcess func(ctx context.Context, sqli SqlInterface, row map[string]any, value any) (replaceValue any, err error)

type Mapping struct {
	PostProcess PostProcess
	SubQuery    *SubQuery
}

type Mappings map[string]Mapping
