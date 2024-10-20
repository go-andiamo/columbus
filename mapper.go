package columbus

import (
	"fmt"
	"strings"
	"sync"
)

type Mapper interface {
	//TODO what basic methods does Mapper expose?
}

func NewMapper[T string | []string](cols T, mappings Mappings, options ...any) (Mapper, error) {
	return newMapper(cols, mappings, options...)
}

func newMapper(cols any, mappings Mappings, options ...any) (*mapper, error) {
	result := &mapper{
		mappings: mappings,
	}
	switch ct := cols.(type) {
	case string:
		result.cols = ct
	case []string:
		result.cols = strings.Join(ct, ",")
	}
	if err := result.addOptions(options...); err != nil {
		return nil, err
	}
	return result, nil
}

type mapper struct {
	mutex             sync.RWMutex
	cols              string
	mappings          Mappings
	rowPostProcessors []RowPostProcessor
	rowSubQueries     []SubQuery
}

func (m *mapper) addOptions(options ...any) error {
	for _, o := range options {
		if o != nil {
			switch option := o.(type) {
			case RowPostProcessor:
				m.rowPostProcessors = append(m.rowPostProcessors, option)
			case SubQuery:
				m.rowSubQueries = append(m.rowSubQueries, option)
			case *SubQuery:
				m.rowSubQueries = append(m.rowSubQueries, *option)
			default:
				return fmt.Errorf("unknown option type: %T", o)
			}
		}
	}
	return nil
}
