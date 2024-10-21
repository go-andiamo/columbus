package columbus

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExcludeProperties_Exclude(t *testing.T) {
	exp := ExcludeProperties{
		"foo": nil,
		"bar": func(property string, row map[string]any) bool {
			return true
		},
	}
	row := map[string]any{}
	ex := exp.Exclude("foo", row)
	require.False(t, ex)
	ex = exp.Exclude("bar", row)
	require.True(t, ex)
	ex = exp.Exclude("baz", row)
	require.True(t, ex)
}
