package columbus

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAllowedProperties_Exclude(t *testing.T) {
	ap := AllowedProperties{
		"foo": nil,
		"bar": func(property string, path []string) bool {
			return true
		},
	}
	ex := ap.Exclude("foo", nil)
	require.False(t, ex)
	ex = ap.Exclude("bar", nil)
	require.True(t, ex)
	ex = ap.Exclude("baz", nil)
	require.True(t, ex)
}
