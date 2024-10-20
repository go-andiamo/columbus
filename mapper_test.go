package columbus

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewMapper(t *testing.T) {
	m, err := NewMapper("a,b,c", nil)
	require.NoError(t, err)
	require.NotNil(t, m)
	mt := m.(*mapper)
	require.Equal(t, "a,b,c", mt.cols)

	m, err = NewMapper([]string{"a", "b", "c"}, nil)
	require.NoError(t, err)
	require.NotNil(t, m)
	mt = m.(*mapper)
	require.Equal(t, "a,b,c", mt.cols)
}

func TestNewMapper_WithOptions(t *testing.T) {
	_, err := NewMapper("a,b,c", nil, nil)
	require.NoError(t, err)

	drpp := &dummyRowPostProcessor{}
	m, err := NewMapper("a,b,c", nil, drpp)
	require.NoError(t, err)
	mt := m.(*mapper)
	require.Equal(t, 1, len(mt.rowPostProcessors))

	sq := SubQuery{}
	m, err = NewMapper("a,b,c", nil, sq)
	require.NoError(t, err)
	mt = m.(*mapper)
	require.Equal(t, 1, len(mt.rowSubQueries))

	m, err = NewMapper("a,b,c", nil, &sq)
	require.NoError(t, err)
	mt = m.(*mapper)
	require.Equal(t, 1, len(mt.rowSubQueries))

	_, err = NewMapper("a,b,c", nil, "not a valid option")
	require.Error(t, err)
	require.Equal(t, "unknown option type: string", err.Error())
}
