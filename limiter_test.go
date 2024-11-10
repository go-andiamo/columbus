package columbus

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDefaultLimiter(t *testing.T) {
	require.False(t, defaultLimiter.LimitReached(1000000))
}

type testLimiter struct {
	limit int
}

func (n *testLimiter) LimitReached(rowCount int) bool {
	return rowCount > n.limit
}
