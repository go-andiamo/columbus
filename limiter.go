package columbus

// Limiter is an interface that can be passed as an option to Mapper.Rows or Mapper.WriteRows
//
// and is used to limit the number of rows read/written
type Limiter interface {
	// LimitReached should return true if the rowCount arg exceeds the maximum
	LimitReached(rowCount int) bool
}

type nullLimiter struct{}

var _ Limiter = (*nullLimiter)(nil)

func (n *nullLimiter) LimitReached(rowCount int) bool {
	return false
}
