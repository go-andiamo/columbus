package columbus

type ConditionalExclude func(row map[string]any) bool

type ExcludeProperties map[string]ConditionalExclude

func (xp ExcludeProperties) exclude(row map[string]any, property string) bool {
	if cx, ok := xp[property]; ok {
		if cx != nil {
			return cx(row)
		}
		return true
	}
	return false
}
