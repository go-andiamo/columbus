package columbus

type PropertyExclusions interface {
	Exclude(property string, row map[string]any) bool
}

type ConditionalExclude func(property string, row map[string]any) bool

type ExcludeProperties map[string]ConditionalExclude

func (xp ExcludeProperties) Exclude(property string, row map[string]any) bool {
	if cx, ok := xp[property]; ok {
		if cx != nil {
			return cx(property, row)
		}
		return false
	}
	return true
}
