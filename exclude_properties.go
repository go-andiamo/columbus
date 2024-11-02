package columbus

type PropertyExclusion interface {
	// Exclude should return true if the property is to be excluded
	Exclude(property string, path []string) bool
}

type PropertyExclusions []PropertyExclusion

type ConditionalExclude func(property string, path []string) bool

type AllowedProperties map[string]ConditionalExclude

var _ PropertyExclusion = AllowedProperties{}

func (xp AllowedProperties) Exclude(property string, path []string) bool {
	if cx, ok := xp[property]; ok {
		if cx != nil {
			return cx(property, path)
		}
		return false
	}
	return true
}
