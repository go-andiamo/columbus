package columbus

// PropertyExcluder is an option that can be passed to Mapper.Rows, Mapper.FirstRow and Mapper.ExactlyOneRow
// and is called during row mapping to determine whether a property is to be excluded from the final row
type PropertyExcluder interface {
	// Exclude should return true if the property is to be excluded
	Exclude(property string, path []string) bool
}

// PropertyExclusions is a slice of PropertyExcluder and can be passed as an option
// to Mapper.Rows, Mapper.FirstRow and Mapper.ExactlyOneRow
type PropertyExclusions []PropertyExcluder

func (p PropertyExclusions) Exclude(property string, path []string) bool {
	for _, exc := range p {
		if exc != nil {
			if exc.Exclude(property, path) {
				return true
			}
		}
	}
	return false
}

// AllowedProperties is a map implementation of PropertyExcluder
//
// If the property is in the map, it is not excluded
type AllowedProperties map[string]ConditionalExclude

type ConditionalExclude func(property string, path []string) bool

func (f ConditionalExclude) Exclude(property string, path []string) bool {
	return f(property, path)
}

var _ PropertyExcluder = (AllowedProperties)(nil)

func (xp AllowedProperties) Exclude(property string, path []string) bool {
	if cx, ok := xp[property]; ok {
		if cx != nil {
			return cx(property, path)
		}
		return false
	}
	return true
}
