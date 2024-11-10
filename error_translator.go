package columbus

// ErrorTranslator is an option that can be passed to Mapper, Mapper.Rows, Mapper.FirstRow and Mapper.ExactlyOneRow
//
// and is called with any errors so that they can be translated (or wrapped)
//
// Is particularly useful for translating sql.ErrNoRows errors to your own 'not found' errors
type ErrorTranslator interface {
	// Translate translates the passed error
	Translate(error) error
}

func translateError(err error, translator ErrorTranslator) error {
	if err == nil {
		return nil
	}
	return translator.Translate(err)
}

type ErrorTranslatorFunc func(error) error

var defaultErrorTranslator ErrorTranslator = &defErrorTranslator{}

type defErrorTranslator struct{}

func (e *defErrorTranslator) Translate(err error) error {
	return err
}
