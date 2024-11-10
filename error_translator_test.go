package columbus

import (
	"database/sql"
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDefaultErrorTranslator(t *testing.T) {
	require.NoError(t, defaultErrorTranslator.Translate(nil))
	require.Error(t, defaultErrorTranslator.Translate(errors.New("")))
}

type testErrorTranslator struct{}

var _ ErrorTranslator = &testErrorTranslator{}

func (t testErrorTranslator) Translate(err error) error {
	if err == sql.ErrNoRows {
		return errors.New("no rows found!!!")
	}
	return err
}
