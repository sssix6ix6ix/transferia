package events

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
)

func validateValue(value abstract2.Value) error {
	if err := value.Column().Type().Validate(value); err != nil {
		return xerrors.Errorf("Column '%v', value validation error: %w", value.Column().FullName(), err)
	}
	return nil
}
