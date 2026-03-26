package types

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type DateValue interface {
	abstract2.Value
	DateValue() *time.Time
}

type DateType struct {
}

func NewDateType() *DateType {
	return &DateType{}
}

func (typ *DateType) Cast(value abstract2.Value) (DateValue, error) {
	dateValue, ok := value.(DateValue)
	if ok {
		return dateValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to DateValue", value)
	}
}

func (typ *DateType) Validate(value abstract2.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *DateType) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeDate, nil
}

type DefaultDateValue struct {
	column abstract2.Column
	value  *time.Time
}

func NewDefaultDateValue(value *time.Time, column abstract2.Column) *DefaultDateValue {
	return &DefaultDateValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultDateValue) Column() abstract2.Column {
	return value.column
}

func (value *DefaultDateValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultDateValue) DateValue() *time.Time {
	return value.value
}

func (value *DefaultDateValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}

	return *value.value, nil
}
