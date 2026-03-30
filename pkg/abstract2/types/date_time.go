package types

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type DateTimeValue interface {
	abstract2.Value
	DateTimeValue() *time.Time
}

type DateTimeType struct {
}

func NewDateTimeType() *DateTimeType {
	return &DateTimeType{}
}

func (typ *DateTimeType) Cast(value abstract2.Value) (DateTimeValue, error) {
	dateTimeValue, ok := value.(DateTimeValue)
	if ok {
		return dateTimeValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to DateTimeValue", value)
	}
}

func (typ *DateTimeType) Validate(value abstract2.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *DateTimeType) ToOldType() (ytschema.Type, error) {
	return ytschema.TypeDatetime, nil
}

type DefaultDateTimeValue struct {
	column abstract2.Column
	value  *time.Time
}

func NewDefaultDateTimeValue(value *time.Time, column abstract2.Column) *DefaultDateTimeValue {
	return &DefaultDateTimeValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultDateTimeValue) Column() abstract2.Column {
	return value.column
}

func (value *DefaultDateTimeValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultDateTimeValue) DateTimeValue() *time.Time {
	return value.value
}

func (value *DefaultDateTimeValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
