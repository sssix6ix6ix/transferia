package types

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type TimestampValue interface {
	abstract2.Value
	TimestampValue() *time.Time
}

type TimestampType struct {
	precision int
}

func NewTimestampType(precision int) *TimestampType {
	return &TimestampType{
		precision: precision,
	}
}

func (typ *TimestampType) Cast(value abstract2.Value) (TimestampValue, error) {
	dateValue, ok := value.(TimestampValue)
	if ok {
		return dateValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to TimestampValue", value)
	}
}

func (typ *TimestampType) Validate(value abstract2.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *TimestampType) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeTimestamp, nil
}

func (typ *TimestampType) Precision() int {
	return typ.precision
}

type DefaultTimestampValue struct {
	column abstract2.Column
	value  *time.Time
}

func NewDefaultTimestampValue(value *time.Time, column abstract2.Column) *DefaultTimestampValue {
	return &DefaultTimestampValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultTimestampValue) Column() abstract2.Column {
	return value.column
}

func (value *DefaultTimestampValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultTimestampValue) TimestampValue() *time.Time {
	return value.value
}

func (value *DefaultTimestampValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
