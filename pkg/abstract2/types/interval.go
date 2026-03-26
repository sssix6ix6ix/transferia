package types

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type IntervalValue interface {
	abstract2.Value
	IntervalValue() *time.Duration
}

type IntervalType struct {
}

func NewIntervalType() *IntervalType {
	return &IntervalType{}
}

func (typ *IntervalType) Cast(value abstract2.Value) (IntervalValue, error) {
	intervalValue, ok := value.(IntervalValue)
	if ok {
		return intervalValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to IntervalValue", value)
	}
}

func (typ *IntervalType) Validate(value abstract2.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *IntervalType) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeInterval, nil
}

type DefaultIntervalValue struct {
	column abstract2.Column
	value  *time.Duration
}

func NewDefaultIntervalValue(value *time.Duration, column abstract2.Column) *DefaultIntervalValue {
	return &DefaultIntervalValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultIntervalValue) Column() abstract2.Column {
	return value.column
}

func (value *DefaultIntervalValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultIntervalValue) IntervalValue() *time.Duration {
	return value.value
}

func (value *DefaultIntervalValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
