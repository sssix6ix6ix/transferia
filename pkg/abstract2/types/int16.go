package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type Int16Value interface {
	abstract2.Value
	Int16Value() *int16
}

type Int16Type struct {
}

func NewInt16Type() *Int16Type {
	return &Int16Type{}
}

func (typ *Int16Type) Cast(value abstract2.Value) (Int16Value, error) {
	int16Value, ok := value.(Int16Value)
	if ok {
		return int16Value, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to Int16Value", value)
	}
}

func (typ *Int16Type) Validate(value abstract2.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *Int16Type) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeInt16, nil
}

type DefaultInt16Value struct {
	column abstract2.Column
	value  *int16
}

func NewDefaultInt16Value(value *int16, column abstract2.Column) *DefaultInt16Value {
	return &DefaultInt16Value{
		column: column,
		value:  value,
	}
}

func (value *DefaultInt16Value) Column() abstract2.Column {
	return value.column
}

func (value *DefaultInt16Value) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultInt16Value) Int16Value() *int16 {
	return value.value
}

func (value *DefaultInt16Value) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
