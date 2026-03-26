package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type Int8Value interface {
	abstract2.Value
	Int8Value() *int8
}

type Int8Type struct {
}

func NewInt8Type() *Int8Type {
	return &Int8Type{}
}

func (typ *Int8Type) Cast(value abstract2.Value) (Int8Value, error) {
	int8Value, ok := value.(Int8Value)
	if ok {
		return int8Value, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to Int8Value", value)
	}
}

func (typ *Int8Type) Validate(value abstract2.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *Int8Type) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeInt8, nil
}

type DefaultInt8Value struct {
	column abstract2.Column
	value  *int8
}

func NewDefaultInt8Value(value *int8, column abstract2.Column) *DefaultInt8Value {
	return &DefaultInt8Value{
		column: column,
		value:  value,
	}
}

func (value *DefaultInt8Value) Column() abstract2.Column {
	return value.column
}

func (value *DefaultInt8Value) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultInt8Value) Int8Value() *int8 {
	return value.value
}

func (value *DefaultInt8Value) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
