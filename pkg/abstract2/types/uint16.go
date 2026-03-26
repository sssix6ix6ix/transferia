package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type UInt16Value interface {
	abstract2.Value
	UInt16Value() *uint16
}

type UInt16Type struct {
}

func NewUInt16Type() *UInt16Type {
	return &UInt16Type{}
}

func (typ *UInt16Type) Cast(value abstract2.Value) (UInt16Value, error) {
	uint8Value, ok := value.(UInt16Value)
	if ok {
		return uint8Value, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to UInt16Value", value)
	}
}

func (typ *UInt16Type) Validate(value abstract2.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *UInt16Type) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeUint16, nil
}

type DefaultUInt16Value struct {
	column abstract2.Column
	value  *uint16
}

func NewDefaultUInt16Value(value *uint16, column abstract2.Column) *DefaultUInt16Value {
	return &DefaultUInt16Value{
		column: column,
		value:  value,
	}
}

func (value *DefaultUInt16Value) Column() abstract2.Column {
	return value.column
}

func (value *DefaultUInt16Value) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultUInt16Value) UInt16Value() *uint16 {
	return value.value
}

func (value *DefaultUInt16Value) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
