package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type UInt8Value interface {
	abstract2.Value
	UInt8Value() *uint8
}

type UInt8Type struct {
}

func NewUInt8Type() *UInt8Type {
	return &UInt8Type{}
}

func (typ *UInt8Type) Cast(value abstract2.Value) (UInt8Value, error) {
	uint8Value, ok := value.(UInt8Value)
	if ok {
		return uint8Value, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to UInt8Value", value)
	}
}

func (typ *UInt8Type) Validate(value abstract2.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *UInt8Type) ToOldType() (ytschema.Type, error) {
	return ytschema.TypeUint8, nil
}

type DefaultUInt8Value struct {
	column abstract2.Column
	value  *uint8
}

func NewDefaultUInt8Value(value *uint8, column abstract2.Column) *DefaultUInt8Value {
	return &DefaultUInt8Value{
		column: column,
		value:  value,
	}
}

func (value *DefaultUInt8Value) Column() abstract2.Column {
	return value.column
}

func (value *DefaultUInt8Value) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultUInt8Value) UInt8Value() *uint8 {
	return value.value
}

func (value *DefaultUInt8Value) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
