package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type UInt64Value interface {
	abstract2.Value
	UInt64Value() *uint64
}

type UInt64Type struct {
}

func NewUInt64Type() *UInt64Type {
	return &UInt64Type{}
}

func (typ *UInt64Type) Cast(value abstract2.Value) (UInt64Value, error) {
	uint64Value, ok := value.(UInt64Value)
	if ok {
		return uint64Value, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to UInt64Value", value)
	}
}

func (typ *UInt64Type) Validate(value abstract2.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *UInt64Type) ToOldType() (ytschema.Type, error) {
	return ytschema.TypeUint64, nil
}

type DefaultUInt64Value struct {
	column abstract2.Column
	value  *uint64
}

func NewDefaultUInt64Value(value *uint64, column abstract2.Column) *DefaultUInt64Value {
	return &DefaultUInt64Value{
		column: column,
		value:  value,
	}
}

func (value *DefaultUInt64Value) Column() abstract2.Column {
	return value.column
}

func (value *DefaultUInt64Value) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultUInt64Value) UInt64Value() *uint64 {
	return value.value
}

func (value *DefaultUInt64Value) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
