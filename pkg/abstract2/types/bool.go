package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type BoolValue interface {
	abstract2.Value
	BoolValue() *bool
}

type BoolType struct {
}

func NewBoolType() *BoolType {
	return &BoolType{}
}

func (typ *BoolType) Cast(value abstract2.Value) (BoolValue, error) {
	boolValue, ok := value.(BoolValue)
	if ok {
		return boolValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to BoolValue", value)
	}
}

func (typ *BoolType) Validate(value abstract2.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *BoolType) ToOldType() (ytschema.Type, error) {
	return ytschema.TypeBoolean, nil
}

type DefaultBoolValue struct {
	column abstract2.Column
	value  *bool
}

func NewDefaultBoolValue(value *bool, column abstract2.Column) *DefaultBoolValue {
	return &DefaultBoolValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultBoolValue) Column() abstract2.Column {
	return value.column
}

func (value *DefaultBoolValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultBoolValue) BoolValue() *bool {
	return value.value
}

func (value *DefaultBoolValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
