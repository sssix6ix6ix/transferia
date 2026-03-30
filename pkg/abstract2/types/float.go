package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type FloatValue interface {
	abstract2.Value
	FloatValue() *float32
}

type FloatType struct {
}

func NewFloatType() *FloatType {
	return &FloatType{}
}

func (typ *FloatType) Cast(value abstract2.Value) (FloatValue, error) {
	floatValue, ok := value.(FloatValue)
	if ok {
		return floatValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to FloatValue", value)
	}
}

func (typ *FloatType) Validate(value abstract2.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *FloatType) ToOldType() (ytschema.Type, error) {
	return ytschema.TypeFloat32, nil
}

type DefaultFloatValue struct {
	column abstract2.Column
	value  *float32
}

func NewDefaultFloatValue(value *float32, column abstract2.Column) *DefaultFloatValue {
	return &DefaultFloatValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultFloatValue) Column() abstract2.Column {
	return value.column
}

func (value *DefaultFloatValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultFloatValue) FloatValue() *float32 {
	return value.value
}

func (value *DefaultFloatValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
