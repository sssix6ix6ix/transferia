package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type BigFloatValue interface {
	abstract2.Value
	BigFloatValue() *string
}

type BigFloatType struct {
	precision int
}

func NewBigFloatType(precision int) *BigFloatType {
	return &BigFloatType{
		precision: precision,
	}
}

func (typ *BigFloatType) Cast(value abstract2.Value) (BigFloatValue, error) {
	BigFloatValue, ok := value.(BigFloatValue)
	if ok {
		return BigFloatValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to BigFloatValue", value)
	}
}

func (typ *BigFloatType) Validate(value abstract2.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *BigFloatType) ToOldType() (ytschema.Type, error) {
	return ytschema.TypeString, nil
}

func (typ *BigFloatType) Precision() int {
	return typ.precision
}

type DefaultBigFloatValue struct {
	column abstract2.Column
	value  *string
}

func NewDefaultBigFloatValue(value *string, column abstract2.Column) *DefaultBigFloatValue {
	return &DefaultBigFloatValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultBigFloatValue) Column() abstract2.Column {
	return value.column
}

func (value *DefaultBigFloatValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultBigFloatValue) BigFloatValue() *string {
	return value.value
}

func (value *DefaultBigFloatValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
