package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type DecimalValue interface {
	abstract2.Value
	DecimalValue() *string
}

type DecimalType struct {
	precision int
	scale     int
}

func NewDecimalType(precision int, scale int) *DecimalType {
	return &DecimalType{
		precision: precision,
		scale:     scale,
	}
}

func (typ *DecimalType) Cast(value abstract2.Value) (DecimalValue, error) {
	decimalValue, ok := value.(DecimalValue)
	if ok {
		return decimalValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to DecimalValue", value)
	}
}

func (typ *DecimalType) Validate(value abstract2.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *DecimalType) ToOldType() (ytschema.Type, error) {
	return ytschema.TypeString, nil
}

func (typ *DecimalType) Precision() int {
	return typ.precision
}

func (typ *DecimalType) Scale() int {
	return typ.scale
}

type DefaultDecimalValue struct {
	column abstract2.Column
	value  *string
}

func NewDefaultDecimalValue(value *string, column abstract2.Column) *DefaultDecimalValue {
	return &DefaultDecimalValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultDecimalValue) Column() abstract2.Column {
	return value.column
}

func (value *DefaultDecimalValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return *value.value
}

func (value *DefaultDecimalValue) DecimalValue() *string {
	return value.value
}

func (value *DefaultDecimalValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return *value.value, nil
}
