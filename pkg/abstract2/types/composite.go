package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	yt_schema "go.ytsaurus.tech/yt/go/schema"
)

type CompositeValue interface {
	abstract2.Value
	CompositeValue() interface{}
}

type CompositeType struct {
}

func NewCompositeType() *CompositeType {
	return &CompositeType{}
}

func (typ *CompositeType) Cast(value abstract2.Value) (CompositeValue, error) {
	compositeValue, ok := value.(CompositeValue)
	if ok {
		return compositeValue, nil
	} else {
		return nil, xerrors.Errorf("Can't cast value of type '%T' to AnyValue", value)
	}
}

func (typ *CompositeType) Validate(value abstract2.Value) error {
	_, err := typ.Cast(value)
	return err
}

func (typ *CompositeType) ToOldType() (yt_schema.Type, error) {
	return yt_schema.TypeString, nil
}

type DefaultCompositeValue struct {
	column abstract2.Column
	value  interface{}
}

func NewDefaultCompositeValue(value interface{}, column abstract2.Column) *DefaultCompositeValue {
	return &DefaultCompositeValue{
		column: column,
		value:  value,
	}
}

func (value *DefaultCompositeValue) Column() abstract2.Column {
	return value.column
}

func (value *DefaultCompositeValue) Value() interface{} {
	if value.value == nil {
		return nil
	}
	return value.value
}

func (value *DefaultCompositeValue) CompositeValue() interface{} {
	return value.value
}

func (value *DefaultCompositeValue) ToOldValue() (interface{}, error) {
	if value.value == nil {
		return nil, nil
	}
	return value.value, nil
}
