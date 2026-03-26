package types

import (
	"encoding/json"

	"github.com/transferia/transferia/pkg/abstract2"
	"go.ytsaurus.tech/yt/go/schema"
)

type JSONValue interface {
	abstract2.Value
	JSONValue() ([]byte, error)
}

type DefaultJSONValue struct {
	column abstract2.Column
	raw    interface{}
}

func (v *DefaultJSONValue) Column() abstract2.Column {
	return v.column
}

func (v *DefaultJSONValue) Value() interface{} {
	return v.raw
}

func (v *DefaultJSONValue) ToOldValue() (interface{}, error) {
	return v.raw, nil
}

func (v *DefaultJSONValue) JSONValue() ([]byte, error) {
	return json.Marshal(v.raw)
}

func NewDefaultJSONValue(value interface{}, column abstract2.Column) *DefaultJSONValue {
	return &DefaultJSONValue{
		column: column,
		raw:    value,
	}
}

type JSONType struct{}

func (t *JSONType) Validate(value abstract2.Value) error {
	return nil
}

func (t *JSONType) ToOldType() (schema.Type, error) {
	return schema.TypeAny, nil
}

func NewJSONType() *JSONType {
	return &JSONType{}
}
