package changeitem

import (
	"fmt"
	"strings"

	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const (
	DefaultPropertyKey = "default"
)

type ColSchema struct {
	TableSchema  string `json:"table_schema"` // table namespace - for SQL it's database name
	TableName    string `json:"table_name"`
	Path         string `json:"path"`
	ColumnName   string `json:"name"`
	DataType     string `json:"type"` // string with YT type from arcadia/yt/go/schema/schema.go
	PrimaryKey   bool   `json:"key"`
	FakeKey      bool   `json:"fake_key"`      // TODO: remove after migration (TM-1225)
	Required     bool   `json:"required"`      // Required - it's about 'can field contains nil'
	Expression   string `json:"expression"`    // expression for generated columns
	OriginalType string `json:"original_type"` // db-prefix:db-specific-type. example: "mysql:bigint(20) unsigned"

	// field to carry additional optional info.
	// It's either nil or pair key-value. Value can be nil, if it's meaningful
	Properties map[PropertyKey]interface{} `json:"properties,omitempty"`
}

func NewColSchema(columnName string, dataType ytschema.Type, isPrimaryKey bool) ColSchema {
	return ColSchema{
		TableSchema:  "",
		TableName:    "",
		Path:         "",
		ColumnName:   columnName,
		DataType:     dataType.String(),
		PrimaryKey:   isPrimaryKey,
		FakeKey:      false,
		Required:     false,
		Expression:   "",
		OriginalType: "",
		Properties:   nil,
	}
}

func MakeOriginallyTypedColSchema(colName, dataType string, origType string) ColSchema {
	var colSchema ColSchema
	colSchema.ColumnName = colName
	colSchema.DataType = dataType
	colSchema.OriginalType = origType
	return colSchema
}

func MakeTypedColSchema(colName, dataType string, primaryKey bool) ColSchema {
	var colSchema ColSchema
	colSchema.ColumnName = colName
	colSchema.DataType = dataType
	colSchema.PrimaryKey = primaryKey
	return colSchema
}

func (c *ColSchema) AddProperty(key PropertyKey, val interface{}) {
	if c.Properties == nil {
		c.Properties = map[PropertyKey]interface{}{}
	}
	c.Properties[key] = val
}

func (c *ColSchema) Copy() *ColSchema {
	result := *c
	result.Properties = make(map[PropertyKey]interface{})
	for p, v := range c.Properties {
		result.Properties[p] = v
	}
	return &result
}

func (c *ColSchema) ColPath() string {
	if len(c.Path) > 0 {
		return c.Path
	}

	return c.ColumnName
}

func (c *ColSchema) Fqtn() string {
	return c.TableSchema + "_" + c.TableName
}

func (c *ColSchema) TableID() TableID {
	return TableID{Namespace: c.TableSchema, Name: c.TableName}
}

func (c *ColSchema) IsNestedKey() bool {
	return strings.ContainsAny(c.Path, ".") || strings.ContainsAny(c.Path, "/")
}

func (c *ColSchema) IsKey() bool {
	return c.PrimaryKey
}

var numericTypes = map[ytschema.Type]bool{
	ytschema.TypeFloat32: true,
	ytschema.TypeFloat64: true,
	ytschema.TypeInt64:   true,
	ytschema.TypeInt32:   true,
	ytschema.TypeInt16:   true,
	ytschema.TypeInt8:    true,
	ytschema.TypeUint64:  true,
	ytschema.TypeUint32:  true,
	ytschema.TypeUint16:  true,
	ytschema.TypeUint8:   true,
}

func (c *ColSchema) Numeric() bool {
	return numericTypes[ytschema.Type(c.DataType)]
}

func (c *ColSchema) String() string {
	return fmt.Sprintf("Col(Name:%s, Path:%s Type:%s Key:%v Required:%v)", c.ColumnName, c.ColPath(), c.DataType, c.PrimaryKey, c.Required)
}
