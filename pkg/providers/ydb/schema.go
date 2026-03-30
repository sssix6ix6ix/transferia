package ydb

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
	ydb_options "github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type column struct {
	Name string
	Type string
}

func buildColumnDescription(col *column, isPkey bool) abstract.ColSchema {
	ydbTypeStr := col.Type
	isOptional := strings.Contains(ydbTypeStr, "Optional") || strings.Contains(ydbTypeStr, "?")
	ydbTypeStr = strings.ReplaceAll(ydbTypeStr, "?", "")
	ydbTypeStr = strings.ReplaceAll(ydbTypeStr, "Optional<", "")
	ydbTypeStr = strings.ReplaceAll(ydbTypeStr, ">", "")
	if bracketsStart := strings.Index(ydbTypeStr, "("); bracketsStart > 0 {
		ydbTypeStr = ydbTypeStr[:bracketsStart]
	}

	var dataType ytschema.Type
	switch ydbTypeStr {
	case "Bool":
		dataType = ytschema.TypeBoolean
	case "Int8":
		dataType = ytschema.TypeInt8
	case "Int16":
		dataType = ytschema.TypeInt16
	case "Int32":
		dataType = ytschema.TypeInt32
	case "Int64":
		dataType = ytschema.TypeInt64
	case "Uint8":
		dataType = ytschema.TypeUint8
	case "Uint16":
		dataType = ytschema.TypeUint16
	case "Uint32":
		dataType = ytschema.TypeUint32
	case "Uint64":
		dataType = ytschema.TypeUint64
	case "Float":
		dataType = ytschema.TypeFloat32
	case "Double":
		dataType = ytschema.TypeFloat64
	case "String":
		dataType = ytschema.TypeBytes
	case "Utf8", "Decimal", "DyNumber":
		dataType = ytschema.TypeString
	case "Date":
		dataType = ytschema.TypeDate
	case "Datetime":
		dataType = ytschema.TypeDatetime
	case "Timestamp":
		dataType = ytschema.TypeTimestamp
	case "Interval":
		dataType = ytschema.TypeInterval
	case "Uuid":
		dataType = ytschema.TypeString
	default:
		dataType = ytschema.TypeAny
	}

	return abstract.ColSchema{
		ColumnName:   col.Name,
		DataType:     string(dataType),
		Required:     !isOptional,
		OriginalType: "ydb:" + ydbTypeStr,
		PrimaryKey:   isPkey,
		TableSchema:  "",
		TableName:    "",
		Path:         "",
		FakeKey:      false,
		Expression:   "",
		Properties:   nil,
	}
}

func fromYdbSchemaImpl(original []column, keys []string) abstract.TableColumns {
	columnNameToPKey := map[string]bool{}
	for _, k := range keys {
		columnNameToPKey[k] = true
	}
	columnNameToIndex := make(map[string]int)
	for i, el := range original {
		columnNameToIndex[el.Name] = i
	}

	result := make([]abstract.ColSchema, 0, len(original))
	for _, currentKey := range keys {
		index := columnNameToIndex[currentKey]
		result = append(result, buildColumnDescription(&original[index], true))
	}
	for i, currentColumn := range original {
		if !columnNameToPKey[currentColumn.Name] {
			result = append(result, buildColumnDescription(&original[i], false))
		}
	}
	return result
}

func FromYdbSchema(original []ydb_options.Column, keys []string) abstract.TableColumns {
	columns := make([]column, len(original))
	for i, el := range original {
		columns[i] = column{
			Name: el.Name,
			Type: fmt.Sprintf("%v", el.Type),
		}
	}
	return fromYdbSchemaImpl(columns, keys)
}
