package ydb

import (
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.SourceRules(ProviderType, map[ytschema.Type][]string{
		ytschema.TypeInt64:     {"Int64"},
		ytschema.TypeInt32:     {"Int32"},
		ytschema.TypeInt16:     {"Int16"},
		ytschema.TypeInt8:      {"Int8"},
		ytschema.TypeUint64:    {"Uint64"},
		ytschema.TypeUint32:    {"Uint32"},
		ytschema.TypeUint16:    {"Uint16"},
		ytschema.TypeUint8:     {"Uint8"},
		ytschema.TypeFloat32:   {"Float"},
		ytschema.TypeFloat64:   {"Double"},
		ytschema.TypeBytes:     {"String"},
		ytschema.TypeString:    {"Utf8", "Decimal", "DyNumber", "Uuid"},
		ytschema.TypeBoolean:   {"Bool"},
		ytschema.TypeAny:       {typesystem.RestPlaceholder},
		ytschema.TypeDate:      {"Date"},
		ytschema.TypeDatetime:  {"Datetime"},
		ytschema.TypeTimestamp: {"Timestamp"},
		ytschema.TypeInterval:  {"Interval"},
	})
	typesystem.TargetRule(ProviderType, map[ytschema.Type]string{
		ytschema.TypeInt64:     "Int64",
		ytschema.TypeInt32:     "Int32",
		ytschema.TypeInt16:     "Int32",
		ytschema.TypeInt8:      "Int32",
		ytschema.TypeUint64:    "Uint64",
		ytschema.TypeUint32:    "Uint32",
		ytschema.TypeUint16:    "Uint32",
		ytschema.TypeUint8:     "Uint8",
		ytschema.TypeFloat32:   typesystem.NotSupportedPlaceholder,
		ytschema.TypeFloat64:   "Double",
		ytschema.TypeBytes:     "String",
		ytschema.TypeString:    "Utf8",
		ytschema.TypeBoolean:   "Bool",
		ytschema.TypeAny:       "Json",
		ytschema.TypeDate:      "Date",
		ytschema.TypeDatetime:  "Datetime",
		ytschema.TypeTimestamp: "Timestamp",
	})
}
