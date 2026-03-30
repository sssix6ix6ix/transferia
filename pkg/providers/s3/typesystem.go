package s3

import (
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const ProviderType = s3_model.ProviderType

func init() {
	typesystem.SourceRules(ProviderType, map[ytschema.Type][]string{
		ytschema.TypeInt64:     {"csv:int64", "parquet:INT64"},
		ytschema.TypeInt32:     {"csv:int32", "parquet:INT32"},
		ytschema.TypeInt16:     {"csv:int16", "parquet:INT16"},
		ytschema.TypeInt8:      {"csv:int8", "parquet:INT8"},
		ytschema.TypeUint64:    {"csv:uint64", "parquet:UINT64", "jsonl:uint64"},
		ytschema.TypeUint32:    {"csv:uint32", "parquet:UINT32"},
		ytschema.TypeUint16:    {"csv:uint16", "parquet:UINT16"},
		ytschema.TypeUint8:     {"csv:uint8", "parquet:UINT8"},
		ytschema.TypeFloat32:   {"csv:float", "parquet:FLOAT"},
		ytschema.TypeFloat64:   {"csv:double", "parquet:DOUBLE", "parquet:DECIMAL", "jsonl:number"},
		ytschema.TypeBytes:     {"csv:string", "parquet:BYTE_ARRAY", "parquet:FIXED_LEN_BYTE_ARRAY"},
		ytschema.TypeString:    {"csv:utf8", "parquet:STRING", "parquet:INT96", "jsonl:string", "jsonl:utf8"},
		ytschema.TypeBoolean:   {"csv:boolean", "parquet:BOOLEAN", "jsonl:boolean"},
		ytschema.TypeAny:       {"csv:any", typesystem.RestPlaceholder, "jsonl:object", "jsonl:array"},
		ytschema.TypeDate:      {"csv:date", "parquet:DATE"},
		ytschema.TypeDatetime:  {"csv:datetime"},
		ytschema.TypeTimestamp: {"csv:timestamp", "parquet:TIMESTAMP", "jsonl:timestamp"},
		ytschema.TypeInterval:  {"csv:interval"},
	})
	typesystem.TargetRule(ProviderType, map[ytschema.Type]string{
		ytschema.TypeInt64:     "",
		ytschema.TypeInt32:     "",
		ytschema.TypeInt16:     "",
		ytschema.TypeInt8:      "",
		ytschema.TypeUint64:    "",
		ytschema.TypeUint32:    "",
		ytschema.TypeUint16:    "",
		ytschema.TypeUint8:     "",
		ytschema.TypeFloat32:   "",
		ytschema.TypeFloat64:   "",
		ytschema.TypeBytes:     "",
		ytschema.TypeString:    "",
		ytschema.TypeBoolean:   "",
		ytschema.TypeAny:       "",
		ytschema.TypeDate:      "",
		ytschema.TypeDatetime:  "",
		ytschema.TypeTimestamp: "",
	})
}
