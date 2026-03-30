package logadapter

import (
	ydb_log "github.com/ydb-platform/ydb-go-sdk/v3/log"
	"go.ytsaurus.tech/library/go/core/log"
)

func fieldToField(field ydb_log.Field) log.Field {
	switch field.Type() {
	case ydb_log.IntType:
		return log.Int(field.Key(), field.IntValue())
	case ydb_log.Int64Type:
		return log.Int64(field.Key(), field.Int64Value())
	case ydb_log.StringType:
		return log.String(field.Key(), field.StringValue())
	case ydb_log.BoolType:
		return log.Bool(field.Key(), field.BoolValue())
	case ydb_log.DurationType:
		return log.Duration(field.Key(), field.DurationValue())
	case ydb_log.StringsType:
		return log.Strings(field.Key(), field.StringsValue())
	case ydb_log.ErrorType:
		return log.Error(field.ErrorValue())
	case ydb_log.StringerType:
		return log.String(field.Key(), field.Stringer().String())
	default:
		return log.Any(field.Key(), field.AnyValue())
	}
}

func ToCoreFields(fields []ydb_log.Field) []log.Field {
	ff := make([]log.Field, len(fields))
	for i, f := range fields {
		ff[i] = fieldToField(f)
	}
	return ff
}
