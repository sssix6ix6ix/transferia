package airbyte

import (
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func init() {
	// see: https://docs.airbyte.com/understanding-airbyte/supported-data-types/#the-types
	typesystem.SourceRules(ProviderType, map[ytschema.Type][]string{
		ytschema.TypeInt64:   {"integer"},
		ytschema.TypeInt32:   {},
		ytschema.TypeInt16:   {},
		ytschema.TypeInt8:    {},
		ytschema.TypeUint64:  {},
		ytschema.TypeUint32:  {},
		ytschema.TypeUint16:  {},
		ytschema.TypeUint8:   {},
		ytschema.TypeFloat32: {},
		ytschema.TypeFloat64: {"number"},
		ytschema.TypeBytes:   {},
		ytschema.TypeString:  {"time_without_timezone", "time_with_timezone", "string"},
		ytschema.TypeBoolean: {"boolean"},
		ytschema.TypeAny: {
			"object", "array", typesystem.RestPlaceholder,
		},
		ytschema.TypeDate:      {"date"},
		ytschema.TypeDatetime:  {"date-time"},
		ytschema.TypeTimestamp: {"timestamp", "timestamp_with_timezone", "timestamp_without_timezone"},
	})
}
