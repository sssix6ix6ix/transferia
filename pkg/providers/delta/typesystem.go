package delta

import (
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	delta_types "github.com/transferia/transferia/pkg/providers/delta/types"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.SourceRules(ProviderType, map[ytschema.Type][]string{
		ytschema.TypeInt64:     new(delta_types.LongType).Aliases(),
		ytschema.TypeInt32:     new(delta_types.IntegerType).Aliases(),
		ytschema.TypeInt16:     new(delta_types.ShortType).Aliases(),
		ytschema.TypeInt8:      new(delta_types.ByteType).Aliases(),
		ytschema.TypeUint64:    {},
		ytschema.TypeUint32:    {},
		ytschema.TypeUint16:    {},
		ytschema.TypeUint8:     {},
		ytschema.TypeFloat32:   {new(delta_types.DoubleType).Name()},
		ytschema.TypeFloat64:   new(delta_types.FloatType).Aliases(),
		ytschema.TypeBytes:     {new(delta_types.BinaryType).Name()},
		ytschema.TypeString:    {new(delta_types.StringType).Name()},
		ytschema.TypeBoolean:   {new(delta_types.BooleanType).Name()},
		ytschema.TypeDate:      {new(delta_types.DateType).Name()},
		ytschema.TypeDatetime:  {},
		ytschema.TypeTimestamp: {new(delta_types.TimestampType).Name()},
		ytschema.TypeInterval:  {},
		ytschema.TypeAny: {
			typesystem.RestPlaceholder,
		},
	})
}
