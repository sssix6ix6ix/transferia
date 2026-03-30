package mongo

import (
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.SourceRules(ProviderType, map[ytschema.Type][]string{
		ytschema.TypeInt64:     {},
		ytschema.TypeInt32:     {},
		ytschema.TypeInt16:     {},
		ytschema.TypeInt8:      {},
		ytschema.TypeUint64:    {},
		ytschema.TypeUint32:    {},
		ytschema.TypeUint16:    {},
		ytschema.TypeUint8:     {},
		ytschema.TypeFloat32:   {},
		ytschema.TypeFloat64:   {},
		ytschema.TypeBytes:     {},
		ytschema.TypeString:    {"bson_id"},
		ytschema.TypeBoolean:   {},
		ytschema.TypeAny:       {"bson"},
		ytschema.TypeDate:      {},
		ytschema.TypeDatetime:  {},
		ytschema.TypeTimestamp: {},
		ytschema.TypeInterval:  {},
	})
	typesystem.TargetRule(ProviderType, map[ytschema.Type]string{
		ytschema.TypeInt64:     "bson",
		ytschema.TypeInt32:     "bson",
		ytschema.TypeInt16:     "bson",
		ytschema.TypeInt8:      "bson",
		ytschema.TypeUint64:    "bson",
		ytschema.TypeUint32:    "bson",
		ytschema.TypeUint16:    "bson",
		ytschema.TypeUint8:     "bson",
		ytschema.TypeFloat32:   "bson",
		ytschema.TypeFloat64:   "bson",
		ytschema.TypeBytes:     "bson",
		ytschema.TypeString:    "bson",
		ytschema.TypeBoolean:   "bson",
		ytschema.TypeAny:       "bson",
		ytschema.TypeDate:      "bson",
		ytschema.TypeDatetime:  "bson",
		ytschema.TypeTimestamp: "bson",
	})
}
