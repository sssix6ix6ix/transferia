package elastic

import (
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.SourceRules(ProviderType, map[ytschema.Type][]string{
		ytschema.TypeInt64:   {"long"},
		ytschema.TypeInt32:   {"integer"},
		ytschema.TypeInt16:   {"short"},
		ytschema.TypeInt8:    {"byte"},
		ytschema.TypeUint64:  {"unsigned_long"},
		ytschema.TypeUint32:  {},
		ytschema.TypeUint16:  {},
		ytschema.TypeUint8:   {},
		ytschema.TypeFloat32: {"float", "half_float"},
		ytschema.TypeFloat64: {"double", "scaled_float", "rank_feature"},
		ytschema.TypeBytes:   {"binary"},
		ytschema.TypeString:  {"text", "ip", "constant_keyword", "match_only_text", "search_as_you_type"},
		ytschema.TypeBoolean: {"boolean"},
		ytschema.TypeAny: {
			"object", "nested", "join", "flattened", "integer_range", "float_range", "long_range", "double_range",
			"date_range", "ip_range", "keyword", "wildcard", "version", "aggregate_metric_double", "histogram",
			"completion", "dense_vector", "geo_point", "point", "rank_features", "geo_shape", "shape", "percolator",
		},
		ytschema.TypeDate:      {},
		ytschema.TypeDatetime:  {},
		ytschema.TypeTimestamp: {"date", "date_nanos"},
	})

	typesystem.TargetRule(ProviderType, map[ytschema.Type]string{
		ytschema.TypeInt64:     "long",
		ytschema.TypeInt32:     "integer",
		ytschema.TypeInt16:     "short",
		ytschema.TypeInt8:      "byte",
		ytschema.TypeUint64:    "unsigned_long",
		ytschema.TypeUint32:    "unsigned_long",
		ytschema.TypeUint16:    "unsigned_long",
		ytschema.TypeUint8:     "unsigned_long",
		ytschema.TypeFloat32:   "float",
		ytschema.TypeFloat64:   "double",
		ytschema.TypeBytes:     "binary",
		ytschema.TypeString:    "text",
		ytschema.TypeBoolean:   "boolean",
		ytschema.TypeAny:       "object",
		ytschema.TypeDate:      "date",
		ytschema.TypeDatetime:  "date",
		ytschema.TypeTimestamp: "date",
	})
}
