package clickhouse

import (
	"slices"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.SourceRules(ProviderType, map[ytschema.Type][]string{
		ytschema.TypeInt64:     {"Int64"},
		ytschema.TypeInt32:     {"Int32"},
		ytschema.TypeInt16:     {"Int16"},
		ytschema.TypeInt8:      {"Int8"},
		ytschema.TypeUint64:    {"UInt64"},
		ytschema.TypeUint32:    {"UInt32"},
		ytschema.TypeUint16:    {"UInt16"},
		ytschema.TypeUint8:     {"UInt8"},
		ytschema.TypeFloat32:   {},
		ytschema.TypeFloat64:   {"Float64"},
		ytschema.TypeBytes:     {"FixedString", "String"},
		ytschema.TypeString:    {"IPv4", "IPv6", "Enum8", "Enum16"},
		ytschema.TypeBoolean:   {},
		ytschema.TypeAny:       {typesystem.RestPlaceholder},
		ytschema.TypeDate:      {"Date"},
		ytschema.TypeDatetime:  {"DateTime"},
		ytschema.TypeTimestamp: {"DateTime64"},
	})
	typesystem.TargetRule(ProviderType, map[ytschema.Type]string{
		ytschema.TypeInt64:     "Int64",
		ytschema.TypeInt32:     "Int32",
		ytschema.TypeInt16:     "Int16",
		ytschema.TypeInt8:      "Int8",
		ytschema.TypeUint64:    "UInt64",
		ytschema.TypeUint32:    "UInt32",
		ytschema.TypeUint16:    "UInt16",
		ytschema.TypeUint8:     "UInt8",
		ytschema.TypeFloat32:   "Float64",
		ytschema.TypeFloat64:   "Float64",
		ytschema.TypeBytes:     "String",
		ytschema.TypeString:    "String",
		ytschema.TypeBoolean:   "UInt8",
		ytschema.TypeAny:       "String",
		ytschema.TypeDate:      "Date",
		ytschema.TypeDatetime:  "DateTime",
		ytschema.TypeTimestamp: "DateTime64(9)",
	})
}

// availableTypesAlters is list of column type changes, used when ChDestination.MigrationOptions.AddNewColumns enabled.
var availableTypesAlters = map[string][]string{
	"Int8":  {"Int16", "Int32", "Int64"},
	"Int16": {"Int32", "Int64"},
	"Int32": {"Int64"},

	"UInt8":  {"UInt16", "UInt32", "UInt64"},
	"UInt16": {"UInt32", "UInt64"},
	"UInt32": {"UInt64"},
}

// isAlterPossible returns nil if alter is possible, otherwise returns cause in error.
func isAlterPossible(old, new abstract.ColSchema) error {
	if isOldNull, isNewNull := isCHNullable(&old), isCHNullable(&new); isOldNull != isNewNull {
		return xerrors.Errorf("Nullable cannot change (%v -> %v)", isOldNull, isNewNull)
	}
	oldType := chColumnType(old)
	newType := chColumnType(new)
	if oldType == newType {
		return xerrors.Errorf("Types suggested equal (%s -> %s)", oldType, newType)
	}
	if columntypes.IsCompositeType(oldType) || columntypes.IsCompositeType(newType) {
		return xerrors.Errorf("Types change with modifiers is not allowed (%s -> %s)", oldType, newType)
	}
	oldBase := columntypes.BaseType(strings.TrimPrefix(oldType, originalTypePrefix))
	newBase := columntypes.BaseType(strings.TrimPrefix(newType, originalTypePrefix))
	if !slices.Contains(availableTypesAlters[oldBase], newBase) {
		return xerrors.Errorf("Types change %s -> %s is not allowed", oldType, newType)
	}
	return nil
}

func chColumnType(col abstract.ColSchema) string {
	if origType, ok := getCHOriginalType(col.OriginalType); ok {
		return origType
	}
	return columntypes.ToChType(col.DataType)
}
