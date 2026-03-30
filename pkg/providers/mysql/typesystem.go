package mysql

import (
	"regexp"
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.SourceRules(ProviderType, map[ytschema.Type][]string{
		ytschema.TypeInt64:   {"BIGINT"},
		ytschema.TypeInt32:   {"INT", "MEDIUMINT"},
		ytschema.TypeInt16:   {"SMALLINT"},
		ytschema.TypeInt8:    {"TINYINT"},
		ytschema.TypeUint64:  {"BIGINT UNSIGNED"},
		ytschema.TypeUint32:  {"INT UNSIGNED", "MEDIUMINT UNSIGNED"},
		ytschema.TypeUint16:  {"SMALLINT UNSIGNED"},
		ytschema.TypeUint8:   {"TINYINT UNSIGNED"},
		ytschema.TypeFloat32: {},
		ytschema.TypeFloat64: {"DECIMAL", "DECIMAL UNSIGNED", "DOUBLE", "FLOAT", "FLOAT UNSIGNED"},
		ytschema.TypeBytes: {
			"TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB", "BINARY", "VARBINARY", "BIT",
			"GEOMETRY", "GEOMCOLLECTION", "POINT", "MULTIPOINT", "LINESTRING", "MULTILINESTRING", "POLYGON", "MULTIPOLYGON",
			typesystem.RestPlaceholder,
		},
		ytschema.TypeString:    {"TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "VARCHAR", "CHAR", "TIME", "YEAR", "ENUM", "SET"},
		ytschema.TypeBoolean:   {},
		ytschema.TypeDate:      {"DATE"},
		ytschema.TypeDatetime:  {},
		ytschema.TypeTimestamp: {"DATETIME", "TIMESTAMP"},
		ytschema.TypeAny:       {"JSON"},
	})
	typesystem.TargetRule(ProviderType, map[ytschema.Type]string{
		ytschema.TypeInt64:     "BIGINT",
		ytschema.TypeInt32:     "INT",
		ytschema.TypeInt16:     "SMALLINT",
		ytschema.TypeInt8:      "TINYINT",
		ytschema.TypeUint64:    "BIGINT",
		ytschema.TypeUint32:    "INT",
		ytschema.TypeUint16:    "SMALLINT",
		ytschema.TypeUint8:     "TINYINT",
		ytschema.TypeFloat32:   "FLOAT",
		ytschema.TypeFloat64:   "FLOAT",
		ytschema.TypeBytes:     "TEXT",
		ytschema.TypeString:    "TEXT",
		ytschema.TypeBoolean:   "BIT",
		ytschema.TypeAny:       "JSON",
		ytschema.TypeDate:      "DATE",
		ytschema.TypeDatetime:  "TIMESTAMP",
		ytschema.TypeTimestamp: "TIMESTAMP",
	})
}

func ClearOriginalType(colSchema abstract.ColSchema) string {
	clearTyp := strings.ToUpper(strings.TrimPrefix(colSchema.OriginalType, "mysql:"))
	// we do not need it
	if strings.Contains(clearTyp, "ENUM") {
		return "ENUM"
	}
	if strings.Contains(clearTyp, "SET") {
		return "SET"
	}
	clearTyp = strings.ReplaceAll(clearTyp, " ZEROFILL", "")
	clearTyp = regexp.MustCompile(`\(\d*\)`).ReplaceAllString(clearTyp, "")
	clearTyp = regexp.MustCompile(`\(\d*\,\d*\)`).ReplaceAllString(clearTyp, "")
	return clearTyp
}
