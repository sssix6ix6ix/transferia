package mysql

import (
	sql_driver "database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func isUnsigned(rawColumnType string) bool {
	// We can't use HasPrefix(rawColumnType, " unsigned")
	// bcs of such rawColumnType:
	//     "bigint(20) unsigned zerofill"
	// boyer-moore is ~13-17%% faster than Contains for such input data
	//     but for LoadSchema it's overengineering - it's not a bottleneck
	// ADDED: but zerofill always means unsigned. I can HasPrefix these 2 strings
	return strings.Contains(rawColumnType, " unsigned")
}

func CastToMySQL(val interface{}, typ abstract.ColSchema) string {
	if typ.Expression != "" {
		return "default"
	}
	if val == nil {
		if typ.Required {
			return "default"
		}
		return "null"
	}

	if abstract.IsMysqlBinaryType(typ.OriginalType) {
		if vStr, ok := val.(string); ok {
			val = []byte(vStr)
		}
		if valueBytes, ok := val.([]byte); ok {
			valueHexStr := hex.EncodeToString(valueBytes)
			return fmt.Sprintf("X'%v'", valueHexStr)
		}
	}
	if typ.DataType == string(ytschema.TypeAny) {
		valueJSON, _ := json.Marshal(val)
		return quoteString(string(valueJSON))
	}

	switch v := val.(type) {
	case sql_driver.Valuer:
		val, _ := v.Value()
		return CastToMySQL(val, typ)
	case string:
		return strings.ToValidUTF8(quoteString(v), "�")
	case time.Time:
		switch typ.DataType {
		case string(ytschema.TypeDate):
			return fmt.Sprintf("'%v'", v.Format(layoutDateMySQL))
		default:
			return fmt.Sprintf("'%v'", v.Format(layoutDatetimeMySQL))
		}
	case []byte:
		return CastToMySQL(string(v), typ)
	case *int:
		return fmt.Sprintf("%v", *v)
	case *int16:
		return fmt.Sprintf("%v", *v)
	case *int32:
		return fmt.Sprintf("%v", *v)
	case *int64:
		return fmt.Sprintf("%v", *v)
	case *uint:
		return fmt.Sprintf("%v", *v)
	case *uint32:
		return fmt.Sprintf("%v", *v)
	case *uint64:
		return fmt.Sprintf("%v", *v)
	case int:
		return fmt.Sprintf("%v", v)
	case int16:
		return fmt.Sprintf("%v", v)
	case int32:
		return fmt.Sprintf("%v", v)
	case int64:
		return fmt.Sprintf("%v", v)
	case uint:
		return fmt.Sprintf("%v", v)
	case uint32:
		return fmt.Sprintf("%v", v)
	case uint64:
		return fmt.Sprintf("%v", v)
	case float64:
		return fmt.Sprintf("%v", v)
	case *float64:
		return fmt.Sprintf("%v", *v)
	default:
		return fmt.Sprintf("'%v'", v)
	}
}

const (
	layoutDateMySQL     = "2006-01-02"
	layoutDatetimeMySQL = "2006-01-02 15:04:05.999999"
)

func quoteString(str string) string {
	escaped := strings.ReplaceAll(str, "\\", "\\\\")
	return fmt.Sprintf("'%v'", strings.ReplaceAll(escaped, "'", "''"))
}

func TypeToMySQL(column abstract.ColSchema) string {
	if strings.HasPrefix(column.OriginalType, "mysql:") {
		return strings.TrimPrefix(column.OriginalType, "mysql:")
	}
	switch ytschema.Type(column.DataType) {
	case ytschema.TypeAny:
		return "JSON"
	case ytschema.TypeBoolean:
		return "BIT(2)"
	case ytschema.TypeString:
		return "TEXT"
	case ytschema.TypeInterval:
		return "TEXT"
	case ytschema.TypeBytes:
		return "TEXT"
	case ytschema.TypeInt8, ytschema.TypeUint8:
		return "TINYINT"
	case ytschema.TypeInt16, ytschema.TypeUint16:
		return "SMALLINT"
	case ytschema.TypeInt32, ytschema.TypeUint32:
		return "INT"
	case ytschema.TypeInt64, ytschema.TypeUint64:
		return "BIGINT"
	case ytschema.TypeFloat64, ytschema.TypeFloat32:
		return "FLOAT"
	case ytschema.TypeDate:
		return "DATE"
	case ytschema.TypeDatetime, ytschema.TypeTimestamp:
		return "TIMESTAMP"
	}
	return column.DataType
}

func TypeToYt(rawColumnType string) ytschema.Type {
	return typeToYt(abstract.TrimMySQLType(rawColumnType), rawColumnType)
}

func typeToYt(dataType string, rawColumnType string) ytschema.Type {
	unsigned := isUnsigned(rawColumnType)

	switch dataType {
	case "tinyint":
		if unsigned {
			return ytschema.TypeUint8
		} else {
			return ytschema.TypeInt8
		}
	case "smallint":
		if unsigned {
			return ytschema.TypeUint16
		} else {
			return ytschema.TypeInt16
		}
	case "int", "mediumint":
		if unsigned {
			return ytschema.TypeUint32
		} else {
			return ytschema.TypeInt32
		}
	case "bigint":
		if unsigned {
			return ytschema.TypeUint64
		} else {
			return ytschema.TypeInt64
		}
	case "decimal", "double", "float":
		return ytschema.TypeFloat64
	case "date":
		return ytschema.TypeDate
	case "datetime", "timestamp":
		return ytschema.TypeTimestamp // TODO: TM-2944
	case "tinytext", "text", "mediumtext", "longtext", "varchar", "char", "time", "year", "enum", "set":
		return ytschema.TypeString
	case "tinyblob", "blob", "mediumblob", "longblob", "binary", "varbinary", "bit":
		return ytschema.TypeBytes
	case "geometry", "geomcollection", "point", "multipoint", "linestring", "multilinestring", "polygon", "multipolygon":
		return ytschema.TypeBytes
	case "json":
		return ytschema.TypeAny
	default:
		logger.Log.Debugf("Unknown type '%v' on inferring stage", dataType)
		return ytschema.TypeBytes
	}
}
