package snapshot

import (
	"database/sql"
	"strconv"

	"github.com/spf13/cast"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	mysql_unmarshaller_types "github.com/transferia/transferia/pkg/providers/mysql/unmarshaller/types"
	"github.com/transferia/transferia/pkg/util/castx"
	"github.com/transferia/transferia/pkg/util/strict"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func unmarshalHetero(value interface{}, colSchema *abstract.ColSchema) (any, error) {
	if value == nil {
		return nil, nil
	}

	var result any
	var err error

	// in the switch below, the usage of `UnexpectedSQL` indicates an unexpected or even impossible situation.
	// However, in order for Data Transfer to remain resilient, "unexpected" casts must exist
	switch ytschema.Type(colSchema.DataType) {
	case ytschema.TypeInt64:
		result, err = strict.ExpectedSQL[*sql.NullInt64](value, cast.ToInt64E)
	case ytschema.TypeInt32:
		result, err = strict.UnexpectedSQL(value, cast.ToInt32E)
	case ytschema.TypeInt16:
		result, err = strict.ExpectedSQL[*sql.NullInt64](value, cast.ToInt16E)
	case ytschema.TypeInt8:
		result, err = strict.ExpectedSQL[*sql.NullInt64](value, cast.ToInt8E)
	case ytschema.TypeUint64:
		result, err = strict.ExpectedSQL[*mysql_unmarshaller_types.NullUint64](value, cast.ToUint64E)
	case ytschema.TypeUint32:
		result, err = strict.UnexpectedSQL(value, cast.ToUint32E)
	case ytschema.TypeUint16:
		result, err = strict.ExpectedSQL[*sql.NullInt64](value, cast.ToUint16E)
	case ytschema.TypeUint8:
		result, err = strict.ExpectedSQL[*sql.NullInt64](value, cast.ToUint8E)
	case ytschema.TypeFloat32:
		result, err = strict.UnexpectedSQL(value, cast.ToFloat32E)
	case ytschema.TypeFloat64:
		switch v := value.(type) {
		case *sql.NullFloat64:
			result, err = strict.ExpectedSQL[*sql.NullFloat64](v, castx.ToJSONNumberE)
		case *sql.NullString:
			result, err = strict.ExpectedSQL[*sql.NullString](v, castx.ToJSONNumberE)
		default:
			result, err = strict.UnexpectedSQL(v, castx.ToJSONNumberE)
		}
	case ytschema.TypeBytes:
		switch v := value.(type) {
		case *[]byte:
			result, err = strict.Expected[[]byte](unwrapBytes(v), castx.ToByteSliceE)
		default:
			result, err = strict.UnexpectedSQL(v, castx.ToByteSliceE)
		}
	case ytschema.TypeBoolean:
		result, err = strict.UnexpectedSQL(value, cast.ToBoolE)
	case ytschema.TypeDate:
		result, err = strict.ExpectedSQL[*mysql_unmarshaller_types.Temporal](value, cast.ToTimeE)
	case ytschema.TypeDatetime:
		result, err = strict.UnexpectedSQL(value, cast.ToTimeE)
	case ytschema.TypeTimestamp:
		result, err = strict.ExpectedSQL[*mysql_unmarshaller_types.Temporal](value, cast.ToTimeE)
	case ytschema.TypeInterval:
		result, err = strict.UnexpectedSQL(value, cast.ToDurationE)
	case ytschema.TypeString:
		switch v := value.(type) {
		case *sql.NullString:
			result, err = strict.ExpectedSQL[*sql.NullString](v, castx.ToStringE)
		case *[]byte:
			result, err = strict.Expected[[]byte](unwrapBytes(v), castx.ToStringE)
		case *sql.NullInt64:
			result, err = unmarshalInt64AsString(v)
		default:
			result, err = strict.UnexpectedSQL(v, castx.ToStringE)
		}
	case ytschema.TypeAny:
		result, err = strict.ExpectedSQL[*mysql_unmarshaller_types.JSON](value, castx.ToJSONMarshallableE[any])
	default:
		return nil, abstract.NewFatalError(xerrors.Errorf("unexpected target type %s (original type %q, value of type %T), unmarshalling is not implemented", colSchema.DataType, colSchema.OriginalType, value))
	}

	if err != nil {
		return nil, abstract.NewStrictifyError(colSchema, ytschema.Type(colSchema.DataType), err)
	}
	return result, nil
}

func unwrapBytes(v *[]byte) any {
	if v == nil || *v == nil {
		return nil
	}
	// https://st.yandex-team.ru/TM-6428 copying bytes here is REQUIRED
	result := make([]byte, len(*v))
	copy(result, *v)
	return result
}

func unmarshalInt64AsString(v *sql.NullInt64) (any, error) {
	extractionResult, err := strict.ExpectedSQL[*sql.NullInt64](v, cast.ToInt64E)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse int64 from NullInt64: %w", err)
	}
	if extractionResult == nil {
		return nil, nil
	}
	i64, ok := extractionResult.(int64)
	if !ok {
		return nil, xerrors.Errorf("ToInt64E returned a value of type %T", extractionResult)
	}
	return strconv.FormatInt(i64, 10), nil
}
