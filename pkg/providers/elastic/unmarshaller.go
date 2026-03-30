package elastic

import (
	"bytes"
	"encoding/json"
	"slices"
	"strconv"
	"time"

	"github.com/spf13/cast"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/castx"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"github.com/transferia/transferia/pkg/util/strict"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const (
	epochSecond = "epoch_second"
)

func unmarshalField(value any, colSchema *abstract.ColSchema) (any, error) {
	if value == nil {
		return nil, nil
	}
	var result any
	var err error

	// in the switch below, the usage of `strict.Unexpected` indicates an unexpected or even impossible situation.
	// However, in order for Data Transfer to remain resilient, "unexpected" casts must exist
	switch ytschema.Type(colSchema.DataType) {
	case ytschema.TypeInt64:
		result, err = strict.Expected[json.Number](value, cast.ToInt64E)
	case ytschema.TypeInt32:
		result, err = strict.Expected[json.Number](value, cast.ToInt32E)
	case ytschema.TypeInt16:
		result, err = strict.Expected[json.Number](value, cast.ToInt16E)
	case ytschema.TypeInt8:
		result, err = strict.Expected[json.Number](value, cast.ToInt8E)
	case ytschema.TypeUint64:
		// We cannot use cast.ToUint64 because it uses ParseInt and not supports numbers greater than MaxInt64.
		caster := func(i any) (uint64, error) { return strconv.ParseUint(string(i.(json.Number)), 10, 64) }
		result, err = strict.Expected[json.Number](value, caster)
	case ytschema.TypeUint32:
		result, err = strict.Unexpected(value, cast.ToUint32E)
	case ytschema.TypeUint16:
		result, err = strict.Unexpected(value, cast.ToUint16E)
	case ytschema.TypeUint8:
		result, err = strict.Unexpected(value, cast.ToUint8E)
	case ytschema.TypeFloat32:
		result, err = strict.Expected[json.Number](value, cast.ToFloat32E)
	case ytschema.TypeFloat64:
		result, err = strict.Expected[json.Number](value, cast.ToFloat64E)
	case ytschema.TypeBytes:
		result, err = strict.Expected[*json.RawMessage](value, castx.ToByteSliceE)
	case ytschema.TypeBoolean:
		result, err = strict.Expected[*json.RawMessage](value, cast.ToBoolE)
	case ytschema.TypeDate:
		result, err = strict.Unexpected(value, cast.ToTimeE)
	case ytschema.TypeDatetime:
		result, err = strict.Unexpected(value, cast.ToTimeE)
	case ytschema.TypeTimestamp:
		result, err = handleTimestamp(value, colSchema)
	case ytschema.TypeInterval:
		result, err = strict.Unexpected(value, cast.ToDurationE)
	case ytschema.TypeString:
		result, err = strict.Expected[*json.RawMessage](value, castx.ToStringE)
	case ytschema.TypeAny:
		result, err = expectedAnyCast(value)
	default:
		return nil, abstract.NewFatalError(xerrors.Errorf(
			"unexpected target type %s (original type %q, value of type %T), unmarshalling is not implemented",
			colSchema.DataType, colSchema.OriginalType, value))
	}

	if err != nil {
		return nil, abstract.NewStrictifyError(colSchema, ytschema.Type(colSchema.DataType), err)
	}
	return result, nil
}

func handleTimestamp(value any, colSchema *abstract.ColSchema) (any, error) {
	// NOTE: Custom date formats are not fully supported by data transfer for now.
	// We can handle only:
	//	elasticsearch:date:
	//		epoch_millis – json.Number without any properties.
	//		epoch_seconds – json.Number with epoch_second format send as colSchema.Properties[fieldFormatSchemaKey].
	//
	//	elasticsearch:date_nanos:
	//		json.Number as milliseconds since the epoch according to
	//		https://www.elastic.co/guide/en/elasticsearch/reference/current/date_nanos.html.
	//
	// 	both elasticsearch:date and elasticsearch:date_nanos:
	//		strings containing formatted dates – !!only strings that could be parsed by cast.ToTimeE!!

	if _, isNumber := value.(json.Number); !isNumber {
		// TODO: Support custom date formats.
		// www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html#custom-date-formats
		result, err := strict.Expected[*json.RawMessage](value, cast.ToTimeE)
		if err != nil {
			return nil, xerrors.Errorf("unable to handle timestamp ('%v'): %w", value, err)
		}
		return result, nil
	}

	format, found := colSchema.Properties[fieldFormatSchemaKey]
	if found && slices.Contains(format.([]string), epochSecond) {
		// cast.ToTimeE handles json.Number as "seconds since 01.01.1970"
		result, err := strict.Expected[json.Number](value, cast.ToTimeE)
		if err != nil {
			return nil, xerrors.Errorf("unable to handle date '%v' in seconds: %w", value, err)
		}
		return result, nil
	}

	caster := func(value any) (time.Time, error) {
		asNumber, ok := value.(json.Number)
		if !ok {
			return time.Time{}, xerrors.Errorf("unable to convert '%v' of type '%T' to json.Number", value, value)
		}
		millis, err := asNumber.Int64()
		if err != nil {
			return time.Time{}, xerrors.Errorf("unable to cast json.Number ('%s') to int64: %w", asNumber.String(), err)
		}
		return time.UnixMilli(millis), nil
	}
	result, err := strict.Expected[json.Number](value, caster)
	if err != nil {
		return nil, xerrors.Errorf("unable to handle date '%v' in milliseconds: %w", value, err)
	}
	return result, nil
}

func expectedAnyCast(value any) (any, error) {
	var result any
	var err error

	switch v := value.(type) {
	case *json.RawMessage:
		result, err = unmarshalJSON(v)
	default:
		result, err = v, nil
	}

	if err != nil {
		return nil, xerrors.Errorf("failed to cast %T to any: %w", value, err)
	}
	resultJS, err := ensureJSONMarshallable(result)
	if err != nil {
		return nil, xerrors.Errorf(
			"successfully casted %T to any (%T), but the result is not JSON-serializable: %w", value, resultJS, err)
	}
	return resultJS, nil
}

func unmarshalJSON(v *json.RawMessage) (any, error) {
	result, err := jsonx.NewValueDecoder(jsonx.NewDefaultDecoder(bytes.NewReader(*v))).Decode()
	if err != nil {
		return nil, xerrors.Errorf("failed to decode a serialized JSON: %w", err)
	}
	return result, nil
}

func ensureJSONMarshallable(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	return castx.ToJSONMarshallableE(v)
}
