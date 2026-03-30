package debezium

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	debezium_common "github.com/transferia/transferia/pkg/debezium/common"
	debezium_mysql "github.com/transferia/transferia/pkg/debezium/mysql"
	debezium_pg "github.com/transferia/transferia/pkg/debezium/pg"
	debezium_ydb "github.com/transferia/transferia/pkg/debezium/ydb"
)

var prefixToNotDefaultReceiver map[string]debezium_common.NotDefaultReceiverDescription

func init() {
	// init this map into init() to avoid 'initialization loop'
	prefixToNotDefaultReceiver = map[string]debezium_common.NotDefaultReceiverDescription{
		"pg:":    debezium_pg.KafkaTypeToOriginalTypeToFieldReceiverFunc,
		"ydb:":   debezium_ydb.KafkaTypeToOriginalTypeToFieldReceiverFunc,
		"mysql:": debezium_mysql.KafkaTypeToOriginalTypeToFieldReceiverFunc,
	}
}

func handleFieldReceiverMatchers(fieldReceiver debezium_common.FieldReceiver, originalType *debezium_common.OriginalTypeInfo, schema *debezium_common.Schema) debezium_common.FieldReceiver {
	switch t := fieldReceiver.(type) {
	case *debezium_common.FieldReceiverMatchers:
		checkFieldReceiverArr := t.Matchers
		for _, matcher := range checkFieldReceiverArr {
			if matcher.IsMatched(originalType, schema) {
				return matcher
			}
		}
		return nil
	default:
		return fieldReceiver
	}
}

func findFieldReceiver(in debezium_common.NotDefaultReceiverDescription, originalType *debezium_common.OriginalTypeInfo, schema *debezium_common.Schema) debezium_common.FieldReceiver {
	if in == nil {
		return nil
	}
	originalTypeToFieldReceiverFunc, ok := in[debezium_common.KafkaType(schema.Type)]
	if !ok {
		return nil
	}
	if _, ok := originalTypeToFieldReceiverFunc[originalType.OriginalType]; ok { // match by full OriginalType
		return originalTypeToFieldReceiverFunc[originalType.OriginalType]
	} else {
		bracketIndex := strings.Index(originalType.OriginalType, "(")
		if bracketIndex != -1 {
			originalTypeUnparametrized := originalType.OriginalType[0:bracketIndex] // match by unparametrized OriginalType
			if _, ok := originalTypeToFieldReceiverFunc[originalTypeUnparametrized]; ok {
				return originalTypeToFieldReceiverFunc[originalTypeUnparametrized]
			}
		}

		if fieldReceiver, ok := originalTypeToFieldReceiverFunc[debezium_common.DTMatchByFunc]; ok { // match by matcher
			return handleFieldReceiverMatchers(fieldReceiver, originalType, schema)
		}
		return nil
	}
}

func getDatabaseSpecificReceiver(originalType string) (debezium_common.NotDefaultReceiverDescription, error) {
	if originalType == "" {
		return nil, nil
	}

	prefixIndex := strings.Index(originalType, ":")
	if prefixIndex == -1 {
		return nil, xerrors.Errorf("unable to extract prefix, str: %s", originalType)
	}
	prefix := originalType[0 : prefixIndex+1]

	return prefixToNotDefaultReceiver[prefix], nil
}

func receiveFieldChecked(ytType string, inSchemaDescr *debezium_common.Schema, val interface{}, originalType *debezium_common.OriginalTypeInfo) (interface{}, bool, error) {
	val, isAbsentVal, err := receiveField(inSchemaDescr, val, originalType, false)
	if err != nil {
		return nil, false, xerrors.Errorf("unable to receive value, field: %s, err: %w", inSchemaDescr.Field, err)
	}
	if isAbsentVal {
		return nil, true, nil
	}
	inType := debezium_common.KafkaType(inSchemaDescr.Type)
	resultTypes, ok := debezium_common.KafkaTypeToResultYTTypes[inType]
	if !ok {
		return nil, false, xerrors.Errorf("unknown kafka_type, field: %s", inSchemaDescr.Type)
	}
	assertValidateRetType(ytType, resultTypes.ResultType, resultTypes.AlternativeResultType, inSchemaDescr.Type)
	return val, false, nil
}

func receiveFieldColSchema(inSchemaDescr *debezium_common.Schema, originalType *debezium_common.OriginalTypeInfo) (*abstract.ColSchema, error) {
	colSchema := &abstract.ColSchema{
		TableSchema:  "",
		TableName:    "",
		Path:         "", // don't know what is it
		ColumnName:   inSchemaDescr.Field,
		DataType:     "", // will be filled in this function further
		PrimaryKey:   !inSchemaDescr.Optional,
		FakeKey:      false,                     // don't know what is it
		Required:     false,                     // don't know where is true - bcs even when PrimaryKey:true, Required:false
		Expression:   "",                        // for sure is empty - bcs we don't have such information
		OriginalType: originalType.OriginalType, // for sure is empty - bcs we don't have OriginalType info
		Properties:   nil,
	}
	if debezium_common.KafkaType(inSchemaDescr.Type) == debezium_common.KafkaTypeArray {
		colSchema.DataType = "any"
	} else {
		receiverDescription, err := getDatabaseSpecificReceiver(originalType.OriginalType)
		if err != nil {
			return nil, xerrors.Errorf("unable to get database-specific receiver")
		}
		fieldReceiverFunc := findFieldReceiver(receiverDescription, originalType, inSchemaDescr)
		if fieldReceiverFunc == nil {
			fieldReceiverFunc = debezium_common.TypeToDefault[debezium_common.KafkaType(inSchemaDescr.Type)]
			if fieldReceiverFunc != nil {
				fieldReceiverFunc = handleFieldReceiverMatchers(fieldReceiverFunc, originalType, inSchemaDescr)
			}
		}
		if fieldReceiverFunc == nil {
			return nil, xerrors.Errorf("unable to find field receiver - even default, for kafka type: %s", inSchemaDescr.Type)
		}

		if additionalInfo, ok := fieldReceiverFunc.(debezium_common.ContainsColSchemaAdditionalInfo); ok {
			additionalInfo.AddInfo(inSchemaDescr, colSchema)
		}
		colSchema.DataType = fieldReceiverFunc.YTType()
	}
	return colSchema, nil
}

func receiveField(inSchemaDescr *debezium_common.Schema, val interface{}, originalType *debezium_common.OriginalTypeInfo, intoArr bool) (interface{}, bool, error) {
	if val == nil {
		return nil, false, nil
	}
	if valStr, ok := val.(string); ok {
		if valStr == "__debezium_unavailable_value" {
			return nil, true, nil
		}
	}

	if debezium_common.KafkaType(inSchemaDescr.Type) == debezium_common.KafkaTypeArray {
		outVal, err := arrayReceive(inSchemaDescr, val, originalType, intoArr)
		if err != nil {
			return nil, false, xerrors.Errorf("unable to receive data from array")
		}
		return outVal, false, nil
	}

	// unpack values

	receiverDescription, err := getDatabaseSpecificReceiver(originalType.OriginalType)
	if err != nil {
		return "", false, xerrors.Errorf("unable to get database-specific receiver")
	}
	fieldReceiverFunc := findFieldReceiver(receiverDescription, originalType, inSchemaDescr)
	if fieldReceiverFunc == nil {
		fieldReceiverFunc = debezium_common.TypeToDefault[debezium_common.KafkaType(inSchemaDescr.Type)]
		if fieldReceiverFunc != nil {
			fieldReceiverFunc = handleFieldReceiverMatchers(fieldReceiverFunc, originalType, inSchemaDescr)
		}
	}
	if fieldReceiverFunc == nil {
		return nil, false, xerrors.Errorf("unable to find field receiver - even default, for kafka type: %s", inSchemaDescr.Type)
	}

	var valInt64 *int64
	var valBoolean *bool
	var valString *string
	var valFloat64 *float64
	err = extractVal(fieldReceiverFunc, val, &valInt64, &valBoolean, &valString, &valFloat64)
	if err != nil {
		return nil, false, xerrors.Errorf("unable to extract value, err: %w", err)
	}

	// assert

	if valInt64 == nil && valBoolean == nil && valString == nil && valFloat64 == nil {
		switch fieldReceiverFunc.(type) {
		case debezium_common.StructToFloat64, debezium_common.StructToString, debezium_common.AnyToDouble, debezium_common.AnyToAny:
		default:
			inSchemaDescrStr, _ := json.Marshal(inSchemaDescr)
			valStr, _ := json.Marshal(val)
			originalTypeStr, _ := json.Marshal(originalType)
			return nil, false, xerrors.Errorf("assert no one value extracted, inSchemaDescr:%s, val:%s, originalType:%s", inSchemaDescrStr, valStr, originalTypeStr)
		}
	}

	// convert values

	result, err := convertVal(fieldReceiverFunc, inSchemaDescr, val, originalType, intoArr, valInt64, valBoolean, valString, valFloat64)
	return result, false, err
}

func extractVal(
	receiver debezium_common.FieldReceiver,
	val interface{},
	valInt64 **int64,
	valBoolean **bool,
	valString **string,
	valFloat64 **float64,
) error {
	switch receiver.(type) {
	case debezium_common.Int8ToInt8,
		debezium_common.Int8ToUint8,
		debezium_common.Int16ToInt16,
		debezium_common.IntToInt32,
		debezium_common.IntToUint16,
		debezium_common.IntToUint32,
		debezium_common.IntToString,
		debezium_common.Int64ToInt64,
		debezium_common.Int64ToTime,
		debezium_common.DurationToInt64,
		debezium_common.Int64ToUint64: // uint64 is stored in int64
		switch v := val.(type) {
		case json.Number:
			valInt64Tmp, err := v.Int64()
			if err != nil {
				return xerrors.Errorf("unable to convert json.Number into Int64(), val: %s, err: %w", v.String(), err)
			}
			*valInt64 = &valInt64Tmp
		}
	case debezium_common.BooleanToBoolean,
		debezium_common.BooleanToInt8,
		debezium_common.BooleanToBytes,
		debezium_common.BooleanToString:
		switch v := val.(type) {
		case bool:
			*valBoolean = &v
		}
	case debezium_common.StringToString,
		debezium_common.StringToTime,
		debezium_common.StringToAny,
		debezium_common.StringToBytes:
		switch v := val.(type) {
		case string:
			*valString = &v
		case json.Number:
			valStr := v.String()
			*valString = &valStr
		}
	case debezium_common.Float64ToFloat32,
		debezium_common.Float64ToFloat64:
		switch v := val.(type) {
		case json.Number:
			valFloat64Tmp, err := v.Float64()
			if err != nil {
				return xerrors.Errorf("unable to convert json.Number into Float64(), val: %s, err: %w", v.String(), err)
			}
			*valFloat64 = &valFloat64Tmp
		}
	case debezium_common.StructToFloat64:
	case debezium_common.StructToString:
	case debezium_common.AnyToDouble:
	case debezium_common.AnyToAny:
	default:
		return xerrors.Errorf("unknown receiver type: %T", receiver)
	}
	return nil
}

func convertVal(
	receiver debezium_common.FieldReceiver,
	inSchemaDescr *debezium_common.Schema,
	val interface{},
	originalType *debezium_common.OriginalTypeInfo,
	intoArr bool,
	valInt64 *int64,
	valBoolean *bool,
	valString *string,
	valFloat64 *float64,
) (interface{}, error) {
	var result interface{}
	var err error

	switch fieldReceiverObj := receiver.(type) {
	case debezium_common.Int8ToInt8:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debezium_common.Int8ToUint8:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debezium_common.Int16ToInt16:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debezium_common.IntToInt32:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debezium_common.IntToUint32:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debezium_common.IntToUint16:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debezium_common.IntToString:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debezium_common.Int64ToInt64:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debezium_common.Int64ToUint64:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debezium_common.DurationToInt64:
		result, err = fieldReceiverObj.Do(time.Duration(*valInt64), originalType, inSchemaDescr, intoArr)
	case debezium_common.Int64ToTime:
		result, err = fieldReceiverObj.Do(*valInt64, originalType, inSchemaDescr, intoArr)
	case debezium_common.BooleanToBoolean:
		result, err = fieldReceiverObj.Do(*valBoolean, originalType, inSchemaDescr, intoArr)
	case debezium_common.BooleanToInt8:
		result, err = fieldReceiverObj.Do(*valBoolean, originalType, inSchemaDescr, intoArr)
	case debezium_common.BooleanToBytes:
		result, err = fieldReceiverObj.Do(*valBoolean, originalType, inSchemaDescr, intoArr)
	case debezium_common.BooleanToString:
		result, err = fieldReceiverObj.Do(*valBoolean, originalType, inSchemaDescr, intoArr)
	case debezium_common.StringToString:
		result, err = fieldReceiverObj.Do(*valString, originalType, inSchemaDescr, intoArr)
	case debezium_common.StringToTime:
		result, err = fieldReceiverObj.Do(*valString, originalType, inSchemaDescr, intoArr)
	case debezium_common.StringToBytes:
		result, err = fieldReceiverObj.Do(*valString, originalType, inSchemaDescr, intoArr)
	case debezium_common.StringToAny:
		result, err = fieldReceiverObj.Do(*valString, originalType, inSchemaDescr, intoArr)
	case debezium_common.Float64ToFloat32:
		result, err = fieldReceiverObj.Do(*valFloat64, originalType, inSchemaDescr, intoArr)
	case debezium_common.Float64ToFloat64:
		result, err = fieldReceiverObj.Do(*valFloat64, originalType, inSchemaDescr, intoArr)
	case debezium_common.StructToFloat64:
		result, err = fieldReceiverObj.Do(val, originalType, inSchemaDescr, intoArr)
	case debezium_common.StructToString:
		result, err = fieldReceiverObj.Do(val, originalType, inSchemaDescr, intoArr)
	case debezium_common.AnyToDouble:
		result, err = fieldReceiverObj.Do(val, originalType, inSchemaDescr, intoArr)
	case debezium_common.AnyToAny:
		result, err = fieldReceiverObj.Do(val, originalType, inSchemaDescr, intoArr)
	default:
		return nil, xerrors.Errorf("unknown receiver type: %T", receiver)
	}
	if err != nil {
		return nil, xerrors.Errorf("unable to receive field, err: %w", err)
	}
	return result, nil
}

func arrayReceive(s *debezium_common.Schema, v interface{}, originalType *debezium_common.OriginalTypeInfo, _ bool) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	resultVal := make([]interface{}, 0)
	vArr := v.([]interface{})
	items := debezium_common.Schema(*s.Items)
	for _, el := range vArr {
		elVal, isAbsent, err := receiveField(&items, el, originalType.GetArrElemTypeDescr(), true)
		if err != nil {
			return nil, xerrors.Errorf("unable to receive array's element, err: %w", err)
		}
		if isAbsent {
			return nil, xerrors.Errorf("array can't contains absent values")
		}
		resultVal = append(resultVal, elVal)
	}
	return resultVal, nil
}

func assertValidateRetType(retType string, resultType string, alternativeResultType []string, inType string) {
	if retType == resultType {
		return
	}
	for _, el := range alternativeResultType {
		if retType == el {
			return
		}
	}
	panic(fmt.Sprintf("%s type converted to %s, what is undocumented", inType, retType))
}
