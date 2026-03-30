package mysql

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	debezium_common "github.com/transferia/transferia/pkg/debezium/common"
	"github.com/transferia/transferia/pkg/debezium/typeutil"
	"github.com/transferia/transferia/pkg/util/jsonx"
)

//---------------------------------------------------------------------------------------------------------------------
// mysql non-default converting

var KafkaTypeToOriginalTypeToFieldReceiverFunc = map[debezium_common.KafkaType]map[string]debezium_common.FieldReceiver{
	debezium_common.KafkaTypeBoolean: {
		"mysql:tinyint(1)": new(TinyInt1),
		"mysql:bit(1)":     new(Bit1),
	},
	debezium_common.KafkaTypeInt16: {
		"mysql:smallint": new(debezium_common.Int16ToInt16Default),
		debezium_common.DTMatchByFunc: &debezium_common.FieldReceiverMatchers{
			Matchers: []debezium_common.FieldReceiverMatcher{new(TinyIntSigned), new(TinyIntUnsigned)},
		},
	},
	debezium_common.KafkaTypeInt32: {
		"mysql:date": new(Date),
		"mysql:year": new(debezium_common.IntToStringDefault),
		debezium_common.DTMatchByFunc: &debezium_common.FieldReceiverMatchers{
			Matchers: []debezium_common.FieldReceiverMatcher{new(SmallIntSigned), new(SmallIntUnsigned)},
		},
	},
	debezium_common.KafkaTypeInt64: {
		"mysql:time":     new(Time),
		"mysql:datetime": new(Datetime),
		debezium_common.DTMatchByFunc: &debezium_common.FieldReceiverMatchers{
			Matchers: []debezium_common.FieldReceiverMatcher{new(IntSigned), new(IntUnsigned), new(Int64Unsigned)},
		},
	},
	debezium_common.KafkaTypeString: {
		"mysql:timestamp": new(Timestamp),
		"mysql:json":      new(JSON),
	},
	debezium_common.KafkaTypeBytes: {
		"mysql:decimal": new(Decimal),
		debezium_common.DTMatchByFunc: &debezium_common.FieldReceiverMatchers{
			Matchers: []debezium_common.FieldReceiverMatcher{new(DebeziumBuf)},
		},
	},
}

//---------------------------------------------------------------------------------------------------------------------
// bool

type TinyInt1 struct {
	debezium_common.BooleanToInt8
	debezium_common.YTTypeBoolean
	debezium_common.FieldReceiverMarker
}

func (d *TinyInt1) Do(in bool, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (int8, error) {
	if in {
		return 1, nil
	} else {
		return 0, nil
	}
}

type Bit1 struct {
	debezium_common.BooleanToBytes
	debezium_common.YTTypeBytes
	debezium_common.FieldReceiverMarker
}

func (d *Bit1) Do(in bool, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) ([]byte, error) {
	if in {
		return []byte{0, 0, 0, 0, 0, 0, 0, 1}, nil
	} else {
		return []byte{0, 0, 0, 0, 0, 0, 0, 0}, nil
	}
}

// int16

type TinyIntUnsigned struct {
	debezium_common.Int16ToUint8
	debezium_common.YTTypeUint8
	debezium_common.FieldReceiverMarker
}

func (d *TinyIntUnsigned) IsMatched(originalTypeInfo *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema) bool {
	return strings.HasPrefix(originalTypeInfo.OriginalType, "mysql:tinyint") && strings.HasSuffix(originalTypeInfo.OriginalType, " unsigned")
}

func (d *TinyIntUnsigned) Do(in int64, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (uint8, error) {
	return uint8(in), nil
}

type TinyIntSigned struct {
	debezium_common.Int16ToInt8
	debezium_common.YTTypeInt8
	debezium_common.FieldReceiverMarker
}

func (d *TinyIntSigned) IsMatched(originalTypeInfo *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema) bool {
	return strings.HasPrefix(originalTypeInfo.OriginalType, "mysql:tinyint") && (!strings.HasSuffix(originalTypeInfo.OriginalType, " unsigned"))
}

func (d *TinyIntSigned) Do(in int64, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (int8, error) {
	return int8(in), nil
}

// int32

type SmallIntUnsigned struct {
	debezium_common.Int16ToUint16
	debezium_common.YTTypeUint16
	debezium_common.FieldReceiverMarker
}

func (d *SmallIntUnsigned) IsMatched(originalTypeInfo *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema) bool {
	return strings.HasPrefix(originalTypeInfo.OriginalType, "mysql:smallint") && strings.HasSuffix(originalTypeInfo.OriginalType, " unsigned")
}

func (d *SmallIntUnsigned) Do(in int64, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (uint16, error) {
	return uint16(in), nil
}

type SmallIntSigned struct {
	debezium_common.Int16ToInt16
	debezium_common.YTTypeInt16
	debezium_common.FieldReceiverMarker
}

func (d *SmallIntSigned) IsMatched(originalTypeInfo *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema) bool {
	return strings.HasPrefix(originalTypeInfo.OriginalType, "mysql:smallint") && (!strings.HasSuffix(originalTypeInfo.OriginalType, " unsigned"))
}

func (d *SmallIntSigned) Do(in int64, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (int16, error) {
	return int16(in), nil
}

type Date struct {
	debezium_common.Int64ToTime
	debezium_common.YTTypeString
	debezium_common.FieldReceiverMarker
}

func (d *Date) Do(in int64, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (time.Time, error) {
	return time.Unix(in*86400, 0).UTC(), nil
}

type IntUnsigned struct {
	debezium_common.Int64ToUint32
	debezium_common.YTTypeUint32
	debezium_common.FieldReceiverMarker
}

func (d *IntUnsigned) IsMatched(originalTypeInfo *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema) bool {
	return strings.HasPrefix(originalTypeInfo.OriginalType, "mysql:int") && strings.HasSuffix(originalTypeInfo.OriginalType, " unsigned")
}

func (d *IntUnsigned) Do(in int64, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (uint32, error) {
	return uint32(in), nil
}

type IntSigned struct {
	debezium_common.Int64ToInt32
	debezium_common.YTTypeInt32
	debezium_common.FieldReceiverMarker
}

func (d *IntSigned) IsMatched(originalTypeInfo *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema) bool {
	return strings.HasPrefix(originalTypeInfo.OriginalType, "mysql:int") && (!strings.HasSuffix(originalTypeInfo.OriginalType, " unsigned"))
}

func (d *IntSigned) Do(in int64, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (int32, error) {
	return int32(in), nil
}

// int64

type Int64Unsigned struct {
	debezium_common.Int64ToUint64
	debezium_common.YTTypeUint64
	debezium_common.FieldReceiverMarker
}

func (d *Int64Unsigned) IsMatched(originalTypeInfo *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema) bool {
	return strings.HasPrefix(originalTypeInfo.OriginalType, "mysql:bigint") && strings.HasSuffix(originalTypeInfo.OriginalType, " unsigned")
}

func (d *Int64Unsigned) Do(in int64, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (uint64, error) {
	return uint64(in), nil
}

type Time struct {
	debezium_common.IntToString
	debezium_common.YTTypeString
	debezium_common.FieldReceiverMarker
}

func (d *Time) Do(in int64, originalTypeInfo *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (string, error) {
	precision, err := typeutil.ExtractParameter(originalTypeInfo.OriginalType)
	if err != nil {
		precision = 0
	}
	hh := in / (3600 * 1000000)
	mm := (in % (3600 * 1000000)) / (60 * 1000000)
	ss := (in % (60 * 1000000)) / 1000000
	fraction := in % 1000000

	result := fmt.Sprintf(`%02d:%02d:%02d`, hh, mm, ss)
	return result + typeutil.MakeFractionSecondSuffix(fraction, precision), nil
}

type Datetime struct {
	debezium_common.Int64ToTime
	debezium_common.YTTypeString
	debezium_common.FieldReceiverMarker
}

func (d *Datetime) Do(in int64, originalType *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (time.Time, error) {
	datetimePrecisionInt := typeutil.GetTimePrecision(originalType.OriginalType)
	if datetimePrecisionInt == -1 {
		datetimePrecisionInt = 0
	}
	var result time.Time
	if datetimePrecisionInt >= 4 && datetimePrecisionInt <= 6 {
		result = time.Unix(in/1000000, (in%1000000)*1000).UTC()
	} else {
		result = time.Unix(in/1000, (in%1000)*1000000).UTC()
	}
	return result, nil
}

// string

type Timestamp struct {
	debezium_common.StringToTime
	debezium_common.YTTypeTimestamp
	debezium_common.FieldReceiverMarker
}

func (d *Timestamp) Do(in string, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (time.Time, error) {
	return typeutil.ParseTimestamp(in)
}

type JSON struct {
	debezium_common.StringToAny
	debezium_common.YTTypeAny
	debezium_common.FieldReceiverMarker
}

func (j *JSON) Do(in string, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (interface{}, error) {
	var result interface{}
	if err := jsonx.NewDefaultDecoder(strings.NewReader(in)).Decode(&result); err != nil {
		return "", err
	}
	return result, nil
}

// bytes

type DebeziumBuf struct {
	debezium_common.StringToBytes
	debezium_common.YTTypeBytes
	debezium_common.FieldReceiverMarker
}

func (b *DebeziumBuf) IsMatched(_ *debezium_common.OriginalTypeInfo, schema *debezium_common.Schema) bool {
	return schema.Name == "io.debezium.data.Bits"
}

func (b *DebeziumBuf) Do(in string, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) ([]byte, error) {
	resultBuf, err := base64.StdEncoding.DecodeString(in)
	if err != nil {
		return nil, xerrors.Errorf("unable to decode base64: %s, err: %w", in, err)
	}
	return typeutil.ReverseBytesArr(resultBuf), nil
}

type Decimal struct {
	debezium_common.AnyToDouble
	debezium_common.YTTypeFloat64
	debezium_common.FieldReceiverMarker
}

func (b *Decimal) Do(in interface{}, originalTypeInfo *debezium_common.OriginalTypeInfo, schema *debezium_common.Schema, _ bool) (json.Number, error) {
	decimal := new(debezium_common.Decimal)
	resultStr, err := decimal.Do(in.(string), originalTypeInfo, schema, false)
	if err != nil {
		return "", xerrors.Errorf("unable to receive decimal, err: %w", err)
	}
	return json.Number(resultStr), nil
}
