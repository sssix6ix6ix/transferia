package ydb

import (
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	debezium_common "github.com/transferia/transferia/pkg/debezium/common"
	"github.com/transferia/transferia/pkg/debezium/typeutil"
	"github.com/transferia/transferia/pkg/util/jsonx"
)

//---------------------------------------------------------------------------------------------------------------------
// ydb non-default converting

var KafkaTypeToOriginalTypeToFieldReceiverFunc = map[debezium_common.KafkaType]map[string]debezium_common.FieldReceiver{
	debezium_common.KafkaTypeInt8: {
		"ydb:Uint8": new(debezium_common.Int8ToUint8Default),
	},
	debezium_common.KafkaTypeInt16: {
		"ydb:Uint16": new(debezium_common.Int16ToUint16Default),
	},
	debezium_common.KafkaTypeInt32: {
		"ydb:Uint32": new(debezium_common.IntToUint32Default),
		"ydb:Date":   new(Date),
	},
	debezium_common.KafkaTypeInt64: {
		"ydb:Uint64":    new(debezium_common.Int64ToUint64Default),
		"ydb:Datetime":  new(Datetime),
		"ydb:Timestamp": new(Timestamp),
		"ydb:Interval":  new(Interval),
	},
	debezium_common.KafkaTypeFloat32: {
		"ydb:Float": new(debezium_common.Float64ToFloat32Default),
	},
	debezium_common.KafkaTypeString: {
		"ydb:Json":         new(JSON),
		"ydb:JsonDocument": new(JSON),
	},
}

type Date struct {
	debezium_common.Int64ToTime
	debezium_common.YTTypeDate
	debezium_common.FieldReceiverMarker
}

func (i *Date) Do(in int64, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (time.Time, error) {
	return typeutil.TimeFromDate(in), nil
}

type Datetime struct {
	debezium_common.Int64ToTime
	debezium_common.YTTypeDateTime
	debezium_common.FieldReceiverMarker
}

func (i *Datetime) Do(in int64, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (time.Time, error) {
	return typeutil.TimeFromDatetime(in), nil
}

type Timestamp struct {
	debezium_common.Int64ToTime
	debezium_common.YTTypeTimestamp
	debezium_common.FieldReceiverMarker
}

func (i *Timestamp) Do(in int64, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (time.Time, error) {
	return typeutil.TimeFromTimestamp(in), nil
}

type Interval struct {
	debezium_common.DurationToInt64
	debezium_common.YTTypeInterval
	debezium_common.FieldReceiverMarker
}

func (i *Interval) Do(in time.Duration, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (int64, error) {
	return int64(in), nil
}

type JSON struct {
	debezium_common.StringToAny
	debezium_common.YTTypeAny
	debezium_common.FieldReceiverMarker
}

func (i *JSON) Do(in string, _ *debezium_common.OriginalTypeInfo, _ *debezium_common.Schema, _ bool) (interface{}, error) {
	var result interface{}
	if err := jsonx.NewDefaultDecoder(strings.NewReader(in)).Decode(&result); err != nil {
		return "", xerrors.Errorf("unable to unmarshal json - err: %w", err)
	}
	return result, nil
}
