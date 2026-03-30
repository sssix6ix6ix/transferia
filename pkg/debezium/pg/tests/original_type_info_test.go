package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/debezium"
	debezium_common "github.com/transferia/transferia/pkg/debezium/common"
	debezium_parameters "github.com/transferia/transferia/pkg/debezium/parameters"
	debezium_pg "github.com/transferia/transferia/pkg/debezium/pg"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
)

func TestOriginalType(t *testing.T) {
	originalTypeProperties := debezium_pg.GetOriginalTypeProperties(&abstract.ColSchema{
		Properties: map[abstract.PropertyKey]interface{}{provider_postgres.DatabaseTimeZone: "Europe/Moscow"},
	})

	originalTypeInfo := &debezium_common.OriginalTypeInfo{
		OriginalType: "pg:timestamp without time zone",
		Properties:   originalTypeProperties,
	}

	timestampReceiverDescr := debezium_pg.TimestampWithoutTimeZone{}
	currTime, err := timestampReceiverDescr.Do(1136214245999999, originalTypeInfo, nil, false) // int: 1136214245999999, GMT: Monday, 2 January 2006 г., 15:04:05.999
	require.NoError(t, err)

	require.Equal(t, "Europe/Moscow", currTime.Location().String())
}

func TestPgTimestampWithoutTimezone(t *testing.T) {
	initTimeStr := "2006-01-02T15:04:05.999999000+03:00"
	tz, _ := time.LoadLocation("Europe/Moscow")
	// we need function time.ParseInLocation to set 'Europe/Moscow' explicitly. time.Parse set 'Local', and it's bad for comparison with recovered time.Time
	initTimeTime, err := time.ParseInLocation(time.RFC3339Nano, initTimeStr, tz)
	require.NoError(t, err)

	changeItem := &abstract.ChangeItem{
		Kind:         abstract.InsertKind,
		ColumnNames:  []string{"id", "val"},
		ColumnValues: []interface{}{1, initTimeTime},
		TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "id", OriginalType: "pg:integer"},
			{ColumnName: "val", OriginalType: "pg:timestamp without time zone", Properties: map[abstract.PropertyKey]interface{}{provider_postgres.DatabaseTimeZone: "Europe/Moscow"}},
		}),
	}

	// prepare engines

	paramsWithOriginalTypes := debezium_parameters.EnrichedWithDefaults(map[string]string{
		debezium_parameters.DatabaseDBName:   "public",
		debezium_parameters.TopicPrefix:      "my_topic",
		debezium_parameters.SourceType:       "pg",
		debezium_parameters.AddOriginalTypes: "true",
	})
	emitterWithOriginalTypes, err := debezium.NewMessagesEmitter(paramsWithOriginalTypes, "", false, logger.Log)
	require.NoError(t, err)
	paramsWithoutOriginalTypes := debezium_parameters.EnrichedWithDefaults(map[string]string{
		debezium_parameters.DatabaseDBName:   "public",
		debezium_parameters.TopicPrefix:      "my_topic",
		debezium_parameters.SourceType:       "pg",
		debezium_parameters.AddOriginalTypes: "false",
	})
	emitterWithoutOriginalTypes, err := debezium.NewMessagesEmitter(paramsWithoutOriginalTypes, "", false, logger.Log)
	require.NoError(t, err)

	receiver := debezium.NewReceiver(nil, nil)

	// chain

	currDebeziumKVWithOriginalTypes, err := emitterWithOriginalTypes.EmitKV(changeItem, time.Time{}, true, nil)
	require.NoError(t, err)
	currDebeziumKVWithoutOriginalTypes, err := emitterWithoutOriginalTypes.EmitKV(changeItem, time.Time{}, true, nil)
	require.NoError(t, err)

	recoveredChangeItemWith, err := receiver.Receive(*currDebeziumKVWithOriginalTypes[0].DebeziumVal)
	require.NoError(t, err)
	recoveredChangeItemWithout, err := receiver.Receive(*currDebeziumKVWithoutOriginalTypes[0].DebeziumVal)
	require.NoError(t, err)

	// check

	v0 := recoveredChangeItemWith.AsMap()["val"] // time.Time: "2006-01-02T15:04:05.999999000+03:00"
	fmt.Println(v0)
	require.Equal(t, initTimeTime, v0)

	v1 := recoveredChangeItemWithout.AsMap()["val"] // int: 1136214245999999, GMT: Monday, 2 January 2006 г., 15:04:05.999
	require.Equal(t, int64(1136214245999999), v1)
}
