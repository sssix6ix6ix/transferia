package sample

import (
	_ "embed"
	"encoding/json"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/util"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

//go:embed data/user-activities.json
var userActivitiesJSONData []byte

type userActivitiesData struct {
	EventTypes     []string `json:"eventTypes"`
	DeviceTypes    []string `json:"deviceTypes"`
	DeviceOS       []string `json:"deviceOS"`
	TrafficSources []string `json:"trafficSources"`
}

var userActivitiesMarshaledData userActivitiesData

func init() {
	if err := json.Unmarshal(userActivitiesJSONData, &userActivitiesMarshaledData); err != nil {
		panic(err)
	}
}

const (
	sampleSourceCityNameColumn      = "city_name"
	sampleSourceDeviceOSColumn      = "device_os"
	sampleSourceCountryNameColumn   = "country_name"
	sampleSourceEventRevenueColumn  = "event_revenue"
	sampleSourceTrafficSourceColumn = "traffic_source"
	sampleSourceUserIDColumn        = "user_id"
	sampleSourceSessionIDColumn     = "session_id"
	sampleSourceEventDuration       = "event_duration"
)

var (
	_ StreamingData = (*UserActivities)(nil)

	userActivitiesColumnSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: sampleSourceEventID, DataType: ytschema.TypeString.String(), Required: true, PrimaryKey: true},
		{ColumnName: sampleSourceTimeColumn, DataType: ytschema.TypeTimestamp.String(), Required: true, PrimaryKey: true},
		{ColumnName: sampleSourceEventType, DataType: ytschema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceDeviceID, DataType: ytschema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceDeviceType, DataType: ytschema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceDeviceOSColumn, DataType: ytschema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceCountryNameColumn, DataType: ytschema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceCityNameColumn, DataType: ytschema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceEventRevenueColumn, DataType: ytschema.TypeFloat64.String(), Required: false},
		{ColumnName: sampleSourceTrafficSourceColumn, DataType: ytschema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceUserIDColumn, DataType: ytschema.TypeInt64.String(), Required: true},
		{ColumnName: sampleSourceSessionIDColumn, DataType: ytschema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceEventDuration, DataType: ytschema.TypeFloat64.String(), Required: true},
	})

	userActivitiesColumns = []string{
		sampleSourceEventID,
		sampleSourceTimeColumn,
		sampleSourceEventType,
		sampleSourceDeviceID,
		sampleSourceDeviceType,
		sampleSourceDeviceOSColumn,
		sampleSourceCountryNameColumn,
		sampleSourceCityNameColumn,
		sampleSourceEventRevenueColumn,
		sampleSourceTrafficSourceColumn,
		sampleSourceUserIDColumn,
		sampleSourceSessionIDColumn,
		sampleSourceEventDuration,
	}
)

type UserActivities struct {
	table         string
	eventID       string
	eventTime     time.Time
	eventType     string
	deviceID      string
	deviceType    string
	deviceOS      string
	countryName   string
	cityName      string
	eventRevenue  float64
	trafficSource string
	userID        int64
	sessionID     string
	eventDuration float64
}

func (u *UserActivities) TableName() abstract.TableID {
	return abstract.TableID{
		Namespace: "",
		Name:      u.table,
	}
}

func (u *UserActivities) ToChangeItem(offset int64) abstract.ChangeItem {
	return abstract.ChangeItem{
		ID:          0,
		LSN:         uint64(offset),
		CommitTime:  uint64(u.eventTime.UnixNano()),
		Counter:     0,
		Kind:        abstract.InsertKind,
		Schema:      "",
		Table:       u.table,
		PartID:      "",
		TableSchema: userActivitiesColumnSchema,
		ColumnNames: userActivitiesColumns,
		ColumnValues: []interface{}{
			u.eventID,
			u.eventTime,
			u.eventType,
			u.deviceID,
			u.deviceType,
			u.deviceOS,
			u.countryName,
			u.cityName,
			u.eventRevenue,
			u.trafficSource,
			u.userID,
			u.sessionID,
			u.eventDuration,
		},
		OldKeys: abstract.EmptyOldKeys(),
		// removing table string size because it is not added in column values
		Size:             abstract.RawEventSize(util.SizeOfStruct(*u) - uint64(len(u.table))),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}
}

func NewUserActivities(table string) *UserActivities {
	return &UserActivities{
		table:         table,
		eventID:       generateCustomID(generateCustomIDLength),
		eventTime:     time.Now(),
		eventType:     gofakeit.RandomString(userActivitiesMarshaledData.EventTypes),
		deviceID:      gofakeit.UUID(),
		deviceType:    gofakeit.RandomString(userActivitiesMarshaledData.DeviceTypes),
		deviceOS:      gofakeit.RandomString(userActivitiesMarshaledData.DeviceOS),
		countryName:   gofakeit.Country(),
		cityName:      gofakeit.City(),
		eventRevenue:  gofakeit.Price(0, 1000),
		trafficSource: gofakeit.RandomString(userActivitiesMarshaledData.TrafficSources),
		userID:        gofakeit.Int64(),
		sessionID:     gofakeit.MacAddress(),
		eventDuration: gofakeit.Float64Range(1, 300),
	}
}
