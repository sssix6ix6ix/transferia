package bigquery

import (
	google_bigquery "cloud.google.com/go/bigquery"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.TargetRule(ProviderType, map[ytschema.Type]string{
		ytschema.TypeInt64:     string(google_bigquery.BigNumericFieldType),
		ytschema.TypeInt32:     string(google_bigquery.IntegerFieldType),
		ytschema.TypeInt16:     string(google_bigquery.IntegerFieldType),
		ytschema.TypeInt8:      string(google_bigquery.IntegerFieldType),
		ytschema.TypeUint64:    string(google_bigquery.BigNumericFieldType),
		ytschema.TypeUint32:    string(google_bigquery.IntegerFieldType),
		ytschema.TypeUint16:    string(google_bigquery.IntegerFieldType),
		ytschema.TypeUint8:     string(google_bigquery.IntegerFieldType),
		ytschema.TypeFloat32:   string(google_bigquery.FloatFieldType),
		ytschema.TypeFloat64:   string(google_bigquery.FloatFieldType),
		ytschema.TypeBytes:     string(google_bigquery.BytesFieldType),
		ytschema.TypeString:    string(google_bigquery.StringTargetType),
		ytschema.TypeBoolean:   string(google_bigquery.BooleanFieldType),
		ytschema.TypeAny:       string(google_bigquery.JSONFieldType),
		ytschema.TypeDate:      string(google_bigquery.DateFieldType),
		ytschema.TypeDatetime:  string(google_bigquery.DateTimeFieldType),
		ytschema.TypeTimestamp: string(google_bigquery.TimestampFieldType),
	})
}
