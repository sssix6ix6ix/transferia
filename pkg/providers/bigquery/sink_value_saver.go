package bigquery

import (
	"encoding/json"
	"fmt"

	google_bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/spf13/cast"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var _ google_bigquery.ValueSaver = (*ChangeItem)(nil)

type ChangeItem struct {
	abstract.ChangeItem
}

func (c ChangeItem) Save() (row map[string]google_bigquery.Value, insertID string, err error) {
	res := map[string]google_bigquery.Value{}
	for i, val := range c.ColumnValues {
		res[c.ColumnNames[i]], err = typeFit(
			c.TableSchema.FastColumns()[abstract.ColumnName(c.ColumnNames[i])],
			val,
		)
		if err != nil {
			return nil, "", xerrors.Errorf("unable to type-fit: %w", err)
		}
	}
	return res, fmt.Sprintf("%s/%v/%s", c.Table, c.LSN, c.TxID), nil
}

func typeFit(col abstract.ColSchema, val any) (google_bigquery.Value, error) {
	switch ytschema.Type(col.DataType) {
	case ytschema.TypeAny:
		jsonData, err := json.Marshal(val)
		if err != nil {
			return nil, xerrors.Errorf("unable to marshal data: %w", err)
		}
		return jsonData, nil
	case ytschema.TypeDate:
		return civil.DateOf(cast.ToTime(val)), nil
	case ytschema.TypeTimestamp:
		return val, nil
	case ytschema.TypeDatetime:
		return civil.DateTimeOf(cast.ToTime(val)), nil
	case ytschema.TypeUint8:
		return cast.ToInt8(val), nil
	case ytschema.TypeUint16:
		return cast.ToInt16(val), nil
	case ytschema.TypeUint32:
		return cast.ToInt32(val), nil
	case ytschema.TypeUint64:
		return cast.ToInt64(val), nil
	default:
		return val, nil
	}
}
