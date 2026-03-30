package abstract

import (
	"fmt"
	"time"

	"github.com/transferia/transferia/pkg/abstract/changeitem"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func MakeInitTableLoad(pos LogPosition, table TableDescription, commitTime time.Time, tableSchema *TableSchema) []ChangeItem {
	return []ChangeItem{{
		ID:           pos.ID,
		TxID:         pos.TxID,
		LSN:          pos.LSN,
		CommitTime:   uint64(commitTime.UnixNano()),
		Table:        table.Name,
		Schema:       table.Schema,
		PartID:       table.GeneratePartID(),
		Kind:         InitTableLoad,
		TableSchema:  tableSchema,
		ColumnNames:  []string{},
		ColumnValues: []interface{}{},
	}}
}

func MakeDoneTableLoad(pos LogPosition, table TableDescription, commitTime time.Time, tableSchema *TableSchema) []ChangeItem {
	return []ChangeItem{{
		ID:           pos.ID,
		TxID:         pos.TxID,
		LSN:          pos.LSN,
		CommitTime:   uint64(commitTime.UnixNano()),
		Table:        table.Name,
		Schema:       table.Schema,
		PartID:       table.GeneratePartID(),
		Kind:         DoneTableLoad,
		TableSchema:  tableSchema,
		ColumnNames:  []string{},
		ColumnValues: []interface{}{},
	}}
}

func MakeTxDone(txSequence uint32, lsn uint64, execTS time.Time, lastPushedGTID, gtidStr string) ChangeItem {
	return ChangeItem{
		ID:               txSequence,
		LSN:              lsn,
		CommitTime:       uint64(execTS.UnixNano()),
		Counter:          0,
		Kind:             DDLKind,
		Schema:           "",
		Table:            "",
		PartID:           "",
		ColumnNames:      []string{"query"},
		ColumnValues:     []interface{}{fmt.Sprintf("-- transaction %v done", lastPushedGTID)},
		TableSchema:      NewTableSchema([]ColSchema{{}}),
		OldKeys:          *new(OldKeysType),
		Size:             RawEventSize(0),
		TxID:             gtidStr,
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}
}

func MakeSynchronizeEvent() ChangeItem {
	return ChangeItem{
		ID:               0,
		LSN:              0,
		CommitTime:       uint64(time.Now().UnixNano()),
		Counter:          0,
		Kind:             SynchronizeKind,
		Schema:           "",
		Table:            "",
		PartID:           "",
		ColumnNames:      nil,
		ColumnValues:     []interface{}{},
		TableSchema:      nil,
		OldKeys:          EmptyOldKeys(),
		Size:             EmptyEventSize(),
		TxID:             "",
		Query:            "",
		QueueMessageMeta: changeitem.QueueMessageMeta{TopicName: "", PartitionNum: 0, Offset: 0, Index: 0},
	}
}

//---

// DefaultValue returns a default instance of the type represented by this schema. This method only works safely in heterogenous transfers.
func DefaultValue(c *changeitem.ColSchema) interface{} {
	switch ytschema.Type(c.DataType) {
	case ytschema.TypeInt64, ytschema.TypeInt32, ytschema.TypeInt16, ytschema.TypeInt8, ytschema.TypeUint64, ytschema.TypeUint32, ytschema.TypeUint16, ytschema.TypeUint8:
		return Restore(*c, float64(0))
	case ytschema.TypeFloat32:
		return float32(0)
	case ytschema.TypeFloat64:
		return float64(0)
	case ytschema.TypeBytes, ytschema.TypeString:
		return ""
	case ytschema.TypeBoolean:
		return false
	case ytschema.TypeAny:
		return Restore(*c, "{}")
	case ytschema.TypeDate, ytschema.TypeDatetime, ytschema.TypeTimestamp:
		return time.Unix(0, 0)
	case ytschema.TypeInterval:
		return time.Duration(0)
	default:
		return nil
	}
}
