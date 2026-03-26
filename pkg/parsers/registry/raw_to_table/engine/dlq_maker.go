package engine

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/raw_to_table/raw_to_table_model"
)

// DLQMaker produces rows for the dead-letter table. That table intentionally uses a fixed,
// wider layout than the main sink: it always has timestamp, headers, key, and value columns
// (key and value as raw bytes), plus failure_reason. This preserves the full original
// message for inspection and does not mirror the user’s main-table toggles or typed key/value
// settings.
type DLQMaker struct {
	tableName   string
	dlqSuffix   string
	tableSchema *abstract.TableSchema
	columnNames []string
}

func (m *DLQMaker) BuildDLQChangeItem(msg parsers.Message, partition abstract.Partition, failureReasonValue string) abstract.ChangeItem {
	baseTable := m.tableName
	if baseTable == "" {
		baseTable = partition.Topic
	}
	tableName := baseTable + m.dlqSuffix
	columnValues := buildColumnValues(msg, partition, true, raw_to_table_model.Bytes, raw_to_table_model.Bytes, true, true, m.columnNames)
	columnValues = append(columnValues, failureReasonValue)
	result := buildChangeItem(
		tableName,
		msg,
		partition,
		m.columnNames,
		columnValues,
		m.tableSchema,
	)
	return result
}

func NewDLQMaker(tableName string, dlqSuffix string) *DLQMaker {
	currDlqSuffix := dlqSuffix
	if currDlqSuffix == "" {
		currDlqSuffix = defaultDLQSuffix
	}
	dlqConfig := &CommonConfig{
		TableName:          tableName,
		IsKeyEnabled:       true,
		KeyType:            raw_to_table_model.Bytes,
		ValueType:          raw_to_table_model.Bytes,
		IsTimestampEnabled: true,
		IsHeadersEnabled:   true,
		DLQSuffix:          currDlqSuffix,
	}
	tableSchema, columnNames := buildTableSchemaAndColumnNames(dlqConfig, true)

	return &DLQMaker{
		tableName:   tableName,
		dlqSuffix:   currDlqSuffix,
		tableSchema: tableSchema,
		columnNames: columnNames,
	}
}
