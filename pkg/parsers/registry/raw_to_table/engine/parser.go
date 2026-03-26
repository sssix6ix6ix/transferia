package engine

import (
	"encoding/json"
	"strings"
	"unicode/utf8"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/raw_to_table/raw_to_table_model"
)

type RawToTableImpl struct {
	cfg         *CommonConfig
	dlq         *DLQMaker
	tableSchema *abstract.TableSchema
	columnNames []string
}

func (p *RawToTableImpl) sendToDLQReason(msg parsers.Message) string {
	var parts []string

	check := func(wantType raw_to_table_model.DataType, bytes []byte, who string) {
		switch wantType {
		case raw_to_table_model.String:
			if !utf8.Valid(bytes) {
				parts = append(parts, who+" is configured as string, but it isn't valid UTF-8")
			}
		case raw_to_table_model.JSON:
			// TODO - check, can our source-queue build msg where 'key' or 'value' is not nil but empty byte array
			if bytes != nil && !json.Valid(bytes) {
				parts = append(parts, who+" is configured as JSON, but it isn't valid JSON")
			}
		}
	}

	if p.cfg.IsKeyEnabled {
		check(p.cfg.KeyType, msg.Key, "key")
	}
	check(p.cfg.ValueType, msg.Value, "value")

	return strings.Join(parts, "|")
}

func (p *RawToTableImpl) Do(msg parsers.Message, partition abstract.Partition) []abstract.ChangeItem {
	reasonsWhyDLQ := p.sendToDLQReason(msg)
	if reasonsWhyDLQ != "" { // if we should send it to DLQ
		return []abstract.ChangeItem{p.dlq.BuildDLQChangeItem(msg, partition, reasonsWhyDLQ)}
	}
	changeItem := buildChangeItem(
		p.cfg.TableName,
		msg,
		partition,
		p.columnNames,
		buildColumnValues(msg, partition, p.cfg.IsKeyEnabled, p.cfg.KeyType, p.cfg.ValueType, p.cfg.IsTimestampEnabled, p.cfg.IsHeadersEnabled, p.columnNames),
		p.tableSchema,
	)
	return []abstract.ChangeItem{changeItem}
}

func (p *RawToTableImpl) DoBatch(batch parsers.MessageBatch) []abstract.ChangeItem {
	result := make([]abstract.ChangeItem, 0, len(batch.Messages))
	for _, msg := range batch.Messages {
		result = append(result, p.Do(msg, abstract.Partition{Partition: batch.Partition, Topic: batch.Topic})...)
	}
	return result
}

func NewRawToTable(
	cfg *CommonConfig,
) (*RawToTableImpl, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	tableSchema, columnNames := buildTableSchemaAndColumnNames(cfg, false)

	return &RawToTableImpl{
		cfg:         cfg,
		dlq:         NewDLQMaker(cfg.TableName, cfg.DLQSuffix),
		tableSchema: tableSchema,
		columnNames: columnNames,
	}, nil
}
