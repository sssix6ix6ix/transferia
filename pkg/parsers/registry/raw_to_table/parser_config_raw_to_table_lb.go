package raw_to_table

import (
	"github.com/transferia/transferia/pkg/parsers/registry/raw_to_table/engine"
	"github.com/transferia/transferia/pkg/parsers/registry/raw_to_table/raw_to_table_model"
)

// ParserConfigRawToTableLb is the variant without a key column on the main sink table.
//
// DLQ table layout is fixed and includes key as bytes anyway; see package doc.
type ParserConfigRawToTableLb struct {
	TableName string // by default (if empty string), tableName = topicName

	ValueType raw_to_table_model.DataType

	IsTimestampEnabled bool
	IsHeadersEnabled   bool

	// Suffix for the DLQ table name (base + suffix). Empty → "_dlq"; a value without a leading '_' gets one prepended.
	// Rows in that table always include timestamp, headers, key/value as bytes, and failure_reason, independent of the flags above.
	DLQSuffix string
}

func (c *ParserConfigRawToTableLb) IsNewParserConfig() {}

func (c *ParserConfigRawToTableLb) IsAppendOnly() bool {
	return true
}

func (c *ParserConfigRawToTableLb) toCommonConfig() *engine.CommonConfig {
	return &engine.CommonConfig{
		TableName: c.TableName,

		ValueType: c.ValueType,

		IsKeyEnabled: false,
		KeyType:      "",

		IsTimestampEnabled: c.IsTimestampEnabled,
		IsHeadersEnabled:   c.IsHeadersEnabled,

		DLQSuffix: c.DLQSuffix,
	}
}

func (c *ParserConfigRawToTableLb) Validate() error {
	config := c.toCommonConfig()
	return config.Validate()
}
