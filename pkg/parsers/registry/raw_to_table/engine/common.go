package engine

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/parsers/registry/raw_to_table/raw_to_table_model"
)

const defaultDLQSuffix = "_dlq"

type CommonConfig struct {
	TableName string // by default (if empty string), tableName = topicName

	IsKeyEnabled bool
	KeyType      raw_to_table_model.DataType

	ValueType raw_to_table_model.DataType

	IsTimestampEnabled bool
	IsHeadersEnabled   bool

	// Suffix for the DLQ table name: final name is baseTable + normalized suffix
	// Empty uses "_dlq"
	DLQSuffix string
}

func (c *CommonConfig) Validate() error {
	if c.IsKeyEnabled {
		switch c.KeyType {
		case raw_to_table_model.String, raw_to_table_model.Bytes, raw_to_table_model.JSON:
		default:
			return xerrors.Errorf("invalid parser config - unsupported key type: %s", c.KeyType.String())
		}
	}

	switch c.ValueType {
	case raw_to_table_model.String, raw_to_table_model.Bytes, raw_to_table_model.JSON:
	default:
		return xerrors.Errorf("invalid parser config - unsupported value type: %s", c.ValueType.String())
	}

	return nil
}
