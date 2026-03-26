package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/parsers/registry/raw_to_table/raw_to_table_model"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestBuildTableSchemaAndColumnNames_JSONKey(t *testing.T) {
	cfg := &CommonConfig{
		IsKeyEnabled: true,
		KeyType:      raw_to_table_model.JSON,
		ValueType:    raw_to_table_model.Bytes,
	}

	tableSchema, columnNames := buildTableSchemaAndColumnNames(cfg, false)
	require.Contains(t, columnNames, ColNameKey)

	found := false
	for _, column := range tableSchema.Columns() {
		if column.ColumnName == ColNameKey {
			found = true
			require.Equal(t, ytschema.TypeAny.String(), column.DataType)
		}
	}
	require.True(t, found)
}
