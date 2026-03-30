package transformer_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/sink_factory"
	"github.com/transferia/transferia/pkg/transformer"
	transformer_filter "github.com/transferia/transferia/pkg/transformer/registry/filter"
	transformer_replace_primary_key "github.com/transferia/transferia/pkg/transformer/registry/replace_primary_key"
)

type mockSinker struct {
	gotItems []abstract.ChangeItem
}

func (m *mockSinker) Close() error { return nil }
func (m *mockSinker) Push(items []abstract.ChangeItem) error {
	m.gotItems = append(m.gotItems, items...)
	return nil
}

func TestMultipleTransformers(t *testing.T) {
	tableName := "test_table"
	trans := &model.Transformation{
		Transformers: &transformer.Transformers{
			DebugMode: true,
			Transformers: []transformer.Transformer{
				{
					transformer_replace_primary_key.Type: transformer_replace_primary_key.Config{
						Keys: []string{
							"field2",
							"field1",
						},
						Tables: transformer_filter.Tables{
							IncludeTables: []string{tableName},
						},
					},
				},
				{
					transformer_filter.FilterColumnsTransformerType: transformer_filter.FilterColumnsConfig{
						Tables: transformer_filter.Tables{
							IncludeTables: []string{tableName},
						},
						Columns: transformer_filter.Columns{
							IncludeColumns: []string{
								"field2",
								"field1",
								"field4",
							},
						},
					},
				},
			},
			ErrorsOutput: nil,
		},
		ExtraTransformers: nil,
	}

	mockSinker := new(mockSinker)
	transfer := &model.Transfer{
		Src: &model.MockSource{},
		Dst: &model.MockDestination{
			SinkerFactory: func() abstract.Sinker { return mockSinker },
			Cleanup:       model.Drop,
		},
		Transformation: trans,
	}
	asink, err := sink_factory.MakeAsyncSink(
		transfer,
		&model.TransferOperation{},
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		coordinator.NewFakeClient(),
		middlewares.MakeConfig(middlewares.WithNoData),
	)
	require.NoError(t, err)
	var data = []abstract.ChangeItem{
		abstract.ChangeItemFromMap(nil, nil, tableName, string(abstract.InitTableLoad)),
		abstract.ChangeItemFromMap(map[string]interface{}{
			"field1": "test",
			"field2": 2,
			"field3": 1.23,
			"field4": "{}",
		}, abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "field1", PrimaryKey: true},
			{ColumnName: "field2", PrimaryKey: true},
			{ColumnName: "field3", PrimaryKey: true},
			{ColumnName: "field4", PrimaryKey: true},
		}), tableName, string(abstract.InsertKind)),
	}

	require.NoError(t, <-asink.AsyncPush(data))
	require.NoError(t, asink.Close())
	require.Equal(t, len(mockSinker.gotItems), 2)
	require.Equal(t, mockSinker.gotItems[1].Kind, abstract.InsertKind)
	require.Equal(t, mockSinker.gotItems[1].TableSchema,
		abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "field2", PrimaryKey: true},
			{ColumnName: "field1", PrimaryKey: true},
			{ColumnName: "field4", PrimaryKey: false},
		}), tableName, string(abstract.InsertKind))
	require.Equal(t, mockSinker.gotItems[1].ColumnValues, []interface{}{"test", 2, "{}"})

}
