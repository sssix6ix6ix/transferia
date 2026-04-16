package greenplum

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/providers/postgres"
)

func TestSchemasFromIncludes(t *testing.T) {
	t.Run("wildcard pattern", func(t *testing.T) {
		src := &GpSource{IncludeTables: []string{"src.*"}}
		require.Equal(t, []string{"src"}, schemasFromIncludes(src))
	})

	t.Run("multiple schemas", func(t *testing.T) {
		src := &GpSource{IncludeTables: []string{"src.*", "dst.*"}}
		result := schemasFromIncludes(src)
		require.Len(t, result, 2)
		require.Contains(t, result, "src")
		require.Contains(t, result, "dst")
	})

	t.Run("deduplication", func(t *testing.T) {
		src := &GpSource{IncludeTables: []string{"src.table1", "src.table2"}}
		require.Equal(t, []string{"src"}, schemasFromIncludes(src))
	})

	t.Run("quoted schema", func(t *testing.T) {
		src := &GpSource{IncludeTables: []string{`"my-schema".*`}}
		require.Equal(t, []string{"my-schema"}, schemasFromIncludes(src))
	})

	t.Run("empty includes", func(t *testing.T) {
		src := &GpSource{IncludeTables: nil}
		require.Nil(t, schemasFromIncludes(src))
	})

	t.Run("sorted output", func(t *testing.T) {
		src := &GpSource{IncludeTables: []string{"z_schema.*", "a_schema.*"}}
		require.Equal(t, []string{"a_schema", "z_schema"}, schemasFromIncludes(src))
	})
}

func TestFilterOrdered(t *testing.T) {
	schema := string("SCHEMA")
	table := string(postgres.PgObjectTypeTable)
	function := string(postgres.PgObjectTypeFunction)
	typ := string(postgres.PgObjectTypeType)

	t.Run("preserves order from order list", func(t *testing.T) {
		enabled := []string{function, typ, schema}
		order := []postgres.PgObjectType{"SCHEMA", postgres.PgObjectTypeType, postgres.PgObjectTypeFunction}
		require.Equal(t, []string{schema, typ, function}, filterOrdered(enabled, order))
	})

	t.Run("ignores items not in order", func(t *testing.T) {
		enabled := []string{table, "UNKNOWN"}
		order := []postgres.PgObjectType{"SCHEMA", postgres.PgObjectTypeTable, postgres.PgObjectTypeFunction}
		require.Equal(t, []string{table}, filterOrdered(enabled, order))
	})

	t.Run("ignores order items not enabled", func(t *testing.T) {
		enabled := []string{table}
		order := []postgres.PgObjectType{"SCHEMA", postgres.PgObjectTypeTable, postgres.PgObjectTypeFunction}
		require.Equal(t, []string{table}, filterOrdered(enabled, order))
	})

	t.Run("empty enabled", func(t *testing.T) {
		result := filterOrdered(nil, []postgres.PgObjectType{"SCHEMA", postgres.PgObjectTypeTable})
		require.Empty(t, result)
	})

	t.Run("empty order", func(t *testing.T) {
		result := filterOrdered([]string{table}, nil)
		require.Empty(t, result)
	})
}
