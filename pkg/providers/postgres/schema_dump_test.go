package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilterItemsByType(t *testing.T) {
	items := []*pgDumpItem{
		{Typ: "SCHEMA", Name: "public"},
		{Typ: string(PgObjectTypeTable), Name: "users"},
		{Typ: string(PgObjectTypeSequence), Name: "users_id_seq"},
		{Typ: string(PgObjectTypeFunction), Name: "my_func"},
		{Typ: string(PgObjectTypeView), Name: "active_users"},
		{Typ: string(PgObjectTypeTable), Name: "orders"},
	}

	t.Run("single type", func(t *testing.T) {
		result := filterItemsByType(items, []string{string(PgObjectTypeTable)})
		require.Len(t, result, 2)
		require.Equal(t, "users", result[0].Name)
		require.Equal(t, "orders", result[1].Name)
	})

	t.Run("multiple types", func(t *testing.T) {
		result := filterItemsByType(items, []string{string(PgObjectTypeSequence), string(PgObjectTypeFunction)})
		require.Len(t, result, 2)
		require.Equal(t, "users_id_seq", result[0].Name)
		require.Equal(t, "my_func", result[1].Name)
	})

	t.Run("no matching types", func(t *testing.T) {
		result := filterItemsByType(items, []string{string(PgObjectTypeTrigger)})
		require.Empty(t, result)
	})

	t.Run("empty types list", func(t *testing.T) {
		result := filterItemsByType(items, []string{})
		require.Empty(t, result)
	})

	t.Run("empty items list", func(t *testing.T) {
		result := filterItemsByType(nil, []string{string(PgObjectTypeTable)})
		require.Empty(t, result)
	})

	t.Run("preserves order", func(t *testing.T) {
		result := filterItemsByType(items, []string{"SCHEMA", string(PgObjectTypeView), string(PgObjectTypeTable)})
		require.Len(t, result, 4)
		require.Equal(t, "SCHEMA", result[0].Typ)
		require.Equal(t, string(PgObjectTypeTable), result[1].Typ)
		require.Equal(t, string(PgObjectTypeView), result[2].Typ)
		require.Equal(t, string(PgObjectTypeTable), result[3].Typ)
	})
}

func TestSchemaDumpIsEmpty(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		d := &SchemaDump{items: nil}
		require.True(t, d.IsEmpty())
	})

	t.Run("empty slice", func(t *testing.T) {
		d := &SchemaDump{items: []*pgDumpItem{}}
		require.True(t, d.IsEmpty())
	})

	t.Run("non-empty", func(t *testing.T) {
		d := &SchemaDump{items: []*pgDumpItem{{Typ: string(PgObjectTypeTable), Name: "t"}}}
		require.False(t, d.IsEmpty())
	})
}
