package snapshot

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/mysql/unmarshaller/types"
)

func NewValueReceiver(k *sql.ColumnType, originalTypeName string, location *time.Location) any {
	switch k.DatabaseTypeName() {
	case "BIGINT", "UNSIGNED BIGINT":
		if strings.HasSuffix(originalTypeName, "unsigned") || strings.HasSuffix(originalTypeName, "zerofill") {
			return new(types.NullUint64)
		}
	case "JSON":
		return new(types.JSON)
	case "DATE", "DATETIME":
		return types.NewTemporal()
	case "TIMESTAMP":
		return types.NewTemporalInLocation(location)
	}
	return reflect.New(k.ScanType()).Interface()
}

func UnmarshalHetero(receivers []any, table []abstract.ColSchema) ([]any, error) {
	return unmarshal(receivers, table, unmarshalHetero)
}

func UnmarshalHomo(receivers []any, table []abstract.ColSchema) ([]any, error) {
	return unmarshal(receivers, table, unmarshalHomo)
}

func unmarshal(receivers []any, table []abstract.ColSchema, unmarshalFieldFunc func(any, *abstract.ColSchema) (any, error)) ([]any, error) {
	result := make([]any, len(receivers))
	var fieldErrs []error
	for i := range receivers {
		unmarshallingResult, err := unmarshalFieldFunc(receivers[i], &table[i])
		if err != nil {
			fieldErrs = append(fieldErrs, xerrors.Errorf("column [%d] %q: %w", i, table[i].ColumnName, err))
			continue
		}
		result[i] = unmarshallingResult
	}
	if err := errors.Join(fieldErrs...); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal: %w", err)
	}
	return result, nil
}
