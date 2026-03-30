package replication

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/strict"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func UnmarshalHomo(value any, colSchema *abstract.ColSchema, _ *time.Location) (any, error) {
	if colSchema.DataType == string(ytschema.TypeAny) {
		return strict.Expected[string](value, castToAny)
	}
	return value, nil
}
