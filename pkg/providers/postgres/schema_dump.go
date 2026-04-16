package postgres

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
)

// SchemaDump holds parsed pg_dump output and provides methods to apply
// schema objects on a destination database. The underlying pgDumpItem
// type is intentionally kept private.
type SchemaDump struct {
	items []*pgDumpItem
}

// NewSchemaDump executes pg_dump with the given arguments and parses the output.
func NewSchemaDump(pgDumpCmd []string, connString string, password model.SecretString, args []string) (*SchemaDump, error) {
	items, err := execPgDump(pgDumpCmd, connString, password, args)
	if err != nil {
		return nil, err
	}
	return &SchemaDump{items: items}, nil
}

// IsEmpty returns true if no schema objects were extracted.
func (d *SchemaDump) IsEmpty() bool {
	return len(d.items) == 0
}

// ApplyDDL filters items by the given object types and executes their DDL
// bodies on the destination pool. Tolerates "already exists" errors for
// idempotency.
func (d *SchemaDump) ApplyDDL(ctx context.Context, lgr log.Logger, pool *pgxpool.Pool, types []string) error {
	filtered := filterItemsByType(d.items, types)
	if len(filtered) == 0 {
		return nil
	}
	lgr.Info("Applying DDL on destination", log.Int("items", len(filtered)))
	for _, item := range filtered {
		lgr.Info("Applying DDL", log.String("type", item.Typ), log.String("schema", item.Schema), log.String("name", item.Name))
		if _, err := pool.Exec(ctx, item.Body); err != nil {
			if isAlreadyExistsError(err) {
				lgr.Warn("Object already exists, skipping",
					log.String("type", item.Typ), log.String("name", item.Name), log.Error(err))
				continue
			}
			return xerrors.Errorf("failed to apply DDL (type=%s, name=%s.%s): %w", item.Typ, item.Schema, item.Name, err)
		}
	}
	return nil
}

// SyncSequenceValues reads current sequence values from srcPool and sets them on dstPool for all SEQUENCE items.
func (d *SchemaDump) SyncSequenceValues(ctx context.Context, lgr log.Logger, srcPool, dstPool *pgxpool.Pool) error {
	seqItems := filterItemsByType(d.items, []string{string(PgObjectTypeSequence)})
	if len(seqItems) == 0 {
		return nil
	}

	srcConn, err := srcPool.Acquire(ctx)
	if err != nil {
		return xerrors.Errorf("failed to acquire source connection: %w", err)
	}
	defer srcConn.Release()

	lgr.Info("Synchronizing sequence values", log.Int("sequences", len(seqItems)))
	for _, item := range seqItems {
		seqID := abstract.TableID{Namespace: item.Schema, Name: item.Name}
		lastValue, isCalled, err := GetCurrentStateOfSequence(ctx, srcConn.Conn(), seqID)
		if err != nil {
			return xerrors.Errorf("failed to get current state of sequence %s: %w", seqID.Fqtn(), err)
		}
		if err := SetCurrentStateOfSequence(ctx, dstPool, seqID, lastValue, isCalled); err != nil {
			return xerrors.Errorf("failed to set value of sequence %s: %w", seqID.Fqtn(), err)
		}
		lgr.Info("Sequence value synchronized", log.String("sequence", seqID.Fqtn()), log.Int64("last_value", lastValue))
	}
	return nil
}

func filterItemsByType(items []*pgDumpItem, types []string) []*pgDumpItem {
	allowed := set.New(types...)
	var result []*pgDumpItem
	for _, item := range items {
		if allowed.Contains(item.Typ) {
			result = append(result, item)
		}
	}
	return result
}
