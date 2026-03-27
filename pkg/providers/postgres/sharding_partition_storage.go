package postgres

import (
	"context"
	"fmt"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

func (s *Storage) getChildTables(ctx context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	conn, err := s.Conn.Acquire(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to acquire a connection from pool: %w", err)
	}
	defer conn.Release()
	rows, err := conn.Query(ctx, `
SELECT
    c.oid::regclass::text as table_name,
    c.reltuples as eta_rows
FROM pg_class c
     INNER JOIN pg_inherits par on c.oid = par.inhrelid
WHERE
    par.inhparent :: regclass = $1 :: regclass;;
`, table.Fqtn())
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve child tables: %w", err)
	}
	var res []abstract.TableDescription
	for rows.Next() {
		var tableName string
		var etaRow uint64
		if err := rows.Scan(&tableName, &etaRow); err != nil {
			return nil, xerrors.Errorf("unable to scan row: %w", err)
		}
		tid, _ := abstract.NewTableIDFromStringPg(tableName, false)
		res = append(res, abstract.TableDescription{
			Name:   table.Name,
			Schema: table.Schema,
			Filter: abstract.WhereStatement(fmt.Sprintf("%s%s|%s", PartitionsFilterPrefix, tid.Fqtn(), table.Filter)),
			EtaRow: etaRow,
			Offset: 0,
		})
	}

	if len(res) > 0 {
		// Include parent table itself as shard (that reads with ONLY semantics). Needed when parent has its own data.
		parentEtaRow := uint64(0)
		err := conn.QueryRow(ctx, `
SELECT GREATEST(reltuples, 0)::bigint
FROM pg_class
WHERE oid = $1::regclass`,
			table.Fqtn()).Scan(&parentEtaRow)
		if err != nil {
			logger.Log.Warn("unable to estimate parent table rows count", log.Error(err))
		}
		res = append(res, abstract.TableDescription{
			Name:   table.Name,
			Schema: table.Schema,
			Filter: abstract.WhereStatement(fmt.Sprintf("%s%s|%s", ParentOnlyFilterPrefix, table.Fqtn(), string(table.Filter))),
			EtaRow: parentEtaRow,
			Offset: 0,
		})
	}
	return res, nil
}
