package sample

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

// Marker on TableDescription.Filter for per-worker shard loads (Offset is 1-based worker index).
const sampleShardFilterMarker abstract.WhereStatement = "sample_storage_shard_offset_is_worker_index"

func sampleShardRowRange(totalRows int64, shardIndex, shardCount int) (start, end int64) {
	if shardCount <= 0 || totalRows <= 0 || shardIndex < 0 || shardIndex >= shardCount {
		return 0, 0
	}
	base := totalRows / int64(shardCount)
	rem := totalRows % int64(shardCount)

	var startAcc int64
	for i := 0; i < shardIndex; i++ {
		sz := base
		if int64(i) < rem {
			sz++
		}
		startAcc += sz
	}
	sz := base
	if int64(shardIndex) < rem {
		sz++
	}
	endRow := startAcc + sz
	if endRow > totalRows {
		endRow = totalRows
	}
	return startAcc, endRow
}

// ShardTable splits the logical table into one part per snapshot worker when destination supports sharded snapshots.
func (s *Storage) ShardTable(ctx context.Context, table abstract.TableDescription) ([]abstract.TableDescription, error) {
	_ = ctx
	if table.Filter != "" || table.Offset != 0 {
		return []abstract.TableDescription{table}, nil
	}
	if s.partsCount <= 1 {
		return []abstract.TableDescription{table}, nil
	}
	result := make([]abstract.TableDescription, s.partsCount)
	total := s.SnapshotEventCount
	for i := 0; i < s.partsCount; i++ {
		start, end := sampleShardRowRange(total, i, s.partsCount)
		eta := uint64(end - start)
		result[i] = abstract.TableDescription{
			Name:   table.Name,
			Schema: table.Schema,
			Filter: sampleShardFilterMarker,
			Offset: uint64(i + 1),
			EtaRow: eta,
		}
	}
	return result, nil
}

func (s *Storage) loadTableSharded(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	shardIdx := int(table.Offset) - 1
	if shardIdx < 0 || shardIdx >= s.partsCount {
		return xerrors.Errorf("invalid shard table.Offset %d for partsCount %d", table.Offset, s.partsCount)
	}
	start, end := sampleShardRowRange(s.SnapshotEventCount, shardIdx, s.partsCount)
	shardTotal := end - start
	var rowsCount int64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.ctx.Done():
			s.logger.Info("Stopping run")
			return nil
		default:
		}

		sampleData := s.MaxSampleData
		if rowsCount+sampleData > shardTotal {
			sampleData = shardTotal - rowsCount
		}
		if sampleData <= 0 {
			s.logger.Infof("Reached shard target of %d events (shard %d)", shardTotal, shardIdx)
			break
		}
		baseOffset := start + rowsCount
		data := generateRandomDataForSampleType(s.SampleType, sampleData, s.TableName, s.metrics, baseOffset)
		stampSampleRowsPartID(data, table)
		rowsCount += sampleData
		s.metrics.ChangeItems.Add(int64(len(data)))
		if err := pusher(data); err != nil {
			s.logger.Errorf("unable to push %v rows to %v: %v", sampleData, s.TableName, err)
			return xerrors.Errorf("unable to push last %d rows to table %s: %w", sampleData, s.TableName, err)
		}
		if rowsCount >= shardTotal {
			s.logger.Infof("Reached shard target of %d events (shard %d)", shardTotal, shardIdx)
			break
		}
		s.logger.Infof("Current row count in shard is %v", rowsCount)
	}
	return nil
}
