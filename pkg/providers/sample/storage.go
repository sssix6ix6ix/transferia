package sample

import (
	"context"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	_ abstract.Storage         = (*Storage)(nil)
	_ abstract.ShardingStorage = (*Storage)(nil)
)

type Storage struct {
	ctx                context.Context
	metrics            *stats.SourceStats
	cancel             func()
	SampleType         string
	SnapshotEventCount int64
	TableName          string
	MaxSampleData      int64
	MinSleepTime       time.Duration
	logger             log.Logger
	workersCount       int
	partsCount         int
}

func (s *Storage) Close() {
	s.cancel()
}

func (s *Storage) Ping() error {
	return nil
}

func (s *Storage) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	if s.workersCount > 1 && table.Offset > 0 &&
		strings.HasPrefix(string(table.Filter), string(sampleShardFilterMarker)) {
		return s.loadTableSharded(ctx, table, pusher)
	}
	rowsCount := int64(0)
	s.logger.Infof("The total row number is %v", s.SnapshotEventCount)

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("Stopping run")
			return nil
		default:
		}

		sampleData := s.MaxSampleData
		if rowsCount+sampleData > s.SnapshotEventCount {
			sampleData = s.SnapshotEventCount - rowsCount
		}
		if sampleData <= 0 {
			break
		}
		data := generateRandomDataForSampleType(s.SampleType, sampleData, s.TableName, s.metrics, rowsCount)
		stampSampleRowsPartID(data, table)
		rowsCount += sampleData
		s.metrics.ChangeItems.Add(int64(len(data)))
		if err := pusher(data); err != nil {
			s.logger.Errorf("unable to push %v rows to %v: %v", sampleData, s.TableName, err)
			return xerrors.Errorf("unable to push last %d rows to table %s: %w", sampleData, s.TableName, err)
		}
		if rowsCount >= s.SnapshotEventCount {
			s.logger.Infof("Reached the target of %d events", s.SnapshotEventCount)
			break
		}
		s.logger.Infof("Current row count is %v", rowsCount)
	}

	return nil
}

func (s *Storage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	if s.SampleType == userActivitiesSampleType {
		return userActivitiesColumnSchema, nil
	}
	return iotDataColumnSchema, nil
}

func (s *Storage) TableList(filter abstract.IncludeTableList) (abstract.TableMap, error) {
	tables := make(abstract.TableMap)
	tableID := abstract.TableID{Namespace: "", Name: s.TableName}

	if s.SampleType == userActivitiesSampleType {
		tables[tableID] = abstract.TableInfo{
			EtaRow: uint64(s.SnapshotEventCount),
			IsView: false,
			Schema: userActivitiesColumnSchema,
		}
		return tables, nil
	}

	tables[tableID] = abstract.TableInfo{
		EtaRow: uint64(s.SnapshotEventCount),
		IsView: false,
		Schema: iotDataColumnSchema,
	}

	return tables, nil
}

func (s *Storage) ExactTableRowsCount(_ abstract.TableID) (uint64, error) {
	return uint64(s.SnapshotEventCount), nil
}

func (s *Storage) EstimateTableRowsCount(_ abstract.TableID) (uint64, error) {
	return uint64(s.SnapshotEventCount), nil
}

func (s *Storage) TableExists(_ abstract.TableID) (bool, error) {
	return true, nil
}

func stampSampleRowsPartID(rows []abstract.ChangeItem, table abstract.TableDescription) {
	partID := table.GeneratePartID()
	if partID == "" {
		return
	}
	for i := range rows {
		rows[i].PartID = partID
	}
}

func NewStorage(config *SampleSource, workersCount int, log log.Logger) (*Storage, error) {
	if workersCount < 1 {
		workersCount = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	partsCount := 1
	if config.PartsCount > 0 {
		partsCount = config.PartsCount
	}
	return &Storage{
		ctx:                ctx,
		metrics:            stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		SampleType:         config.SampleType,
		SnapshotEventCount: config.SnapshotEventCount,
		TableName:          config.TableName,
		MaxSampleData:      config.MaxSampleData,
		MinSleepTime:       config.MinSleepTime,
		logger:             log,
		cancel:             cancel,
		workersCount:       workersCount,
		partsCount:         partsCount,
	}, nil
}
