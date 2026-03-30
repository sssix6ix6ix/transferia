package logbroker

import (
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	ydb_topics_sink "github.com/transferia/transferia/pkg/providers/ydb/topics/sink"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewReplicationSink(cfg *LbDestination, registry core_metrics.Registry, lgr log.Logger, transferID string) (abstract.Sinker, error) {
	return ydb_topics_sink.NewReplicationSink(cfg.TopicSinkConfig(), registry, lgr, transferID)
}

func NewSnapshotSink(cfg *LbDestination, registry core_metrics.Registry, lgr log.Logger, transferID string) (abstract.Sinker, error) {
	return ydb_topics_sink.NewSnapshotSink(cfg.TopicSinkConfig(), registry, lgr, transferID)
}
