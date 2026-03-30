package stats

import core_metrics "github.com/transferia/transferia/library/go/core/metrics"

type ReplicationStats struct {
	StartUnix core_metrics.Gauge
	Running   core_metrics.Gauge
}

func NewReplicationStats(registry core_metrics.Registry) *ReplicationStats {
	return &ReplicationStats{
		StartUnix: registry.Gauge("replication.start.unix"),
		Running:   registry.Gauge("replication.running"),
	}
}
