package stats

import core_metrics "github.com/transferia/transferia/library/go/core/metrics"

type WorkerStats struct {
	OOMKilled            core_metrics.Gauge
	OOMKills             core_metrics.Counter
	RestartFailure       core_metrics.Gauge
	RestartFailures      core_metrics.Counter
	FatalRestartFailure  core_metrics.Gauge
	FatalRestartFailures core_metrics.Counter
}

func NewWorkerStats(cpRegistry core_metrics.Registry, dpRegistry core_metrics.Registry) *WorkerStats {
	return &WorkerStats{
		OOMKilled:            dpRegistry.Gauge("runtime.oom"),
		OOMKills:             cpRegistry.Counter("runtime.oom"),
		RestartFailure:       dpRegistry.Gauge("worker.failure"),
		RestartFailures:      cpRegistry.Counter("worker.failure"),
		FatalRestartFailure:  dpRegistry.Gauge("worker.failure.fatal"),
		FatalRestartFailures: cpRegistry.Counter("worker.failure.fatal"),
	}
}
