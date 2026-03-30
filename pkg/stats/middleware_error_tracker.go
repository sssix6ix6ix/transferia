package stats

import core_metrics "github.com/transferia/transferia/library/go/core/metrics"

type MiddlewareErrorTrackerStats struct {
	Failures  core_metrics.Counter
	Successes core_metrics.Counter
}

func NewMiddlewareErrorTrackerStats(r core_metrics.Registry) *MiddlewareErrorTrackerStats {
	rWT := r.WithTags(map[string]string{"component": "middleware_error_tracker"})
	return &MiddlewareErrorTrackerStats{
		Failures:  rWT.Counter("middleware.error_tracker.failures"),
		Successes: rWT.Counter("middleware.error_tracker.success"),
	}
}
