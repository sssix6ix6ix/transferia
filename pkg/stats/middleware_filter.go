package stats

import core_metrics "github.com/transferia/transferia/library/go/core/metrics"

type MiddlewareFilterStats struct {
	registry core_metrics.Registry

	Dropped core_metrics.Counter
}

func NewMiddlewareFilterStats(r core_metrics.Registry) *MiddlewareFilterStats {
	rWT := r.WithTags(map[string]string{"component": "middleware_filter"})
	return &MiddlewareFilterStats{
		registry: rWT,

		Dropped: rWT.Counter("middleware.filter.dropped"),
	}
}
