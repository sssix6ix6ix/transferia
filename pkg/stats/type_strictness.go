package stats

import (
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
)

type TypeStrictnessStats struct {
	registry core_metrics.Registry

	Good core_metrics.Counter
	Bad  core_metrics.Counter
}

func NewTypeStrictnessStats(registry core_metrics.Registry) *TypeStrictnessStats {
	result := &TypeStrictnessStats{
		registry: registry,

		Good: registry.Counter("middleware.strictness.good"),
		Bad:  registry.Counter("middleware.strictness.bad"),
	}
	return result
}
