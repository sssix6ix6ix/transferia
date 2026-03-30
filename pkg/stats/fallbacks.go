package stats

import core_metrics "github.com/transferia/transferia/library/go/core/metrics"

type FallbackStats struct {
	registry core_metrics.Registry

	// Items counts the number of items to which fallbacks chain was applied
	Items core_metrics.Counter
	// Deepness tracks the number of fallbacks applied
	Deepness core_metrics.Gauge
	// Errors counts the number of errorneous fallbacks
	Errors core_metrics.Counter
}

// FallbackStatsCombination is an object unifying stats for source and target fallbacks
type FallbackStatsCombination struct {
	registry core_metrics.Registry

	Source *FallbackStats
	Target *FallbackStats
}

func NewFallbackStatsCombination(registry core_metrics.Registry) *FallbackStatsCombination {
	return &FallbackStatsCombination{
		registry: registry,

		Source: &FallbackStats{
			registry: registry,

			Items:    registry.Counter("fallbacks.source.items"),
			Deepness: registry.Gauge("fallbacks.source.deepness"),
			Errors:   registry.Counter("fallbacks.source.errors"),
		},
		Target: &FallbackStats{
			registry: registry,

			Items:    registry.Counter("fallbacks.target.items"),
			Deepness: registry.Gauge("fallbacks.target.deepness"),
			Errors:   registry.Counter("fallbacks.target.errors"),
		},
	}
}
