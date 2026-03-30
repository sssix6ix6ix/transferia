package stats

import (
	"fmt"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
)

type AuthStats struct {
	stats map[authMetricKey]core_metrics.Counter
}

type authMetricKey struct {
	authSuccess bool
}

func (s *AuthStats) Add(authSuccess bool) {
	key := authMetricKey{
		authSuccess: authSuccess,
	}
	s.stats[key].Inc()
}

func NewAuthStats(registry core_metrics.Registry) *AuthStats {
	stats := make(map[authMetricKey]core_metrics.Counter)
	for _, authSuccess := range []bool{false, true} {
		subRegistry := registry.WithTags(map[string]string{"success": fmt.Sprintf("%t", authSuccess)})
		stats[authMetricKey{authSuccess: authSuccess}] = subRegistry.Counter("auth")
	}

	return &AuthStats{stats: stats}
}
