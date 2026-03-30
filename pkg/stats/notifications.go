package stats

import core_metrics "github.com/transferia/transferia/library/go/core/metrics"

type NotificationStats struct {
	registry core_metrics.Registry

	// Sent is the number of notifications sent successfully
	Sent core_metrics.Counter
	// Errors is the number of errors
	Errors core_metrics.Counter
}

func NewNotificationStats(registry core_metrics.Registry) *NotificationStats {
	return &NotificationStats{
		registry: registry,

		Sent:   registry.Counter("notifications.sent"),
		Errors: registry.Counter("notifications.errors"),
	}
}
