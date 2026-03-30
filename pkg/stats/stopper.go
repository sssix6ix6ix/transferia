package stats

import (
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
)

type StopperStats struct {
	TotalTransfers          core_metrics.IntGauge
	WarningLabeledTransfers core_metrics.IntGauge
	ErrorLabeledTransfers   core_metrics.IntGauge

	pm core_metrics.Registry
}

func NewStopperStats(mtrc core_metrics.Registry) *StopperStats {
	pm := mtrc.WithTags(map[string]string{
		"component": "regular_stopper",
	})
	return &StopperStats{
		TotalTransfers:          pm.IntGauge("transfers.total"),
		WarningLabeledTransfers: pm.IntGauge("transfers.warning_labels"),
		ErrorLabeledTransfers:   pm.IntGauge("transfers.error_labels"),
		pm:                      pm,
	}
}
