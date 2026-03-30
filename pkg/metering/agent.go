package metering

import (
	"context"
	"sync"
	"time"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract2"
)

var (
	commonAgent   MeteringAgent
	commonAgentMu sync.Mutex = sync.Mutex{}
)

type MeteringAgent interface {
	// RunPusher starts background metrics pushing process. RunPusher must not be called after Stop
	// Pusher is stopped either when Stop method is called or incoming Context is Done.
	RunPusher(ctx context.Context, interval time.Duration) error
	// Stop is used to stop metrics pusher (if it was run). Stop must not be called concurrently with RunPusher
	Stop() error
	SetOpts(config *MeteringOpts) error
	CountInputRows(items []abstract.ChangeItem)
	CountOutputRows(items []abstract.ChangeItem)
	CountOutputBatch(input abstract2.EventBatch)

	WithStats(stats *MeteringStats)
}

type Writer interface {
	Write(data string) error
	Close() error
}

type MeteringStats struct {
	Preview core_metrics.IntGauge
	Writers core_metrics.IntGauge
}

func (m *MeteringStats) Reset() {
	m.Preview.Set(0)
	m.Writers.Set(0)
}
