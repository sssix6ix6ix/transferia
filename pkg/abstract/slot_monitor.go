package abstract

import (
	"context"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
)

type SlotKiller interface {
	KillSlot() error
}

type StubSlotKiller struct {
}

func (k *StubSlotKiller) KillSlot() error {
	return nil
}

func MakeStubSlotKiller() SlotKiller {
	return &StubSlotKiller{}
}

type MonitorableSlot interface {
	RunSlotMonitor(ctx context.Context, serverSource interface{}, registry core_metrics.Registry) (SlotKiller, <-chan error, error)
}
