package events

import (
	"github.com/transferia/transferia/pkg/abstract2"
	"github.com/transferia/transferia/pkg/providers/yt/tablemeta"
)

type tableEvent struct {
	path *tablemeta.YtTableMeta
}

type TableEvent interface {
	abstract2.Event
	Table() *tablemeta.YtTableMeta
}

func (t *tableEvent) Table() *tablemeta.YtTableMeta {
	return t.path
}

func newTableEvent(path *tablemeta.YtTableMeta) TableEvent {
	return &tableEvent{path}
}
