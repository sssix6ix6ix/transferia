package mysql

import (
	mysql_driver "github.com/go-mysql-org/go-mysql/mysql"
	mysql_replication "github.com/go-mysql-org/go-mysql/replication"
)

type EventHandler interface {
	OnRotate(roateEvent *mysql_replication.RotateEvent) error
	// OnTableChanged is called when the table is created, altered, renamed or dropped.
	// You need to clear the associated data like cache with the table.
	// It will be called before OnDDL.
	OnTableChanged(schema string, table string) error
	OnDDL(nextPos mysql_driver.Position, queryEvent *mysql_replication.QueryEvent) error
	OnRow(event *RowsEvent) error
	OnXID(nextPos mysql_driver.Position) error
	OnGTID(gtid mysql_driver.GTIDSet) error
	// OnPosSynced Use your own way to sync position. When force is true, sync position immediately.
	OnPosSynced(pos mysql_driver.Position, set mysql_driver.GTIDSet, force bool) error
	String() string
}

type DummyEventHandler struct {
}

func (h *DummyEventHandler) OnRotate(*mysql_replication.RotateEvent) error    { return nil }
func (h *DummyEventHandler) OnTableChanged(schema string, table string) error { return nil }
func (h *DummyEventHandler) OnDDL(nextPos mysql_driver.Position, queryEvent *mysql_replication.QueryEvent) error {
	return nil
}
func (h *DummyEventHandler) OnRow(*RowsEvent) error            { return nil }
func (h *DummyEventHandler) OnXID(mysql_driver.Position) error { return nil }
func (h *DummyEventHandler) OnGTID(mysql_driver.GTIDSet) error { return nil }
func (h *DummyEventHandler) OnPosSynced(mysql_driver.Position, mysql_driver.GTIDSet, bool) error {
	return nil
}

func (h *DummyEventHandler) String() string { return "DummyEventHandler" }

// `SetEventHandler` registers the sync handler, you must register your
// own handler before starting Canal.
func (c *Canal) SetEventHandler(h EventHandler) {
	c.eventHandler = h
}
