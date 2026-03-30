package mysql

import (
	"sync"

	mysql_driver "github.com/go-mysql-org/go-mysql/mysql"
	siddontang_log "github.com/siddontang/go-log/log"
)

type masterInfo struct {
	sync.RWMutex

	pos mysql_driver.Position

	gset mysql_driver.GTIDSet

	timestamp uint32
}

func (m *masterInfo) Update(pos mysql_driver.Position) {
	siddontang_log.Debugf("update master position %s", pos)

	m.Lock()
	m.pos = pos
	m.Unlock()
}

func (m *masterInfo) UpdateTimestamp(ts uint32) {
	siddontang_log.Debugf("update master timestamp %d", ts)

	m.Lock()
	m.timestamp = ts
	m.Unlock()
}

func (m *masterInfo) UpdateGTIDSet(gset mysql_driver.GTIDSet) {
	siddontang_log.Debugf("update master gtid set %s", gset)

	m.Lock()
	m.gset = gset
	m.Unlock()
}

func (m *masterInfo) Position() mysql_driver.Position {
	m.RLock()
	defer m.RUnlock()

	return m.pos
}

func (m *masterInfo) Timestamp() uint32 {
	m.RLock()
	defer m.RUnlock()

	return m.timestamp
}

func (m *masterInfo) GTIDSet() mysql_driver.GTIDSet {
	m.RLock()
	defer m.RUnlock()

	if m.gset == nil {
		return nil
	}
	return m.gset.Clone()
}
