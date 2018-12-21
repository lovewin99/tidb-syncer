package canal

import (
	"tidb-syncer/utils/mysql"
	"tidb-syncer/utils/replication"
)

type EventHandler interface {
	OnRotate(roateEvent *replication.RotateEvent) error
	// OnTableChanged is called when the table is created, altered, renamed or dropped.
	// You need to clear the associated data like cache with the table.
	// It will be called before OnDDL.
	OnTableChanged(schema, table string) error
	OnDDL(nextPos *mysql.Position, db string, table string, prefix [][]byte, suffix [][]byte, isDrop bool, args ...string) error
	OnRow(e *RowsEvent) error
	OnXID(nextPos mysql.Position) error
	OnGTID(gtid mysql.GTIDSet) error
	// OnPosSynced Use your own way to sync position. When force is true, sync position immediately.
	OnPosSynced(pos mysql.Position, force bool) error
	String() string
	OnDDLAlter(mb [][]byte, db []byte) (rdb, table []byte, prefix, suffix [][]byte, newInfo []string, err error)
	OnDDLCreateLike(tdb, ttable []byte) (prefix, suffix [][]byte, err error)
}

type DummyEventHandler struct {
}

func (h *DummyEventHandler) OnRotate(*replication.RotateEvent) error   { return nil }
func (h *DummyEventHandler) OnTableChanged(schema, table string) error { return nil }
func (h *DummyEventHandler) OnDDL(nextPos *mysql.Position, db string, table string, prefix [][]byte, suffix [][]byte, isDrop bool, args ...string) error {
	return nil
}
func (h *DummyEventHandler) OnRow(*RowsEvent) error                 { return nil }
func (h *DummyEventHandler) OnXID(mysql.Position) error             { return nil }
func (h *DummyEventHandler) OnGTID(mysql.GTIDSet) error             { return nil }
func (h *DummyEventHandler) OnPosSynced(mysql.Position, bool) error { return nil }
func (h *DummyEventHandler) String() string                         { return "DummyEventHandler" }
func (h *DummyEventHandler) OnDDLAlter(mb [][]byte, db []byte) (rdb, table []byte, prefix, suffix [][]byte, newInfo []string, err error) {
	return
}
func (h *DummyEventHandler) OnDDLCreateLike(tdb, ttable []byte) (prefix, suffix [][]byte, err error) {
	return
}

// `SetEventHandler` registers the sync handler, you must register your
// own handler before starting Canal.
func (c *Canal) SetEventHandler(h EventHandler) {
	c.eventHandler = h
}
