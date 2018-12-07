package river


import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/siddontang/go/sync2"
)

/**
	for promethues
 */


type stat struct {
	r *River

	l net.Listener

	// dml
	EventWriteRow	sync2.AtomicInt64
	EventUpdateRow	sync2.AtomicInt64
	EventDeleteRow	sync2.AtomicInt64
	// ddl
	EventQuery		sync2.AtomicInt64
	// other
	EventRotate		sync2.AtomicInt64
}

func (s *stat) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	log.Debug("ServeHTTP !!!")
	var buf bytes.Buffer

	rr, err := s.r.canal.Execute("SHOW MASTER STATUS")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("execute sql error %v", err)))
		return
	}

	binName, _ := rr.GetString(0, 0)
	binPos, _ := rr.GetUint(0, 1)

	pos := s.r.canal.SyncedPosition()

	binIndex := strings.Split(binName, ".")[1]
	index := strings.Split(pos.Name, ".")[1]

	buf.WriteString("# HELP syncer_binlog_events_total total number of binlog events\n")
	buf.WriteString("# TYPE syncer_binlog_events_total counter\n")
	buf.WriteString("syncer_binlog_events_total{type=\"delete_rows\"} " + fmt.Sprintf("%v", s.EventDeleteRow.Get()) + "\n")
	s.EventDeleteRow.Set(0)
	buf.WriteString("syncer_binlog_events_total{type=\"query\"} " + fmt.Sprintf("%v", s.EventQuery.Get()) + "\n")
	s.EventQuery.Set(0)
	buf.WriteString("syncer_binlog_events_total{type=\"rotate\"} " + fmt.Sprintf("%v", s.EventRotate.Get()) + "\n")
	s.EventRotate.Set(0)
	buf.WriteString("syncer_binlog_events_total{type=\"update_rows\"} " + fmt.Sprintf("%v", s.EventUpdateRow.Get()) + "\n")
	s.EventUpdateRow.Set(0)
	buf.WriteString("syncer_binlog_events_total{type=\"write_rows\"} " + fmt.Sprintf("%v", s.EventWriteRow.Get()) + "\n")
	s.EventWriteRow.Set(0)

	buf.WriteString("# HELP syncer_binlog_file current binlog file index\n")
	buf.WriteString("# TYPE syncer_binlog_file gauge\n")
	buf.WriteString("syncer_binlog_file{node=\"master\"} " + fmt.Sprintf("%v", binIndex) + "\n")
	buf.WriteString("syncer_binlog_file{node=\"syncer\"} " + fmt.Sprintf("%v", index) + "\n")

	buf.WriteString("# HELP syncer_binlog_pos current binlog pos\n")
	buf.WriteString("# TYPE syncer_binlog_pos gauge\n")
	buf.WriteString("syncer_binlog_pos{node=\"master\"} " + fmt.Sprintf("%v", binPos) + "\n")
	buf.WriteString("syncer_binlog_pos{node=\"syncer\"} " + fmt.Sprintf("%v", pos.Pos) + "\n")

	w.Write(buf.Bytes())
}

func (s *stat) Run(addr string) {
	if len(addr) == 0 {
		return
	}
	log.Infof("run status http server %s", addr)
	var err error
	s.l, err = net.Listen("tcp", addr)
	if err != nil {
		log.Errorf("listen stat addr %s err %v", addr, err)
		return
	}

	srv := http.Server{}
	mux := http.NewServeMux()
	mux.Handle("/metrics", s)
	srv.Handler = mux

	srv.Serve(s.l)
}

func (s *stat) Close() {
	if s.l != nil {
		s.l.Close()
	}
}