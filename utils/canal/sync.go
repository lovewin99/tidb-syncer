package canal

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"tidb-syncer/utils/mysql"
	"tidb-syncer/utils/replication"
	"tidb-syncer/utils/schema"
)

var (
	expCreateTable = regexp.MustCompile("(?i)(^CREATE\\s+TABLE)(\\s+IF\\s+NOT\\s+EXISTS){0,1}\\s+`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}\\s*(\\(.*)")
	expAlterTable  = regexp.MustCompile("(?i)(^ALTER\\s+(IGNORE\\s+){0,1}TABLE)\\s+`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}\\s+(DROP|ADD|RENAME|CHANGE|MODIFY|ALTER)\\s+(.*)")
	//expRenameTable = regexp.MustCompile("(?i)(^RENAME\\s+TABLE\\s+.*?)`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}(\\s+TO\\s.*?)")
	expDropTable       = regexp.MustCompile("(?i)(^DROP\\s+TABLE)(\\s+IF\\s+EXISTS){0,1}\\s+`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}\\s+(.*)")
	expTruncateTable   = regexp.MustCompile("(?i)(TRUNCATE)(\\s+TABLE){0,1}\\s+`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+)`{0,1}\\s+")
	expCreateLikeTable = regexp.MustCompile("(?i)(^CREATE\\s+TABLE)(\\s+IF\\s+NOT\\s+EXISTS){0,1}\\s+`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}\\s+(LIKE)\\s+`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}\\s+(.*)")
	expCreateIndex     = regexp.MustCompile("(?i)(^CREATE\\s+INDEX\\s+[^`\\.]+?\\s+ON)\\s+`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}\\s*(\\(\\s*`{0,1}.*?`{0,1}\\s*\\)\\s*)")
)

func (c *Canal) startSyncer() (*replication.BinlogStreamer, error) {
	gset := c.master.GTIDSet()
	if gset == nil {
		pos := c.master.Position()
		s, err := c.syncer.StartSync(pos)
		if err != nil {
			return nil, errors.Errorf("start sync replication at binlog %v error %v", pos, err)
		}
		log.Infof("start sync binlog at binlog file %v", pos)
		return s, nil
	} else {
		s, err := c.syncer.StartSyncGTID(gset)
		if err != nil {
			return nil, errors.Errorf("start sync replication at GTID set %v error %v", gset, err)
		}
		log.Infof("start sync binlog at GTID set %v", gset)
		return s, nil
	}
}

func DelComments(str string) (string, error) {
	tbuf := bytes.Buffer{}
	buf := bytes.Buffer{}
	mb := []rune(str)
	flag := true
	isFirst := true
	for i := 0; i < len(mb)-1; i++ {
		if string(mb[i]) == "/" && string(mb[i+1]) == "*" {
			flag = false
			if tbuf.Len() != 0 {
				buf.Write(tbuf.Bytes())
				tbuf.Reset()
			}
			tbuf.WriteString(string(mb[i]))
			i++
			if i < len(mb) {
				tbuf.WriteString(string(mb[i]))
			}
			continue
		} else if string(mb[i]) == "*" && string(mb[i+1]) == "/" {
			tbuf.Reset()
			flag = true
			i++
			if !isFirst {
				buf.WriteString(" ")
			}
			continue
		}

		if !flag {
			tbuf.WriteString(string(mb[i]))
			if i == len(mb)-2 {
				tbuf.WriteString(string(mb[i+1]))
			}
		}

		if flag && (!isFirst || string(mb[i]) != " ") {
			isFirst = false
			buf.WriteString(string(mb[i]))
			if i == len(mb)-2 {
				buf.WriteString(string(mb[i+1]))
			}
		}
	}
	buf.Write(tbuf.Bytes())
	return buf.String(), nil
}

func (c *Canal) runSyncBinlog() error {
	s, err := c.startSyncer()
	if err != nil {
		return err
	}

	savePos := false
	force := false
	for {
		ev, err := s.GetEvent(c.ctx)

		if err != nil {
			return errors.Trace(err)
		}
		savePos = false
		force = false
		pos := c.master.Position()

		log.Infof("[current pos]%v, [next pos]%v", pos, ev.Header.LogPos)

		curPos := pos.Pos
		//next binlog pos
		pos.Pos = ev.Header.LogPos

		// We only save position with RotateEvent and XIDEvent.
		// For RowsEvent, we can't save the position until meeting XIDEvent
		// which tells the whole transaction is over.
		// TODO: If we meet any DDL query, we must save too.
		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			pos.Name = string(e.NextLogName)
			pos.Pos = uint32(e.Position)
			log.Infof("rotate binlog to %s", pos)
			savePos = true
			force = true
			if err = c.eventHandler.OnRotate(e); err != nil {
				return errors.Trace(err)
			}
		case *replication.RowsEvent:
			// we only focus row based event
			err = c.handleRowsEvent(ev)
			if err != nil {
				e := errors.Cause(err)
				// if error is not ErrExcludedTable or ErrTableNotExist or ErrMissingTableMeta, stop canal
				if e != ErrExcludedTable &&
					e != schema.ErrTableNotExist &&
					e != schema.ErrMissingTableMeta {
					log.Errorf("handle rows event at (%s, %d) error %v\n", pos.Name, curPos, err)
					return errors.Trace(err)
				}
			}
			continue
		case *replication.XIDEvent:
			if e.GSet != nil {
				c.master.UpdateGTIDSet(e.GSet)
			}
			savePos = true
			// try to save the position later
			if err := c.eventHandler.OnXID(pos); err != nil {
				return errors.Trace(err)
			}
		case *replication.MariadbGTIDEvent:
			// try to save the GTID later
			gtid, err := mysql.ParseMariadbGTIDSet(e.GTID.String())
			if err != nil {
				return errors.Trace(err)
			}
			if err := c.eventHandler.OnGTID(gtid); err != nil {
				return errors.Trace(err)
			}
		case *replication.GTIDEvent:
			u, _ := uuid.FromBytes(e.SID)
			gtid, err := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d", u.String(), e.GNO))
			if err != nil {
				return errors.Trace(err)
			}
			if err := c.eventHandler.OnGTID(gtid); err != nil {
				return errors.Trace(err)
			}
		case *replication.QueryEvent:
			if e.GSet != nil {
				c.master.UpdateGTIDSet(e.GSet)
			}

			// skip ddl
			var skipFlag bool
			if len(c.skipDdlRegex) != 0 {
				for _, reg := range c.skipDdlRegex {
					skipFlag = reg.MatchString(strings.ToLower(string(e.Query)))
					if skipFlag {
						break
					}
				}
			}

			if skipFlag {
				log.Infof("skip ddl: %v", string(e.Query))
				continue
			}

			var (
				mb      [][]byte
				db      []byte
				table   []byte
				prefix  [][]byte
				suffix  [][]byte
				newInfo []string
			)

			const (
				Create = iota
				Alter
				Drop
				Truncate
				CreateLike
				CreateIndex
			)
			regexps := []regexp.Regexp{*expCreateTable, *expAlterTable, *expDropTable, *expTruncateTable, *expCreateLikeTable, *expCreateIndex}

			isDrop := false

			log.Debugf("e.query = %v$", string(e.Query))
			query, _ := DelComments(string(e.Query))
			log.Debugf("e.query format= %v$", query)
			for i, reg := range regexps {
				mb = reg.FindSubmatch([]byte(strings.Replace(query+" ", "\n", " ", -1)))
				if len(mb) != 0 {
					switch i {
					case Create:
						log.Debugf("create sql = %v", string(mb[0]))
						if len(mb[3]) == 0 {
							db = e.Schema
						} else {
							db = mb[3]
						}
						table = mb[4]
						prefix = [][]byte{mb[1]}
						prefix = append(prefix, []byte("if not exists"))
						suffix = mb[5:]
					case Alter:
						log.Debugf("alter sql = %v", string(mb[0]))

						// TODO drop primary key
						switch strings.ToLower(string(mb[5])) {
						case "rename":
							//isChanged = true
							isDrop = true
						default:
						}

						var err error
						db, table, prefix, suffix, newInfo, err = c.eventHandler.OnDDLAlter(mb, e.Schema)
						if err != nil {
							if err.Error() == "skip" {
								log.Infof("skip alter ddl: %v", string(mb[0]))
								mb = [][]byte{}
							} else if err.Error() == "not match" {
								mb = [][]byte{}
							} else {
								return err
							}
						}
					case Drop:
						log.Debugf("drop sql = %v", string(mb[0]))
						if len(mb[3]) == 0 {
							db = e.Schema
						} else {
							db = mb[3]
						}
						table = mb[4]
						prefix = [][]byte{mb[1]}
						prefix = append(prefix, []byte("if exists"))
						suffix = [][]byte{}
						isDrop = true
					case Truncate:
						log.Debugf("truncate sql = %v", string(mb[0]))
						if len(mb[3]) == 0 {
							db = e.Schema
						} else {
							db = mb[3]
						}
						table = mb[4]
						prefix = mb[1:3]
						suffix = [][]byte{}
					case CreateLike:
						log.Debugf("CreateLike sql = %v", string(mb[0]))
						//ts := "(?i)(^CREATE\\s+TABLE)(\\s+IF\\s+NOT\\s+EXISTS){0,1}\\s+`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}\\s+(LIKE)\\s+`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}\\s+(.*)"

						if len(mb[3]) == 0 {
							db = e.Schema
						} else {
							db = mb[3]
						}
						table = mb[4]
						var likedb string

						// get like db&table
						if len(mb[6]) == 0 {
							likedb = string(e.Schema)
						} else {
							likedb = string(mb[6])
						}
						liketb := string(mb[7])
						res, err := c.Execute(fmt.Sprintf("show create table %s.%s", likedb, liketb))
						if err != nil {
							log.Errorln(err)
							return errors.Trace(err)
						}

						// Get create table statement
						sql, err := res.GetString(0, 1)
						if err != nil {
							log.Errorln(err)
							return errors.Trace(err)
						}
						query1, _ := DelComments(string(sql))
						// Execute the logic of create table
						mb = (*expCreateTable).FindSubmatch([]byte(strings.Replace(query1+" ", "\n", " ", -1)))
						log.Debugf("create sql = %v", string(mb[0]))
						prefix = [][]byte{mb[1]}
						prefix = append(prefix, []byte("if not exists"))
						suffix = mb[5:]
					case CreateIndex:
						log.Debugf("CreateIndex sql = %v", string(mb[0]))
						if len(mb[2]) == 0 {
							db = e.Schema
						} else {
							db = mb[2]
						}
						table = mb[3]
						prefix = [][]byte{mb[1]}
						suffix = [][]byte{mb[4]}
					}
					break
				}
			}

			if len(mb) == 0 {
				continue
			}

			key := fmt.Sprintf("%s.%s", string(db), string(table))
			if !c.CheckTableMatch(key) {
				log.Debugf("table %v.%v not match !!!", string(db), string(table))
				continue
			}

			savePos = true
			force = true
			c.ClearTableCache(db, table)
			log.Infof("table structure changed, clear table cache: %s.%s", db, table)
			//if !isChanged{
			//	if err = c.eventHandler.OnTableChanged(string(db), string(table)); err != nil {
			//		return errors.Trace(err)
			//	}
			//}

			// Now we only handle Table Changed DDL, maybe we will support more later.
			if err = c.eventHandler.OnDDL(&pos, string(db), string(table), prefix, suffix, isDrop, newInfo...); err != nil {
				return errors.Trace(err)
			}
		default:
			continue
		}

		if savePos {
			c.master.Update(pos)
			c.eventHandler.OnPosSynced(pos, force)
		}
	}

	return nil
}

func (c *Canal) handleRowsEvent(e *replication.BinlogEvent) error {
	ev := e.Event.(*replication.RowsEvent)

	// Caveat: table may be altered at runtime.
	schema := string(ev.Table.Schema)
	table := string(ev.Table.Table)

	t, err := c.GetTable(schema, table)
	if err != nil {
		return err
	}
	var action string
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		action = InsertAction
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		action = DeleteAction
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		action = UpdateAction
	default:
		return errors.Errorf("%s not supported now", e.Header.EventType)
	}
	events := newRowsEvent(t, action, ev.Rows, e.Header)
	return c.eventHandler.OnRow(events)
}

func (c *Canal) FlushBinlog() error {
	_, err := c.Execute("FLUSH BINARY LOGS")
	return errors.Trace(err)
}

func (c *Canal) WaitUntilPos(pos mysql.Position, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	for {
		select {
		case <-timer.C:
			return errors.Errorf("wait position %v too long > %s", pos, timeout)
		default:
			err := c.FlushBinlog()
			if err != nil {
				return errors.Trace(err)
			}
			curPos := c.master.Position()
			if curPos.Compare(pos) >= 0 {
				return nil
			} else {
				log.Debugf("master pos is %v, wait catching %v", curPos, pos)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	return nil
}

func (c *Canal) GetMasterPos() (mysql.Position, error) {
	rr, err := c.Execute("SHOW MASTER STATUS")
	if err != nil {
		return mysql.Position{"", 0}, errors.Trace(err)
	}

	name, _ := rr.GetString(0, 0)
	pos, _ := rr.GetInt(0, 1)

	return mysql.Position{name, uint32(pos)}, nil
}

func (c *Canal) GetMasterGTIDSet() (mysql.GTIDSet, error) {
	query := ""
	switch c.cfg.Flavor {
	case mysql.MariaDBFlavor:
		query = "SELECT @@GLOBAL.gtid_current_pos"
	default:
		query = "SELECT @@GLOBAL.GTID_EXECUTED"
	}
	rr, err := c.Execute(query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	gx, err := rr.GetString(0, 0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	gset, err := mysql.ParseGTIDSet(c.cfg.Flavor, gx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return gset, nil
}

func (c *Canal) CatchMasterPos(timeout time.Duration) error {
	pos, err := c.GetMasterPos()
	if err != nil {
		return errors.Trace(err)
	}

	return c.WaitUntilPos(pos, timeout)
}
