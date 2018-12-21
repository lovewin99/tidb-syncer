package river

import (
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"regexp"
	"strings"
	"tidb-syncer/utils/canal"
	"tidb-syncer/utils/mysql"
	"tidb-syncer/utils/replication"
	"time"
)

const (
	syncInsertDoc = iota
	syncDeleteDoc
	syncUpdateDoc
)

const (
	fieldTypeList = "list"
	// for the mysql int type to es date type
	// set the [rule.field] created_time = ",date"
	fieldTypeDate = "date"
	// The maximum number of savepoints in the saver thread cache
	mapBufferSize = 1000
	// Connection target library failed retry times
	retryNum = 100
)

var (
	expRename = regexp.MustCompile("(?i)(RENAME)\\s+(TO\\s+){0,1}`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}(\\s+.*)")
	expChange = regexp.MustCompile("(?i)(CHANGE)\\s+(COLUMN\\s+){0,1}(.*?)\\s+(.*?)\\s+(.*?)(\\s+.*)")
	expModify = regexp.MustCompile("(?i)(MODIFY)\\s+(COLUMN\\s+){0,1}(.*?)\\s+(.*?)(\\s+.*)")
)

// for saver, save binlog information and count
type saveInfo struct {
	count int
	pos   mysql.Position
}

func (s *saveInfo) add() {
	s.count++
}

type eventHandler struct {
	r *River
}

// Special handling of alter in ddl operation
func (h *eventHandler) OnDDLAlter(mb [][]byte, db []byte) (rdb, table []byte, prefix, suffix [][]byte, newInfo []string, err error) {

	if len(mb[3]) == 0 {
		rdb = db
	} else {
		rdb = mb[3]
	}

	table = mb[4]

	key := fmt.Sprintf("%s.%s", string(rdb), string(table))
	if !h.r.canal.CheckTableMatch(key) {
		log.Debugf("table %v.%v not match !!!", string(db), string(table))
		err = errors.New("not match")
		return
	}

	switch strings.ToLower(string(mb[5])) {
	case "add":
		srcbuf := bytes.Buffer{}
		for _, v := range mb[5:] {
			srcbuf.WriteString(" ")
			srcbuf.Write(v)
		}
		infoArr := []rune(srcbuf.String())
		prefix = [][]byte{mb[1]}

		buf := bytes.Buffer{}
		sqlArr := []string{}

		flag := false
		buf.WriteRune(infoArr[0])
		for i := 1; i < len(infoArr); i++ {
			if string(infoArr[i]) == "'" && string(infoArr[i-1]) != "\\" {
				if flag {
					flag = false
				} else {
					flag = true
				}
			}
			if !flag && string(infoArr[i]) == "," {
				sqlArr = append(sqlArr, buf.String())
				buf.Reset()
				buf.WriteString(" ")
			} else {
				buf.WriteRune(infoArr[i])
			}
		}
		sqlArr = append(sqlArr, buf.String())
		for i := 0; i < len(sqlArr)-1; i++ {
			err = h.OnDDL(nil, string(rdb), string(table), prefix, [][]byte{[]byte(sqlArr[i])}, false)
			if err != nil {
				return
			}
		}

		err = nil
		suffix = [][]byte{[]byte(sqlArr[len(sqlArr)-1])}

	case "change", "modify":
		//Change the field type to allow execution if it is of the same type and the field length becomes larger
		prefix = [][]byte{mb[1]}
		buf := bytes.Buffer{}
		for _, v := range mb[5:] {
			buf.Write(v)
			buf.WriteString(" ")
		}
		var oldColName, newColName, newColType string
		var tbuf [][]byte
		var last []byte
		if strings.ToLower(string(mb[5])) == "change" {
			tbuf = expChange.FindSubmatch(buf.Bytes())
			oldColName = string(tbuf[3])
			newColName = string(tbuf[4])
			newColType = string(tbuf[5])
			last = tbuf[6]
		} else if strings.ToLower(string(mb[5])) == "modify" {
			tbuf := expModify.FindSubmatch(buf.Bytes())
			oldColName = string(tbuf[3])
			newColType = string(tbuf[4])
			newColName = oldColName
			last = tbuf[5]
		}

		var oldColType string
		rule, ok := h.r.rules[ruleKey(string(rdb), string(table))]

		// Connect to TIDB to get old table field information
		var conn *sql.DB
		conn, err = h.r.getConn()
		if err != nil {
			log.Error(err)
			return
		}
		var m map[string]string
		m, err = fetchColType(conn, rule.Tschema, rule.Ttable)
		if err != nil {
			return
		}
		oldColType, ok = m[oldColName]
		if !ok {
			err = errors.New(fmt.Sprintf("colname is not exists in tidb `%v`.`%v`: %v", rule.Tschema, rule.Ttable, oldColName))
			log.Error(err.Error())
			return
		}

		log.Debugf("oldColType = %v", oldColType)
		log.Debugf("oldColName = %v", oldColName)
		log.Debugf("newColType = %v", newColType)
		log.Debugf("newColName = %v", newColName)

		// Both numeric or varchar type
		if IsSameType(oldColType, newColType) {

			_, oldNum, err1 := TypeLen(oldColType)
			if err1 != nil {
				err = err1
				return
			}
			newtype, newNum, err1 := TypeLen(newColType)
			if err1 != nil {
				err = err1
				return
			}

			if newNum >= oldNum {
				suffix = [][]byte{[]byte(fmt.Sprintf("change %v %v %v %v", oldColName, newColName, newtype, string(last)))}
			} else if strings.ToLower(string(mb[5])) == "change" && oldColName != newColName {
				suffix = [][]byte{[]byte(fmt.Sprintf("change %v %v %v %v", oldColName, newColName, oldColType, string(last)))}
			} else {
				err = errors.New("skip")
			}
			return
		} else if strings.ToLower(string(mb[5])) == "change" && oldColName != newColName {
			suffix = [][]byte{[]byte(fmt.Sprintf("change %v %v %v %v", oldColName, newColName, oldColType, string(last)))}
			return
		} else {
			err = errors.New("skip")
			return
		}

	case "rename":
		/**
		For new table mapping
		*/
		prefix = [][]byte{mb[1]}
		// invalid old table; Prevent old table deletion
		//err1 := h.OnTableChanged(string(rdb), string(table))
		//if err1 != nil && err1.Error() != canal.ErrExcludedTable.Error() {
		//	return nil, nil, nil, nil, errors.Trace(err1)
		//}
		var tdb, ttb []byte
		buf := bytes.Buffer{}
		for _, v := range mb[5:] {
			buf.Write(v)
			buf.WriteString(" ")
		}
		log.Debugf("rename suffix $%v$", buf.String())

		// Detailed analysis
		tbuf := expRename.FindSubmatch(buf.Bytes())
		//for _, v := range tbuf{
		//	fmt.Println(string(v))
		//}
		if len(tbuf[3]) == 0 {
			tdb = db
		} else {
			tdb = tbuf[3]
		}
		ttb = tbuf[4]

		log.Debugf("rule db=%v , tb = %v", string(tdb), string(ttb))
		// check rule
		rule, err := h.r.getRuleOrElse(string(tdb), string(ttb))
		if err != nil {
			log.Error(err)
			return nil, nil, nil, nil, nil, err
		}
		suffix = [][]byte{[]byte(fmt.Sprintf("rename to `%v`.`%v`", rule.Tschema, rule.Ttable))}
		newInfo = []string{string(tdb), string(ttb)}
	default:
		prefix = [][]byte{mb[1]}
		suffix = mb[5:]
	}

	return
}

func (h *eventHandler) OnRotate(e *replication.RotateEvent) error {
	// for prometheus
	h.r.st.EventRotate.Add(1)

	pos := mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}

	log.Debugf("OnRotate pos = %v", pos)

	now := time.Now().UnixNano()
	h.r.syncCh <- SaveBarrierMsg{now, false, pos}

	return h.r.ctx.Err()
}

func (h *eventHandler) OnTableChanged(schema, table string) error {

	log.Debugf("OnTableChanged %v.%v", schema, table)
	// for prometheus
	h.r.st.EventQuery.Add(1)

	// update river rule
	err := h.r.updateRule(schema, table)

	if err != nil && err.Error() != canal.ErrExcludedTable.Error() {
		return errors.Trace(err)
	}
	return nil
}

func (h *eventHandler) OnDDLCreateLike(ldb, ltable []byte) (prefix, suffix [][]byte, err error) {
	// check rule
	rule, ok := h.r.rules[ruleKey(string(ldb), string(ltable))]
	if !ok {
		return nil, nil, errors.Errorf("%s.%s not int rules", string(ldb), string(ltable))
	}
	prefix = [][]byte{[]byte("create table if not exists")}
	suffix = [][]byte{[]byte(fmt.Sprintf("like `%v`.`%v`", rule.Tschema, rule.Ttable))}
	return
}

func (h *eventHandler) OnDDL(nextPos *mysql.Position, db string, table string, prefix [][]byte, suffix [][]byte,
	isDrop bool, args ...string) error {

	// check rule
	rule, err := h.r.getRuleOrElse(db, table)

	if err != nil && err.Error() != "table is not exist" {
		log.Errorf("%v.%v %v", db, table, err)
		return err
	}

	// repalce the original schema&table name and generate ddl statement
	var buf bytes.Buffer
	for _, v := range prefix {
		buf.Write(v)
		buf.WriteString(" ")
	}
	buf.WriteString("`")
	buf.WriteString(rule.Tschema)
	buf.WriteString("`")
	buf.WriteString(".")
	buf.WriteString("`")
	buf.WriteString(rule.Ttable)
	buf.WriteString("`")
	buf.WriteString(" ")
	for _, v := range suffix {
		buf.Write(v)
		buf.WriteString(" ")
	}
	buf.WriteString(";")

	log.Debugf("OnDDL sql = %v", buf.String())
	log.Debugf("OnDDL pos = %s", nextPos)

	// ddl -> manager -> task -> saver
	h.r.syncCh <- DdlMsg{buf.String(), nextPos}

	<-h.r.m2sCh

	if len(args) == 2 {
		h.OnTableChanged(args[0], args[1])
	} else {
		h.OnTableChanged(db, table)
	}

	if isDrop {
		delete(h.r.rules, ruleKey(db, table))
	}

	return h.r.ctx.Err()
}

func (h *eventHandler) OnXID(nextPos mysql.Position) error {
	now := time.Now().UnixNano()
	// binlog position -> manager -> task -> saver
	h.r.syncCh <- SaveBarrierMsg{now, false, nextPos}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {

	var dmlMsgArr []*DmlMsg
	var err error
	// Whether it is defined in skipdml
	rule, err := h.r.getRuleOrElse(e.Table.Schema, e.Table.Name)

	if rule.SkipType == e.Action {
		log.Infof("skip %v dml:%v.%v", e.Action, e.Table.Schema, e.Table.Name)
		return nil
	}

	switch e.Action {
	case canal.InsertAction:
		// for promethues
		h.r.st.EventWriteRow.Add(1)
		// generate dml insert statement
		dmlMsgArr, err = h.r.makeInsertRequest(e)
	case canal.UpdateAction:
		// for promethues
		h.r.st.EventUpdateRow.Add(1)
		// generate dml update statement
		dmlMsgArr, err = h.r.makeUpdateRequest(e)
	case canal.DeleteAction:
		// for promethues
		h.r.st.EventUpdateRow.Add(1)
		// generate dml delete statement
		dmlMsgArr, err = h.r.makeDeleteRequest(e)
	default:
		err = errors.Errorf("invalid rows action %s", e.Action)
	}

	if err != nil {
		h.r.cancel()
		return errors.Errorf("make %s request err %v, close sync", e.Action, err)
	}

	//for _, v := range dmlMsgArr {
	//	fmt.Printf("OnRow key = %v sql = %v \n", v.Hashkey, v.Sql)
	//}

	for _, dmlMsg := range dmlMsgArr {
		h.r.syncCh <- *dmlMsg
	}

	return h.r.ctx.Err()
}

func (h *eventHandler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (h *eventHandler) OnPosSynced(pos mysql.Position, force bool) error {
	return nil
}

func (h *eventHandler) String() string {
	return "ESRiverEventHandler"
}
