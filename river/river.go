package river

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"database/sql"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"tidb-syncer/utils/canal"
	"tidb-syncer/utils/mysql"
	//"tidb-syncer/utils/canal"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

const max_allowed_packet = 50000000
const chan_buffer_size = 4096

type River struct {
	// Match the configuration file
	c *Config

	canal *canal.Canal

	rules map[string]*Rule

	ctx    context.Context
	cancel context.CancelFunc

	wg *sync.WaitGroup

	// for promethues
	st *stat

	// for savepoint
	master *masterInfo

	// task -> manager
	barrierCh chan DdlBarrierMsg

	// manager -> task
	mCh map[int](chan Msg)

	// sync -> manager
	syncCh chan Msg

	// manager/task -> saver
	saveCh chan SaveBarrierMsg

	// manager -> sync
	m2sCh chan Msg
}

// NewRiver creates the River from config
func NewRiver(c *Config) (*River, error) {
	r := &River{}

	r.wg = &sync.WaitGroup{}

	r.c = c

	r.rules = make(map[string]*Rule)

	r.syncCh = make(chan Msg, chan_buffer_size)

	r.m2sCh = make(chan Msg, chan_buffer_size)

	r.barrierCh = make(chan DdlBarrierMsg, c.WorkerCount)

	r.saveCh = make(chan SaveBarrierMsg, chan_buffer_size)

	r.mCh = make(map[int]chan Msg, c.WorkerCount)
	for i := 0; i < c.WorkerCount; i++ {
		r.mCh[i] = make(chan Msg, chan_buffer_size)
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	var err error
	if r.master, err = loadMasterInfo(c.DataDir); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.newCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	// Generate rules based on configuration files
	if err = r.prepareRule(); err != nil {
		return nil, errors.Trace(err)
	}

	//  for canal ddl handler
	if err = r.prepareCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	// We must use binlog full row image
	if err = r.canal.CheckBinlogRowImage("FULL"); err != nil {
		return nil, errors.Trace(err)
	}

	r.st = &stat{r: r}
	go r.st.Run(r.c.StatAddr)

	return r, nil
}

// Initialize canal according to configuration
func (r *River) newCanal() error {
	cfg := canal.NewDefaultConfig()
	log.Debugf("host = %v, port = %v", r.c.Fromcfg.Host, strconv.Itoa(int(r.c.Fromcfg.Port)))
	cfg.Addr = r.c.Fromcfg.Host + ":" + strconv.Itoa(int(r.c.Fromcfg.Port))
	cfg.User = r.c.Fromcfg.User
	cfg.Password = r.c.Fromcfg.Password

	cfg.DstAddr = r.c.Tocfg.Host + ":" + strconv.Itoa(int(r.c.Tocfg.Port))
	cfg.DstUser = r.c.Tocfg.User
	cfg.DstPassword = r.c.Tocfg.Password

	cfg.ServerID = r.c.ServerID

	f := func(arr []string, db, tb string) []string {
		if strings.HasPrefix(db, "~") {
			db = strings.Replace(db, "~", "", -1)
			db = strings.Replace(db, "$", "", -1)
		} else {
			db = "^" + db
		}
		if strings.HasPrefix(tb, "~") {
			tb = strings.Replace(tb, "~", "", -1)
			tb = strings.Replace(tb, "^", "", -1)
		} else if tb != "" {
			tb = tb + "$"
		}

		arr = append(arr, db+"\\."+tb)
		return arr
	}

	// replicate-do-db
	for _, v := range r.c.DoDB {
		cfg.IncludeTableRegex = f(cfg.IncludeTableRegex, v, "")
	}

	// replicate-do-table
	for _, s := range r.c.DoTable {
		cfg.IncludeTableRegex = f(cfg.IncludeTableRegex, s.DbName, s.TbName)
	}

	// replicate-ignore-db
	for _, v := range r.c.IgnoreDB {
		cfg.ExcludeTableRegex = f(cfg.ExcludeTableRegex, v, "")
	}

	// replicate-ignore-table
	for _, s := range r.c.IgnoreTable {
		cfg.ExcludeTableRegex = f(cfg.ExcludeTableRegex, s.DbName, s.TbName)
	}

	// skip-ddls
	cfg.SkipDdlRegex = r.c.SkipDdl

	var err error
	r.canal, err = canal.NewCanal(cfg)
	return errors.Trace(err)
}

func (r *River) prepareCanal() error {

	// dump
	r.canal.SetEventHandler(&eventHandler{r})

	return nil
}

// Start the thread oriented to the target db
func (r *River) PreStartSync() {
	// start manager
	go r.ManagerLoop()

	log.Infof("task counts = %v", r.c.WorkerCount)

	// start tasks
	for i := 0; i < r.c.WorkerCount; i++ {
		r.mCh[i] = make(chan Msg, chan_buffer_size)
		//r.wg.Add(1)
		go r.TaskLoop(r.mCh[i], i)
	}

	// start saver
	//r.wg.Add(1)
	go r.SaverLoop()
}

// Run syncs the data from MySQL to manager.
func (r *River) Run() error {
	r.wg.Add(1)
	defer func() {
		r.cancel()
		r.wg.Done()
	}()

	//go r.syncLoop()

	pos := r.master.Position()
	if err := r.canal.RunFrom(pos); err != nil {
		log.Errorf("start canal err %v", err)
		return errors.Trace(err)
	}

	return nil
}

// Ctx returns the internal context for outside use.
func (r *River) Ctx() context.Context {
	return r.ctx
}

// Close closes the River
func (r *River) Close() {
	log.Infof("closing river")

	r.cancel()

	r.canal.Close()

	r.master.Close()

	r.wg.Wait()
}

// get the rule of table
func (r *River) getRuleOrElse(db, table string) (*Rule, error) {
	rule, ok := r.rules[ruleKey(db, table)]
	if !ok {
		log.Infof("create rule for %v.%v", db, table)
		err := r.updateRule(db, table)
		if err != nil && err.Error() != "table is not exist" {
			return r.rules[ruleKey(db, table)], err
		} else {
			rule, _ = r.rules[ruleKey(db, table)]
		}

	}
	return rule, nil
}

// get default rule
func (r *River) newRule(db, table string) error {
	key := ruleKey(db, table)
	if _, ok := r.rules[key]; ok {
		return errors.Errorf("duplicate source %s, %s defined in config", db, table)
	}
	r.rules[key] = newDefaultRule(db, table)
	return nil
}

// According to the configuration to complete the table rules, determine whether to skip some dml operation
func (r *River) updateSkipType(rule *Rule) {
	for _, v := range r.c.SkipDmls {
		if v.TbName == "" && v.DbName == "" {
			rule.SkipType = v.Type
			return
		} else if v.TbName != "" && v.DbName == "" && v.DbName == rule.Pschema {
			rule.SkipType = v.Type
			return
		} else if v.TbName != "" && v.DbName != "" && v.DbName == rule.Pschema && v.TbName == rule.Ptable {
			rule.SkipType = v.Type
			return
		}
	}
}

// fixme 先映射 后加表信息
// Update the rules when the table structure in mysql changes
func (r *River) updateRule(db, table string) error {

	key := ruleKey(db, table)
	delete(r.rules, key)
	r.rules[key] = newDefaultRule(db, table)

	// Whether it is defined in skipdml
	r.updateSkipType(r.rules[key])

	// route-rules mapping
	if r.c.RouteRules != nil {
		for _, rule := range r.c.RouteRules {
			if regexp.QuoteMeta(rule.Ptable) != rule.Ptable {
				reg, err := regexp.Compile(rule.Pschema + "\\." + rule.Ptable)
				if err != nil {
					return errors.Trace(err)
				}
				if reg.MatchString(ruleKey(db, table)) {
					r.rules[key].Tschema = rule.Tschema
					r.rules[key].Ttable = rule.Ttable
				}
			} else if ruleKey(rule.Pschema, rule.Ptable) == ruleKey(db, table) {
				r.rules[key].Tschema = rule.Tschema
				r.rules[key].Ttable = rule.Ttable
			} else if rule.Ptable == "" && rule.Pschema == r.rules[key].Tschema {
				r.rules[key].Tschema = rule.Tschema
			}

		}
	}

	tableInfo, err := r.canal.GetTable(db, table, r.rules[key].Tschema, r.rules[key].Ttable)
	if err != nil {
		return errors.Trace(err)
	}
	r.rules[key].TableInfo = tableInfo

	return nil
}

// Create a rule collection for the first time by sweeping all the tables in the corresponding library in mysql
func (r *River) parseSource() error {
	//flagTables := make(map[string]int)

	// replicate do-db
	for _, s := range r.c.DoDB {
		var sql string
		if regexp.QuoteMeta(s) != s {
			sql = fmt.Sprintf(`SELECT table_schema,table_name FROM information_schema.tables 
				WHERE table_schema RLIKE "%s";`, s)
		} else {
			sql = fmt.Sprintf(`SELECT table_schema,table_name FROM information_schema.tables 
				WHERE table_schema = "%s";`, s)
		}

		res, err := r.canal.Execute(sql)
		if err != nil {
			return errors.Trace(err)
		}

		for i := 0; i < res.Resultset.RowNumber(); i++ {
			dbname, _ := res.GetString(i, 0)
			tbname, _ := res.GetString(i, 1)
			//flagTables[ruleKey(dbname, tbname)] = 1
			err := r.newRule(dbname, tbname)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	// replicate do-table
	for _, t := range r.c.DoTable {
		var sql string
		if regexp.QuoteMeta(t.TbName) != t.TbName || regexp.QuoteMeta(t.DbName) != t.DbName {
			sql = fmt.Sprintf(`SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema RLIKE "%s" AND table_name RLIKE "%s";`, t.DbName, t.TbName)
		} else {
			sql = fmt.Sprintf(`SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = "%s" AND table_name = "%s";`, t.DbName, t.TbName)
		}

		res, err := r.canal.Execute(sql)

		if err != nil {
			return errors.Trace(err)
		}

		for i := 0; i < res.Resultset.RowNumber(); i++ {
			dbname, _ := res.GetString(i, 0)
			tbname, _ := res.GetString(i, 1)
			if _, ok := r.rules[ruleKey(dbname, tbname)]; ok {
				log.Warnf("duplicate wildcard table defined for %s.%s", dbname, tbname)
				continue
			} else {
				//flagTables[ruleKey(dbname, tbname)] = 1
				err := r.newRule(dbname, tbname)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}

	//if len(r.rules) == 0 {
	//	return errors.Errorf("no source data defined")
	//}

	return nil
}

/**
1 Establish mappings based on configuration
2 Improve the primary key and field name ... in the rule according to the mysql source data information
*/
func (r *River) prepareRule() error {
	err := r.parseSource()
	if err != nil {
		return errors.Trace(err)
	}

	// route-rules: Establish mappings based on configuration
	if r.c.RouteRules != nil {
		for _, rule := range r.c.RouteRules {
			if len(rule.Pschema) == 0 {
				return errors.Errorf("empty schema not allowed for route-rules")
			}
			if regexp.QuoteMeta(rule.Pschema) != rule.Pschema {
				return errors.Errorf("regexp dbname is illegal : %s", rule.Pschema)
			}
			//errFlag := true
			for k, v := range r.rules {
				if regexp.QuoteMeta(rule.Ptable) != rule.Ptable {
					reg, err := regexp.Compile(rule.Pschema + "\\." + rule.Ptable)
					if err != nil {
						return errors.Trace(err)
					}
					if reg.MatchString(k) {
						//errFlag = false
						v.Tschema = rule.Tschema
						v.Ttable = rule.Ttable

					}
				} else if ruleKey(rule.Pschema, rule.Ptable) == k {
					//errFlag = false
					v.Tschema = rule.Tschema
					v.Ttable = rule.Ttable
				} else if rule.Ptable == "" && rule.Pschema == v.Pschema {
					v.Tschema = rule.Tschema

				}
			}
			//if errFlag {
			//	return errors.Errorf("route-rules not defined in source : %s %s", rule.Pschema, rule.Ptable)
			//}
		}
	}

	// Improve the primary key and field name ... in the rule according to the tidb data information
	for _, rule := range r.rules {
		// Whether it is defined in skipdml
		r.updateSkipType(rule)

		rule.TableInfo, err = r.canal.GetTable(rule.Pschema, rule.Ptable, rule.Tschema, rule.Ttable)
		if err != nil && err != canal.ErrExcludedTable && err.Error() != "table is not exist" {
			return errors.Trace(err)
		}

		// FIXME PKColumns is null
	}

	return nil
}

// get mysql client
func (r *River) getConn() (*sql.DB, error) {
	var db *sql.DB
	var err error
	for i := 0; i < retryNum && db == nil; i++ {
		db, err = sql.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/", r.c.Tocfg.User, r.c.Tocfg.Password,
			r.c.Tocfg.Host, r.c.Tocfg.Port))
		if err != nil {
			log.Infof("get conn error : retry times *v \n %v", i, err)
			time.Sleep(3 * time.Second)
		}
	}

	return db, err
}

/**
manager thread: Accept the message of the sync thread, if it is a ddl message, send a barrier to the task thread,
start processing the ddl operation after receiving the message returned by all task threads, then send the save
point to the saver thread; if it is a dml message, send it to The corresponding task thread according to the hash key;
if it is a savepoint message, it is sent to all task threads.
*/
func (r *River) ManagerLoop() {
	r.wg.Add(1)

	defer func() {
		log.Infof("manager loop closed")
		r.cancel()
		r.wg.Done()
	}()

	db, err := r.getConn()

	if err != nil {
		log.Errorf("ManagerLoop connnect error \n", err)
		return
	}

	for {
		select {
		case <-r.Ctx().Done():
			return
		case msg := <-r.syncCh:
			switch msg.(type) {
			case DmlMsg:
				key := msg.(DmlMsg).Hashkey
				n := key % uint32(r.c.WorkerCount)
				c, ok := r.mCh[int(n)]
				if !ok {
					log.Errorf("chan is lost: %v", n)
					r.ctx.Done()
				}
				c <- msg
			case DdlMsg:
				for _, c := range r.mCh {
					c <- DdlBarrierMsg{}
				}
				for _, _ = range r.mCh {
					<-r.barrierCh
				}
				sql := msg.(DdlMsg).Sql
				err := ddlexec(db, &sql)
				if err != nil {
					if strings.Contains(err.Error(), "Duplicate column name") || strings.Contains(err.Error(), "Duplicate key name") {
						log.Infof("ddl execute duplicate sql=%v \n %v", sql, err)
					} else {
						log.Errorf("ddl execute error sql=%v \n %v", sql, err)
						r.m2sCh <- DdlBarrierMsg{}
						return
					}
				}
				log.Debugf("ManagerLoop ddl %v", sql)
				if msg.(DdlMsg).Pos != nil {
					m := SaveBarrierMsg{
						Force: true,
						Pos:   *(msg.(DdlMsg).Pos),
					}
					r.saveCh <- m
				}
				r.m2sCh <- DdlBarrierMsg{}
			case SaveBarrierMsg:
				for _, c := range r.mCh {
					c <- msg
				}
			}
		}
	}

}

/**
task thread:If DdlBarrierMsg sent by the manager thread is received, the cached dml operation is executed
immediately and then the manager is notified; if SaveBarrierMsg is received, the cached dml operation is immediately
executed and forwarded to the saver thread; if DmlMsg is received, it is cached and waiting to be executed.
*/

func (r *River) TaskLoop(recChan <-chan Msg, i int) {
	r.wg.Add(1)
	defer func() {
		log.Infof("task loop %v closed ", i)
		r.cancel()
		r.wg.Done()
	}()

	msgArr := []*DmlMsg{}
	lastSavedTime := time.Now()

	db, err := r.getConn()

	if err != nil {
		log.Errorf("TaskLoop %v connnect error \n%v", i, err)
		return
	}

	exec := func(db *sql.DB, msgArr *[]*DmlMsg) error {
		if len(*msgArr) > 0 {
			err := BatchInsert(db, msgArr)
			if err != nil {
				return err
			}
			*msgArr = []*DmlMsg{}
			lastSavedTime = time.Now()
		}
		return nil
	}

	for {
		select {
		case <-r.Ctx().Done():
			return
		case <-time.After(3 * time.Second):
			// prevent long time no message causes cached sql not executed
			now := time.Now()
			if now.Sub(lastSavedTime) > 1*time.Second {
				err := exec(db, &msgArr)
				if err != nil {
					log.Errorf("dml execute error \n %v", err)
					return
				}
			}
		case msg := <-recChan:
			switch msg.(type) {
			// ddl Barrier send to manager
			case DdlBarrierMsg:
				err = exec(db, &msgArr)
				if err != nil {
					log.Errorf("exec() error ! \n %v", err)
					return
				}
				r.barrierCh <- msg.(DdlBarrierMsg)
			// save point send to saver
			case SaveBarrierMsg:
				//set save point before the SQL is executed successfully
				err := exec(db, &msgArr)
				if err != nil {
					log.Errorf("dml execute error \n %v", err)
					return
				}
				r.saveCh <- msg.(SaveBarrierMsg)
			case DmlMsg:
				now := time.Now()
				tmsg := msg.(DmlMsg)
				log.Debugf("taskLoop dml %v %v", tmsg.SqlHead, tmsg.SqlVal)
				msgArr = append(msgArr, &tmsg)
				// exceed the batch size or exceed time, execute sql
				if len(msgArr) >= r.c.Batch || now.Sub(lastSavedTime) > 1*time.Second {
					err := exec(db, &msgArr)
					if err != nil {
						log.Errorf("dml execute error \n %v", err)
						return
					}
				}
			}
		}
	}

}

/**
saver thread: Receive savepoint information, if it is forced to save the information is executed immediately; if it
is normal information, it is cached, the same information receives the number of task threads then save savepoint.
*/
func (r *River) SaverLoop() {
	r.wg.Add(1)
	defer func() {
		log.Infof("SaverLoop closed")
		r.cancel()
		r.wg.Done()
	}()

	mBuf := make(map[int64]*saveInfo, mapBufferSize)

	lastSavedTime := time.Now()
	var pos mysql.Position

	for {
		needSavePos := false
		select {
		case <-r.Ctx().Done():
			return
		case msg := <-r.saveCh:
			if msg.Force {
				pos = msg.Pos
				lastSavedTime = time.Now()
				needSavePos = true
			} else {
				if _, ok := mBuf[msg.Flag]; !ok {
					mBuf[msg.Flag] = &saveInfo{1, msg.Pos}
				} else {
					mBuf[msg.Flag].add()
				}
				if mBuf[msg.Flag].count == r.c.WorkerCount {
					pos = mBuf[msg.Flag].pos
					delete(mBuf, msg.Flag)
					now := time.Now()
					if now.Sub(lastSavedTime) > 3*time.Second {
						lastSavedTime = now
						needSavePos = true
					}
				}
			}

		}
		if needSavePos {
			if err := r.master.Save(pos); err != nil {
				log.Errorf("save sync position %s err %v, close sync", pos, err)
				r.cancel()
				return
			} else {
				log.Debugf("save binlog_name = %v  pos = %v", pos.Name, pos.Pos)
			}
		}
	}

}
