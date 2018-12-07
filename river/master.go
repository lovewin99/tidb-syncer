package river

import (
	"bytes"
	"os"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"github.com/siddontang/go/ioutil2"
	"tidb-syncer/utils/mysql"
)

/**
	Processing savepoints
 */

type masterInfo struct {
	sync.RWMutex

	Name string `toml:"binlog-name"`
	Pos  uint32 `toml:"binlog-pos"`
	Gtid string `toml:"binlog-gtid"`

	filePath     string
	lastSaveTime time.Time
}

/**
Read savepoint file
 */
func loadMasterInfo(dataDir string) (*masterInfo, error) {
	log.Debugf("dataDir = %v", dataDir)
	var m masterInfo

	if len(dataDir) == 0 {
		return &m, nil
	}

	m.filePath = dataDir

	f, err := os.Open(m.filePath)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return nil, errors.Trace(err)
	} else if os.IsNotExist(errors.Cause(err)) {
		return &m, nil
	}
	defer f.Close()

	_, err = toml.DecodeReader(f, &m)
	return &m, errors.Trace(err)
}

/**
write savepoint file
 */
func (m *masterInfo) Save(pos mysql.Position) error {
	log.Infof("save position %s", pos)

	m.Lock()
	defer m.Unlock()

	m.Name = pos.Name
	m.Pos = pos.Pos

	if len(m.filePath) == 0 {
		return nil
	}

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)

	e.Encode(m)

	var err error
	if err = ioutil2.WriteFileAtomic(m.filePath, buf.Bytes(), 0644); err != nil {
		log.Errorf("canal save master info to file %s err %v", m.filePath, err)
	}

	return errors.Trace(err)
}

func (m *masterInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()

	return mysql.Position{
		Name: m.Name,
		Pos:  m.Pos,
	}
}

func (m *masterInfo) Close() error {
	pos := m.Position()

	return m.Save(pos)
}
