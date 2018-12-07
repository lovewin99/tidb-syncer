package river

import (
	"tidb-syncer/utils/mysql"
)

/**
 Communication message
 sync / manage / task / save
 */

type Msg interface {

}

// priority  HighDdlMsg > DmlMsg > LowDdlMsg

type DmlMsg struct {
	Hashkey uint32
	SqlHead string
	SqlVal	[]interface{}
}

type DdlMsg struct {
	Sql string
	Pos *mysql.Position
}

type SaveBarrierMsg struct {
	Flag	int64
	Force	bool
	Pos		mysql.Position
}

type DdlBarrierMsg struct {
}