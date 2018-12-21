package river

import (
	"strings"

	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"regexp"
	"strconv"
	"time"
)

const (
	l_tinyint    = 4
	l_smallint   = 6
	l_mediumint  = 9
	l_int        = 11
	l_bigint     = 20
	l_char       = 1
	l_tinytext   = 255
	l_text       = 21846
	l_mediumtext = 16777215
	l_longtext   = 4294967295
	l_tinyblob   = 255
	l_blob       = 65535
	l_mediumblob = 16777215
	l_longblob   = 4294967295
)

var expNum = regexp.MustCompile("\\(([0-9]*)\\)")

func ruleKey(schema string, table string) string {
	return strings.ToLower(fmt.Sprintf("%s.%s", schema, table))
}

func isValidTables(tables []string) bool {
	if len(tables) > 1 {
		for _, table := range tables {
			if table == "*" {
				return false
			}
		}
	}
	return true
}

func buildTable(table string) string {
	if table == "*" {
		return "." + table
	}
	return table
}

func ddlexec(db *sql.DB, sql *string) error {
	var err error
	_, err = db.Exec(*sql)
	// connection refused： Retry 100 times
	for i := 0; i < retryNum && err != nil && strings.Contains(err.Error(), "connection refused"); i++ {
		time.Sleep(3 * time.Second)
		log.Infof("connect tidb retry 100 times: %v", i+1)
		_, err = db.Exec(*sql)
		if err == nil {
			log.Debugf("%v exec success !!!", sql)
		}
	}

	if err != nil && !strings.Contains(err.Error(), "connection refused") {
		log.Errorf("sql exec error sql = %v, err = %v", *sql, err)
	}

	return err
}

func BatchInsert(db *sql.DB, msgArr *[]*DmlMsg) error {

	var (
		tx  *sql.Tx
		err error
	)

	errFunc := func(i int, err error, sql string, v []interface{}) {
		if strings.Contains(err.Error(), "connection refused") {
			time.Sleep(3 * time.Second)
			log.Infof("connect tidb retry 100 times: %v", i+1)
		} else {
			log.Errorf("sql exec error sql = %v %v, error info = %v", sql, v, err)
		}

	}

	isFirst := true

	for i := 0; i < retryNum && (isFirst || err != nil && (strings.Contains(err.Error(), "connection refused") ||
		strings.Contains(err.Error(), "bad connection"))); i++ {
		isFirst = false
		tx, err = db.Begin()
		if err != nil {
			errFunc(i, err, "begin", []interface{}{})
			continue
		}
		for _, msg := range *msgArr {
			log.Debugf("exec dml sql = %v %v", msg.SqlHead, msg.SqlVal)
			_, err = tx.Exec(msg.SqlHead, msg.SqlVal...)
			if err != nil {
				errFunc(i, err, msg.SqlHead, msg.SqlVal)
				break
			}
		}
		if err == nil {
			tx.Commit()
		}
	}
	return err
}

/**
Connect to the target database to get the type of each field of the corresponding table
*/
func fetchColType(db *sql.DB, schema, tb string) (map[string]string, error) {

	sql1 := fmt.Sprintf("show full columns from `%v`.`%v`", schema, tb)
	var res *sql.Rows
	var err error
	res, err = db.Query(sql1)
	for i := 0; i < retryNum && err != nil && (strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "doesn't exist")); i++ {
		time.Sleep(3 * time.Second)
		log.Infof("connect tidb retry 100 times: %v", i+1)
		res, err = db.Query(sql1)
		if err == nil {
			log.Debugf("%v exec success !!!", sql1)
		}
	}
	if err != nil && !strings.Contains(err.Error(), "connection refused") {
		// log
		log.Errorf("sql exec error sql = %v, error info = %v", sql1, err)
		return nil, err
	}

	m := map[string]string{}

	cloumns, _ := res.Columns()
	values := make([]sql.RawBytes, len(cloumns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for res.Next() {
		err = res.Scan(scanArgs...)
		var k, v string
		var value string
		for i, col := range values {
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			if cloumns[i] == "Field" {
				k = value
			} else if cloumns[i] == "Type" {
				v = value
			}
		}
		m[k] = v
	}

	return m, err

}

func isIntegerType(str string) bool {
	return strings.HasPrefix(str, "tinyint") ||
		strings.HasPrefix(str, "smallint") ||
		strings.HasPrefix(str, "mediumint") ||
		strings.HasPrefix(str, "int") ||
		strings.HasPrefix(str, "bigint")
}

func isCharType(str string) bool {
	return strings.HasPrefix(str, "char") ||
		strings.HasPrefix(str, "varchar") ||
		strings.HasPrefix(str, "text") ||
		strings.HasPrefix(str, "tinytext") ||
		strings.HasPrefix(str, "mediumtext") ||
		strings.HasPrefix(str, "longtext")
}

func isBlobType(str string) bool {
	return strings.HasPrefix(str, "tinyblob") ||
		strings.HasPrefix(str, "blob") ||
		strings.HasPrefix(str, "mediumblob") ||
		strings.HasPrefix(str, "longblob")
}

func IsSameType(lstr, rstr string) bool {
	return isIntegerType(lstr) && isIntegerType(rstr) ||
		isCharType(lstr) && isCharType(rstr) ||
		isBlobType(lstr) && isBlobType(rstr)
}

func TypeLen(typename string) (coltype string, strNum int, err error) {

	log.Debugf("TypeLen typename = %v", typename)

	fnAtoi := func(mb [][]byte) (int, error) {
		n, err := strconv.Atoi(string(mb[1]))
		if err != nil {
			err1 := errors.New(fmt.Sprintf("strconv.Atoi %v  err:%v", string(mb[1]), err))
			log.Error(err1.Error())
			return n, err1
		}
		return n, err
	}

	mb := expNum.FindSubmatch([]byte(typename))
	coltype = typename
	err = nil

	// Assign default values ​​based on type
	if strings.HasPrefix(typename, "tinyint") {
		strNum = l_tinyint
	} else if strings.HasPrefix(typename, "smallint") {
		strNum = l_smallint
	} else if strings.HasPrefix(typename, "mediumint") {
		strNum = l_mediumint
	} else if strings.HasPrefix(typename, "int") {
		strNum = l_int
	} else if strings.HasPrefix(typename, "bigint") {
		strNum = l_bigint
	} else if strings.HasPrefix(typename, "char") {
		strNum = l_char
	} else if strings.HasPrefix(typename, "varchar") {
		if len(mb) != 2 {
			log.Errorf("unknow error coltype = %v", typename)
			err = errors.New("varchar no len")
			return
		}
	} else if strings.HasPrefix(typename, "tinytext") {
		strNum = l_tinytext
	} else if strings.HasPrefix(typename, "text") {
		strNum = l_text
	} else if strings.HasPrefix(typename, "mediumtext") {
		strNum = l_mediumtext
	} else if strings.HasPrefix(typename, "longtext") {
		strNum = l_longtext
	} else if strings.HasPrefix(typename, "tinyblob") {
		strNum = l_tinyblob
	} else if strings.HasPrefix(typename, "blob") {
		strNum = l_text
	} else if strings.HasPrefix(typename, "mediumblob") {
		strNum = l_mediumblob
	} else if strings.HasPrefix(typename, "longblob") {
		strNum = l_longblob
	} else {
		log.Errorf("Regular parsing error ColType=%v", typename)
		err = errors.New("Regular parsing error")
	}

	if len(mb) == 2 {
		strNum, err = fnAtoi(mb)
		if err != nil {
			return
		}
	}

	// Handling special type
	if strings.HasPrefix(typename, "text") && len(mb) == 2 {
		n := strNum
		if n < l_tinytext {
			coltype = "tinytext"
			strNum = l_tinytext
		} else if n < l_text {
			coltype = "text"
			strNum = l_text
		} else if n < l_mediumtext {
			coltype = "mediumtext"
			strNum = l_mediumtext
		} else if n < l_longtext {
			coltype = "longtext"
			strNum = l_longtext
		} else {
			log.Errorf("unknow error coltype = %v, n = %v", typename, n)
			err = errors.New("unknow error")
			return
		}
	} else if strings.HasPrefix(typename, "blob") && len(mb) == 2 {
		n := strNum
		if n < l_tinyblob {
			coltype = "tinyblob"
			strNum = l_tinyblob
		} else if n < l_blob {
			coltype = "blob"
			strNum = l_blob
		} else if n < l_mediumblob {
			coltype = "mediumblob"
			strNum = l_mediumblob
		} else if n < l_longblob {
			coltype = "longblob"
			strNum = l_longblob
		} else {
			log.Errorf("unknow error coltype = %v, n = %v", typename, n)
			err = errors.New("unknow error")
			return
		}
	}
	return
}
