package river

import (
	"bytes"
	"fmt"

	log "github.com/sirupsen/logrus"
	"tidb-syncer/utils/canal"
	"tidb-syncer/utils/schema"
	"github.com/juju/errors"
	"hash/crc32"
)

/**
  generate insert/update/delete sql statement then packaging message
  the primary key field is hashed to ensure that the same piece of data is processed in one thread
 */

// generate insert sql then packaging message
func (r *River) makeInsertRequest(e *canal.RowsEvent) ([]*DmlMsg, error)  {

	rule, ok := r.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	PKColumns := rule.TableInfo.PKColumns
	if !ok{
		return nil, errors.Errorf("%s.%s not int rules", e.Table.Schema, e.Table.Name)
	}

	insertHead := fmt.Sprintf("replace into `%s`.`%s` %s values ", rule.Tschema, rule.Ttable, rule.TableInfo.ColNames)

	dmlMsgArr := []*DmlMsg{}
	for _, row := range e.Rows {
		var keybuf bytes.Buffer
		var buf bytes.Buffer
		buf.WriteString(insertHead)
		buf.WriteString("(")
		var insertVal []interface{}
		// Parse each field to generate a SQL statement
		for ii, v1 := range row {
			str, err := getStrByColType(e.Table.Columns[ii].Type, v1)
			// If there is no primary key, generate a hash key according to the first field.
			if len(PKColumns) == 0 && ii == 0 {
				keybuf.WriteString(str)
			}
			for _, v := range PKColumns {
				if v == ii {
					keybuf.WriteString(str)
					break
				}
			}
			if err != nil{
				return nil, err
			}

			if e.Table.Columns[ii].Type == schema.TYPE_BIT || v1 == nil{
				buf.WriteString(str)
			}else{
				buf.WriteString("?")
				insertVal = append(insertVal, str)
			}

			if ii < len(row) -1{
				buf.WriteString(",")
			}
		}
		buf.WriteString(")")
		buf.WriteString(";")

		// generate hash key
		key := crc32.ChecksumIEEE(keybuf.Bytes())
		dmlMsgArr = append(dmlMsgArr, &DmlMsg{key, buf.String(), insertVal})
	}

	return dmlMsgArr, nil

}

/**
generate update one sql statement then packaging message
head: update `xxx`.`xxx`
oldIndex: old data line number
newIndex: update data line number
setIndexs: self-generated field number
index: primary key field number
e: data
rule: the table rule
 */
func (r *River) getUpdateSql(head string, oldIndex int, newIndex int, setIndexs map[int]int, indexs map[int]int,
	e *canal.RowsEvent, rule *Rule) (*DmlMsg, error)  {

	colnames := rule.TableInfo.Columns

	whereStr := "where "
	setStr := "set "

	var setBuf bytes.Buffer
	setBuf.WriteString(setStr)
	var whereBuf bytes.Buffer
	whereBuf.WriteString(whereStr)
	var keyBuf bytes.Buffer

	var setVal, whereVal []interface{}

	setFunc := func(name string, value string, t int) {
		if setBuf.Len() != len(setStr){
			setBuf.WriteString(",")
		}
		setBuf.WriteString("`")
		setBuf.WriteString(name)
		setBuf.WriteString("`")
		setBuf.WriteString("=")
		if t == schema.TYPE_BIT || value == "null"{
			setBuf.WriteString(value)
		}else{
			setBuf.WriteString("?")
			setVal = append(setVal, value)
		}
	}

	// Parse each field to generate a SQL statement
	for i, col := range colnames{
		if len(indexs) == 0 {
			// not exist primary key
			oldvalue, err := getStrByColType(col.Type, e.Rows[oldIndex][i])
			if err != nil{
				return nil, err
			}
			if i == 0 {
				keyBuf.WriteString(oldvalue)
			}

			newvalue, err := getStrByColType(col.Type, e.Rows[newIndex][i])
			if err != nil{
				return nil, err
			}

			// Each field is used as a condition when there is no index
			if whereBuf.Len() != len(whereStr){
				whereBuf.WriteString(" and ")
			}
			whereBuf.WriteString("`")
			whereBuf.WriteString(col.Name)
			whereBuf.WriteString("`")
			if e.Rows[oldIndex][i] == nil{
				whereBuf.WriteString(" is ")
			}else{
				whereBuf.WriteString("=")
			}

			if col.Type == schema.TYPE_BIT {
				whereBuf.WriteString(oldvalue)
			}else{
				if e.Rows[oldIndex][i] == nil{
					whereBuf.WriteString("null")
				}else{
					whereBuf.WriteString("?")
					whereVal = append(whereVal, oldvalue)
				}
			}

			// Set new value if the new value of each field is not equal to the old value or is a self-generated field
			if _, ok := setIndexs[i]; ok{
				setFunc(col.Name, newvalue, col.Type)
			}else if oldvalue != newvalue{
				setFunc(col.Name, newvalue, col.Type)
			}
		}else{
			// exist primary key

			oldvalue, err := getStrByColType(col.Type, e.Rows[oldIndex][i])
			if err != nil{
				return nil, err
			}
			newvalue, err := getStrByColType(col.Type, e.Rows[newIndex][i])
			if err != nil{
				return nil, err
			}
			// Index field as condition
			if _, ok := indexs[i]; ok{
				if whereBuf.Len() != len(whereStr){
					whereBuf.WriteString(" and ")
				}
				whereBuf.WriteString("`")
				whereBuf.WriteString(col.Name)
				whereBuf.WriteString("`")

				if e.Rows[oldIndex][i] == nil{
					whereBuf.WriteString(" is ")
				}else{
					whereBuf.WriteString("=")
				}

				if col.Type == schema.TYPE_BIT{
					whereBuf.WriteString(oldvalue)
				}else{
					if e.Rows[oldIndex][i] == nil{
						whereBuf.WriteString("null")
					}else{
						whereBuf.WriteString("?")
						whereVal = append(whereVal, oldvalue)
					}
				}

				keyBuf.WriteString(oldvalue)
			}
			// Set new value if the new value of each field is not equal to the old value or is a self-generated field
			if oldvalue == newvalue{
				if _, ok := setIndexs[i]; ok{
					setFunc(col.Name, newvalue, col.Type)
				}
			}else{
				setFunc(col.Name, newvalue, col.Type)
			}
		}
	}

	if setBuf.Len() == len(setStr) || whereBuf.Len() == len(whereStr){
		log.Errorf("update set || where is null \n old row = %v \n new row = %v \n", e.Rows[oldIndex], e.Rows[newIndex])
		return nil, nil
	}

	// generate hash key
	key := crc32.ChecksumIEEE(keyBuf.Bytes())

	return &DmlMsg{key, head + setBuf.String() + " " + whereBuf.String() + ";", append(setVal, whereVal...)}, nil
}

// generate update slice of sql statement then packaging message
func (r *River) makeUpdateRequest(e *canal.RowsEvent) ([]*DmlMsg, error) {

	if len(e.Rows)%2 != 0 {
		return nil, errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(e.Rows))
	}

	rule, ok := r.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	if !ok{
		return nil, errors.Errorf("%s.%s not int rules", e.Table.Schema, e.Table.Name)
	}

	// get primary key field as where conditions
	indexsArr := rule.TableInfo.PKColumns
	// get column names
	colnames := rule.TableInfo.Columns

	setIndexs := make(map[int]int, len(colnames))

	for i, col := range colnames{
		if col.IsAuto{
			setIndexs[i] = 0
		}
	}

	indexs := make(map[int]int, len(indexsArr))
	for _, v := range indexsArr{
		indexs[v] = 0
	}

	updateHead := fmt.Sprintf("update `%s`.`%s` ", rule.Tschema, rule.Ttable)

	dmlMsgArr := []*DmlMsg{}

	for i := 1; i < len(e.Rows); i += 2{

		oArr, err := r.getUpdateSql(updateHead, i-1, i, setIndexs, indexs, e, rule)
		if err != nil{
			return nil, err
		}
		if oArr != nil {
			dmlMsgArr = append(dmlMsgArr, oArr)
		}
	}

	return dmlMsgArr, nil
}

// generate delete slice of sql statement then packaging message
func (r *River) makeDeleteRequest(e *canal.RowsEvent) ([]*DmlMsg, error) {
	rule, ok := r.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	if !ok{
		return nil, errors.Errorf("%s.%s not int rules", e.Table.Schema, e.Table.Name)
	}

	sqlHead := fmt.Sprintf("delete from `%s`.`%s` where ", rule.Tschema, rule.Ttable)

	dmlMsgArr, err := r.GetDelSql(sqlHead, rule, e)

	if err != nil{
		return nil, err
	}

	return dmlMsgArr, nil
}

// Returns the correct field form in the sql statement based on the field type
func getStrByColType(t int, v interface{}) (string, error) {
	switch t {
	case schema.TYPE_NUMBER, schema.TYPE_FLOAT, schema.TYPE_ENUM, schema.TYPE_SET:
		if v == nil {
			return "null", nil
		}else{
			return fmt.Sprintf("%v", v), nil
		}
	case schema.TYPE_STRING, schema.TYPE_DATE, schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP, schema.TYPE_TIME:
		if v == nil{
			return "null", nil
		}else{
			return fmt.Sprintf("%s", v.(string)), nil
		}
	case schema.TYPE_JSON:
		if v == nil{
			return "null", nil
		}else{
			return fmt.Sprintf("%s", string(v.([]uint8))), nil
		}
	case schema.TYPE_BIT:
		if v == nil{
			return "null", nil
		}else{
			return fmt.Sprintf("b'%b'", v), nil
		}
	default:
		return "", errors.Errorf("unknow colomn type %v\n", t)
	}
}

// generate delete slice of sql statement then packaging message
func (r *River) GetDelSql(head string, rule *Rule, e *canal.RowsEvent) ([]*DmlMsg, error) {

	indexs := rule.TableInfo.PKColumns
	colnames := rule.TableInfo.Columns

	dmlMsgArr := []*DmlMsg{}

	if len(indexs) == 0 {
		//strSet := make(map[string]int, len(e.Rows))
		for _, row := range e.Rows {
			var whereVal []interface{}
			var keybuf bytes.Buffer
			var buf bytes.Buffer
			buf.WriteString(head)
			buf.WriteString("(")
			for ii, col := range colnames {
				str, err := getStrByColType(col.Type, row[ii])
				if ii == 0 {
					keybuf.WriteString(str)
				}
				if err != nil {
					return nil, err
				}
				if col.Type == schema.TYPE_BIT{
					buf.WriteString(fmt.Sprintf("`%s` = %s", col.Name, str))
				}else{
					if row[ii] == nil{
						buf.WriteString(fmt.Sprintf("`%s` is null", col.Name))
					}else{
						buf.WriteString(fmt.Sprintf("`%s` = %s", col.Name, "?"))
						whereVal = append(whereVal, str)
					}

				}

				if ii != len(colnames)-1 {
					buf.WriteString(" and ")
				}
			}
			buf.WriteString(");")
			key := crc32.ChecksumIEEE(keybuf.Bytes())
			dmlMsgArr = append(dmlMsgArr, &DmlMsg{key, buf.String(), whereVal})
		}
	} else {
		for _, row := range e.Rows {
			var whereVal []interface{}
			var buf bytes.Buffer
			var keybuf bytes.Buffer
			buf.WriteString(head)
			buf.WriteString("(")
			for ii, index := range indexs {
				str, err := getStrByColType(e.Table.Columns[index].Type, row[index])
				keybuf.WriteString(str)
				if err != nil {
					return nil, err
				}
				if e.Table.Columns[index].Type == schema.TYPE_BIT {
					buf.WriteString(fmt.Sprintf("`%s` = %s", colnames[index].Name, str))
				}else{
					if row[index] == nil {
						buf.WriteString(fmt.Sprintf("`%s`is null", colnames[index].Name))
					}else{
						buf.WriteString(fmt.Sprintf("`%s` = %s", colnames[index].Name, "?"))
						whereVal = append(whereVal, str)
					}
				}

				if ii != len(indexs)-1 {
					buf.WriteString(" and ")
				}
			}
			buf.WriteString(");")
			key := crc32.ChecksumIEEE(keybuf.Bytes())
			dmlMsgArr = append(dmlMsgArr, &DmlMsg{key, buf.String(), whereVal})
		}
	}

	return dmlMsgArr, nil

}