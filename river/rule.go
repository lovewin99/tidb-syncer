package river

import (
	//"strings"
	//"tidb-syncer/utils/schema"
	"tidb-syncer/utils/schema"
)


type Rule struct {
	Pschema	string	`toml:"pattern-schema"`
	Ptable	string	`toml:"pattern-table"`
	Tschema	string	`toml:"target-schema"`
	Ttable	string	`toml:"target-table"`

	SkipType string
	// MySQL table information
	TableInfo *schema.Table

	// TODO filter
	//only MySQL fields in filter will be synced , default sync all fields
	//Filter []string `toml:"filter"`
}

// Create a default rule based on the library name
func newDefaultRule(db string, table string) *Rule{
	r := &Rule{}

	r.Pschema = db
	r.Ptable = table

	r.Tschema = db
	r.Ttable = table

	r.SkipType = ""

	return r
}
