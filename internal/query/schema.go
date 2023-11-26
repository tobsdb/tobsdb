package query

import (
	"github.com/tobsdb/tobsdb/internal/parser"
)

type (
	// Maps row field name to its saved data
	TDBTableRow = map[string]any
	// Maps row id to its saved data
	TDBTableRows = map[int](TDBTableRow)
	// index field name -> index value -> row id
	TDBTableIndexes = map[string]map[string]int
	TDBTableData    struct {
		Rows    TDBTableRows
		Indexes TDBTableIndexes
	}
	// Maps table name to its saved data
	TDBData = map[string]*TDBTableData
)

type Schema struct {
	Tables map[string]*parser.Table
	// table_name -> row_id -> field_name -> value
	Data TDBData
}

const SYS_PRIMARY_KEY = "__tdb_id__"
