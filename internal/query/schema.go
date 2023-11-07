package query

import (
	"github.com/tobsdb/tobsdb/internal/parser"
)

type (
	// Maps row field name to its saved data
	TDBDataRow = map[string]any
	// Maps row id to its saved data
	TDBDataTable = map[int](TDBDataRow)
	// Maps table name to its saved data
	TDBData = map[string]TDBDataTable
)

type Schema struct {
	Tables map[string]*parser.Table
	Data   TDBData
}
