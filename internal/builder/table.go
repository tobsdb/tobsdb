package builder

import (
	"sync/atomic"

	"github.com/tobsdb/tobsdb/pkg"
)

type Table struct {
	Name    string
	Fields  *pkg.InsertSortMap[string, *Field]
	Indexes []string

	IdTracker atomic.Int64

	Schema *Schema `json:"-"`
}

func (t *Table) PrimaryKey() *Field {
	for _, field := range t.Fields.Idx {
		if field.IndexLevel() == IndexLevelPrimary {
			return field
		}
	}
	return nil
}

func (t *Table) Data() *TDBTableData {
	return t.Schema.Data.Get(t.Name)
}

func (t *Table) Rows() *TDBTableRows {
	return t.Data().Rows
}

func (t *Table) Row(id int) TDBTableRow {
	v, ok := t.Data().Rows.Get(id)
	if !ok {
		return nil
	}
	return v
}

func (t *Table) IndexMap(index string) *TDBTableIndexMap {
	return t.Data().Indexes.Get(index)
}
