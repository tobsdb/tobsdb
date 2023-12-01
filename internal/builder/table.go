package builder

import "github.com/tobsdb/tobsdb/pkg"

type Table struct {
	Name      string
	Fields    pkg.Map[string, *Field]
	Indexes   []string
	IdTracker int

	Schema *Schema `json:"-"`
}

func (t *Table) PrimaryKey() *Field {
	for _, field := range t.Fields {
		if field.IndexLevel() == IndexLevelPrimary {
			return field
		}
	}
	return nil
}

func (t *Table) Rows() TDBTableRows {
	return t.Schema.Data.Get(t.Name).Rows
}

func (t *Table) Row(id int) TDBTableRow {
	return t.Schema.Data.Get(t.Name).Rows[id]
}

func (t *Table) IndexMap(index string) TDBTableIndexMap {
	return t.Schema.Data.Get(t.Name).Indexes.Get(index)
}
