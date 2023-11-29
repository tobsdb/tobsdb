package builder

type Table struct {
	Name      string
	Fields    map[string]*Field
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
	return t.Schema.Data[t.Name].Rows
}

func (t *Table) Data(id int) TDBTableRow {
	return t.Schema.Data[t.Name].Rows[id]
}

func (t *Table) IndexMap(index string) map[string]int {
	if _, ok := t.Schema.Data[t.Name].Indexes[index]; !ok {
		t.Schema.Data[t.Name].Indexes[index] = make(map[string]int)
	}
	return t.Schema.Data[t.Name].Indexes[index]
}
