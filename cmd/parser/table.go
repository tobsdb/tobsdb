package parser

func (table *Table) Create(d map[string]any) (map[string]any, error) {
	row := make(map[string]any)
	for _, field := range table.fields {
		input := d[field.name]
		data, err := field.ValidateType(table.Name, input, true)
		if err != nil {
			return nil, err
		} else {
			row[field.name] = data
		}
	}
	return row, nil
}

func (table *Table) Find(where map[string]any) (map[string]any, error) {
	return nil, nil
}

var id_tracker int = 0

func CreateId() int {
	id_tracker++
	return id_tracker
}
