package builder

import (
	"github.com/tobshub/tobsdb/cmd/parser"
	"github.com/tobshub/tobsdb/pkg"
	"golang.org/x/exp/slices"
)

func (db *TobsDB) Create(schema *parser.Table, data map[string]any) (map[string]any, error) {
	row := make(map[string]any)
	for _, field := range schema.Fields {
		input := data[field.Name]
		res, err := field.ValidateType(schema, input, true)
		if err != nil {
			return nil, err
		} else {
			row[field.Name] = res
		}
	}
	return row, nil
}

func (db *TobsDB) Update(schema *parser.Table, row, data map[string]any) error {
	field := db.data[schema.Name][row["id"].(int)]
	for field_name, input := range data {
		f := schema.Fields[field_name]
		res, err := f.ValidateType(schema, input, false)
		if err != nil {
			return err
		}
		field[field_name] = res
	}
	return nil
}

func (db *TobsDB) Find(schema *parser.Table, where map[string]any, allow_empty_where bool) ([]map[string]any, error) {
	found_rows := [](map[string]any){}
	contains_index := false

	if allow_empty_where && len(where) == 0 {
		// nil comparison works here
		found_rows = db.FilterRows(schema, "", nil)
		return found_rows, nil
	}

	// filter with indexes first
	for _, index := range schema.Indexes {
		if input, ok := where[index]; ok {
			contains_index = true
			if len(found_rows) > 0 {
				found_rows = pkg.Filter(found_rows, func(row map[string]any) bool {
					return row[index] == input
				})
			} else {
				found_rows = db.FilterRows(schema, index, where[index])
			}
		}
	}

	// filter with non-indexes
	if len(found_rows) > 0 {
		for field_name := range schema.Fields {
			if !slices.Contains(schema.Indexes, field_name) {
				if input, ok := where[field_name]; ok {
					found_rows = pkg.Filter(found_rows, func(row map[string]any) bool {
						return row[field_name] == input
					})
				}
			}
		}
	} else if !contains_index {
		for field_name := range schema.Fields {
			if input, ok := where[field_name]; ok {
				found_rows = db.FilterRows(schema, field_name, input)
			}
		}
	}

	return found_rows, nil
}

func (db *TobsDB) Delete(schema *parser.Table, row, where map[string]any) {
	delete(db.data[schema.Name], int(row["id"].(float64)))
}
