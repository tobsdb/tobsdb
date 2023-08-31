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
		res, err := field.ValidateType(schema.Name, input, true)
		if err != nil {
			return nil, err
		} else {
			row[field.Name] = res
		}
	}
	return row, nil
}

func (db *TobsDB) Update(schema *parser.Table, where, data map[string]any) ([]map[string]any, error) {
	found, err := db.Find(schema, where)
	if err != nil {
		return nil, err
	}

	for _, row := range found {
		field := db.data[schema.Name][int(row["id"].(float64))]
		for field_name, input := range data {
			f := schema.Fields[field_name]
			res, err := f.ValidateType(input, false)
			if err != nil {
				return nil, err
			}
			field[field_name] = res
		}
	}

	return found, err
}

func (db *TobsDB) Find(schema *parser.Table, where map[string]any) ([]map[string]any, error) {
	found_rows := [](map[string]any){}
	contains_index := false

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

func (db *TobsDB) Delete(schema *parser.Table, where map[string]any) (int, error) {
	delete_count := 0

	found, err := db.Find(schema, where)
	if err != nil {
		return 0, err
	}

	for _, row := range found {
		delete(db.data[schema.Name], int(row["id"].(float64)))
		delete_count++
	}

	return delete_count, nil
}
