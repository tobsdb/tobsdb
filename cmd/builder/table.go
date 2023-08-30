package builder

import (
	"github.com/tobshub/tobsdb/cmd/parser"
	"github.com/tobshub/tobsdb/pkg"
	"golang.org/x/exp/slices"
)

func (db *TobsDB) Create(table *parser.Table, d map[string]any) (map[string]any, error) {
	row := make(map[string]any)
	for _, field := range table.Fields {
		input := d[field.Name]
		data, err := field.ValidateType(table.Name, input, true)
		if err != nil {
			return nil, err
		} else {
			row[field.Name] = data
		}
	}
	return row, nil
}

func (db *TobsDB) Find(table *parser.Table, where map[string]any) ([]map[string]any, error) {
	found_rows := [](map[string]any){}
	contains_index := false

	// filter with indexes first
	for _, index := range table.Indexes {
		if input, ok := where[index]; ok {
			contains_index = true
			if len(found_rows) > 0 {
				found_rows = pkg.Filter(found_rows, func(row map[string]any) bool {
					return row[index] == input
				})
			} else {
				found_rows = db.FilterRows(table.Name, index, where[index])
			}
		}
	}

	// filter with non-indexes
	if len(found_rows) > 0 {
		for field_name := range table.Fields {
			if !slices.Contains(table.Indexes, field_name) {
				if input, ok := where[field_name]; ok {
					found_rows = pkg.Filter(found_rows, func(row map[string]any) bool {
						return row[field_name] == input
					})
				}
			}
		}
	} else if !contains_index {
		for field_name := range table.Fields {
			if input, ok := where[field_name]; ok {
				found_rows = db.FilterRows(table.Name, field_name, input)
			}
		}
	}

	return found_rows, nil
}
