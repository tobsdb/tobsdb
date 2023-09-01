package builder

import (
	"fmt"

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
	field := db.data[schema.Name][pkg.NumToInt(row["id"])]
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

// Note to self: returns a nil value when no row is found(does not throw errow).
// Always make sure to account for this case
func (db *TobsDB) FindUnique(schema *parser.Table, where map[string]any) (map[string]any, error) {
	if len(where) == 0 {
		return nil, fmt.Errorf("Where contraints cannot be empty")
	}

	for _, index := range schema.Indexes {
		if input, ok := where[index]; ok {
			found := db.FilterRows(schema, index, input, true)
			if len(found) > 0 {
				return found[0], nil
			} else {
				return nil, nil
			}
		}
	}

	if len(schema.Indexes) > 0 {
		return nil, fmt.Errorf("Unique fields not included in findUnique request")
	} else {
		return nil, fmt.Errorf("Table does not have any unique fields")
	}
}

func (db *TobsDB) Find(schema *parser.Table, where map[string]any, allow_empty_where bool) ([]map[string]any, error) {
	found_rows := [](map[string]any){}
	contains_index := false

	if allow_empty_where && len(where) == 0 {
		// nil comparison works here
		found_rows = db.FilterRows(schema, "", nil, false)
		return found_rows, nil
	} else if len(where) == 0 {
		return nil, fmt.Errorf("Where contraints cannot be empty")
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
				found_rows = db.FilterRows(schema, index, where[index], false)
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
				found_rows = db.FilterRows(schema, field_name, input, false)
			}
		}
	}

	return found_rows, nil
}

func (db *TobsDB) Delete(schema *parser.Table, row map[string]any) {
	delete(db.data[schema.Name], pkg.NumToInt(row["id"]))
}
