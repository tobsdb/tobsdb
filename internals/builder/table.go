package builder

import (
	"fmt"

	"github.com/tobshub/tobsdb/internals/parser"
	"github.com/tobshub/tobsdb/internals/types"
	"github.com/tobshub/tobsdb/pkg"
	"golang.org/x/exp/slices"
)

func (schema *Schema) Create(t_schema *parser.Table, data map[string]any) (map[string]any, error) {
	row := make(map[string]any)
	for _, field := range t_schema.Fields {
		input := data[field.Name]
		res, err := t_schema.ValidateType(&field, input, true)
		if err != nil {
			return nil, err
		} else {
			if _, ok := field.Properties[types.FieldPropRelation]; ok {
				err := schema.validateRelation(&field, res)
				if err != nil {
					return nil, err
				}
			}
			row[field.Name] = res
		}
	}
	return row, nil
}

func DynamicUpdateVectorField(field, row, input map[string]any) error {
	return nil
}

// TODO: dynamic updates
//
//	eg for vectors: push
//	eg for number: increment, decrement
//	eg for string: append
func (schema *Schema) Update(t_schema *parser.Table, row, data map[string]any) error {
	field := schema.Data[t_schema.Name][pkg.NumToInt(row["id"])]
	for field_name, input := range data {
		f := t_schema.Fields[field_name]

		switch input := input.(type) {
		case map[string]any:
			switch f.BuiltinType {
			case types.FieldTypeVector:
				// FIXIT: make this more dynamic
				to_push := input["push"].([]any)
				field[field_name] = append(field[field_name].([]any), to_push...)
			}
		default:
			res, err := t_schema.ValidateType(&f, input, false)
			if err != nil {
				return err
			}

			if _, ok := f.Properties[types.FieldPropRelation]; ok {
				err := schema.validateRelation(&f, res)
				if err != nil {
					return err
				}
			}

			field[field_name] = res
		}
	}
	return nil
}

// Note to self: returns a nil value when no row is found(does not throw errow).
// Always make sure to account for this case
func (schema *Schema) FindUnique(t_schema *parser.Table, where map[string]any) (map[string]any, error) {
	if len(where) == 0 {
		return nil, fmt.Errorf("Where constraints cannot be empty")
	}

	for _, index := range t_schema.Indexes {
		if input, ok := where[index]; ok {
			found := schema.filterRows(t_schema, index, input, true)
			if len(found) > 0 {
				return found[0], nil
			}

			return nil, nil
		}
	}

	if len(t_schema.Indexes) > 0 {
		return nil, fmt.Errorf("Unique fields not included in findUnique request")
	} else {
		return nil, fmt.Errorf("Table does not have any unique fields")
	}
}

func (schema *Schema) Find(t_schema *parser.Table, where map[string]any, allow_empty_where bool) ([]map[string]any, error) {
	found_rows := [](map[string]any){}
	contains_index := false

	if allow_empty_where && len(where) == 0 {
		// nil comparison works here
		found_rows = schema.filterRows(t_schema, "", nil, false)
		return found_rows, nil
	} else if len(where) == 0 {
		return nil, fmt.Errorf("Where constraints cannot be empty")
	}

	// filter with indexes first
	for _, index := range t_schema.Indexes {
		if input, ok := where[index]; ok {
			contains_index = true
			if len(found_rows) > 0 {
				found_rows = pkg.Filter(found_rows, func(row map[string]any) bool {
					s_field := t_schema.Fields[index]
					return t_schema.Compare(&s_field, row[index], input)
				})
			} else {
				found_rows = schema.filterRows(t_schema, index, where[index], false)
			}
		}
	}

	// filter with non-indexes
	if len(found_rows) > 0 {
		for field_name := range t_schema.Fields {
			if !slices.Contains(t_schema.Indexes, field_name) {
				if input, ok := where[field_name]; ok {
					found_rows = pkg.Filter(found_rows, func(row map[string]any) bool {
						s_field := t_schema.Fields[field_name]
						return t_schema.Compare(&s_field, row[field_name], input)
					})
				}
			}
		}
	} else if !contains_index {
		for field_name := range t_schema.Fields {
			if input, ok := where[field_name]; ok {
				found_rows = schema.filterRows(t_schema, field_name, input, false)
			}
		}
	}

	return found_rows, nil
}

func (schema *Schema) Delete(t_schema *parser.Table, row map[string]any) {
	delete(schema.Data[t_schema.Name], pkg.NumToInt(row["id"]))
}
