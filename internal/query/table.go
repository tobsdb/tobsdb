package query

import (
	"fmt"

	"github.com/tobsdb/tobsdb/internal/parser"
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"github.com/tobsdb/tobsdb/pkg"
)

func (schema *Schema) Create(t_schema *parser.Table, data map[string]any) (map[string]any, error) {
	row := make(map[string]any)
	for _, field := range t_schema.Fields {
		input := data[field.Name]
		res, err := t_schema.ValidateType(field, input, true)
		if err != nil {
			return nil, err
		}

		if _, ok := field.Properties[props.FieldPropRelation]; ok {
			err := schema.validateRelation(t_schema.Name, field, nil, res)
			if err != nil {
				return nil, err
			}
		}

		if input != nil {
			err := schema.validateUnique(t_schema, field, res)
			if err != nil {
				return nil, err
			}
		}

		row[field.Name] = res
	}

	for _, index := range t_schema.Indexes {
		field := t_schema.Fields[index]
		if field.IndexLevel() == parser.IndexLevelPrimary {
			continue
		}

		value, ok := row[field.Name]
		if !ok {
			continue
		}
		index_map := schema.Data[t_schema.Name].Indexes[index]
		if index_map == nil {
			index_map = make(map[string]int)
		}
		index_map[formatIndexValue(value)] = pkg.NumToInt(row["id"])
		schema.Data[t_schema.Name].Indexes[index] = index_map
	}

	return row, nil
}

func (schema *Schema) Update(t_schema *parser.Table, row, data map[string]any) (map[string]any, error) {
	res := make(map[string]any)
	for field_name, field := range t_schema.Fields {
		input, ok := data[field_name]

		if !ok {
			continue
		}

		field_data := row[field_name]

		switch input := input.(type) {
		case map[string]any:
			switch field.BuiltinType {
			case types.FieldTypeVector:
				// TODO: make this more dynamic
				to_push := input["push"].([]any)
				field_data = append(field_data.([]any), to_push...)
			case types.FieldTypeInt:
				for k, v := range input {
					_v, err := t_schema.ValidateType(field, v, true)
					if err != nil {
						return nil, err
					}

					v := _v.(int)
					switch k {
					case "increment":
						field_data = field_data.(int) + v
					case "decrement":
						field_data = field_data.(int) - v
					}
				}
			}
		default:
			v, err := t_schema.ValidateType(field, input, false)
			if err != nil {
				return nil, err
			}
			field_data = v
		}

		if _, ok := field.Properties[props.FieldPropRelation]; ok {
			id := row["id"].(int)
			err := schema.validateRelation(t_schema.Name, field, &id, field_data)
			if err != nil {
				return nil, err
			}
		}

		if input != nil {
			err := schema.validateUnique(t_schema, field, field_data)
			if err != nil {
				return nil, err
			}
		}

		res[field_name] = field_data
	}

	for _, index := range t_schema.Indexes {
		field := t_schema.Fields[index]
		if field.IndexLevel() == parser.IndexLevelPrimary {
			continue
		}

		old_value, ok := row[field.Name]
		if ok {
			delete(schema.Data[t_schema.Name].Indexes[index], formatIndexValue(old_value))
		}

		value, ok := res[field.Name]
		if !ok {
			continue
		}

		index_map := schema.Data[t_schema.Name].Indexes[index]
		if index_map == nil {
			index_map = make(map[string]int)
		}
		index_map[formatIndexValue(value)] = pkg.NumToInt(row["id"])
		schema.Data[t_schema.Name].Indexes[index] = index_map
	}

	return res, nil
}

// Note: returns a nil value when no row is found(does not throw errow).
// Always make sure to account for this case
func (schema *Schema) FindUnique(t_schema *parser.Table, where map[string]any) (map[string]any, error) {
	if len(where) == 0 {
		return nil, fmt.Errorf("Where constraints cannot be empty")
	}

	for _, index := range t_schema.Indexes {
		if input, ok := where[index]; ok {
			found := schema.findFirst(t_schema, index, input)
			if found != nil && compareUtil(t_schema, found, where) {
				return found, nil
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
	return findManyUtil(schema, t_schema, where, allow_empty_where)
}

type FindArgs struct {
	Where   map[string]any
	Take    map[string]int
	OrderBy map[string]string
	Cursor  map[string]int
}

// TODO: support "take" & "order_by" & "cursor" options
//
// take can only be used when order_by is used
// and cursor can only be used when take is used
func (schema *Schema) FindWithArgs(t_schema *parser.Table, args FindArgs, allow_empty_where bool) ([]map[string]any, error) {
	return findManyUtil(schema, t_schema, args.Where, allow_empty_where)
}

func (schema *Schema) Delete(t_schema *parser.Table, row map[string]any) {
	delete(schema.Data[t_schema.Name].Rows, pkg.NumToInt(row["id"]))
}
