package query

import (
	"fmt"
	"net/http"

	"github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"github.com/tobsdb/tobsdb/pkg"
)

func Create(table *builder.Table, data map[string]any) (map[string]any, error) {
	// TODO: use custom type with setter and getter methods
	row := make(map[string]any)
	for _, field := range table.Fields {
		input := data[field.Name]
		if field.IndexLevel() == builder.IndexLevelPrimary {
			if input != nil {
				return nil, NewQueryError(http.StatusForbidden, "primary key cannot be explicitly set")
			}
			continue
		}

		res, err := field.ValidateType(input, true)
		if err != nil {
			return nil, err
		}

		if _, ok := field.Properties[props.FieldPropRelation]; ok {
			err := validateRelation(table, field, nil, res)
			if err != nil {
				return nil, err
			}
		}

		if input != nil {
			err := validateUnique(table, field, res)
			if err != nil {
				return nil, err
			}
		}

		row[field.Name] = res
	}

	row[builder.SYS_PRIMARY_KEY] = table.CreateId()
	primary_key_field := table.PrimaryKey()
	if primary_key_field != nil {
		row[primary_key_field.Name] = row[builder.SYS_PRIMARY_KEY]
	}

	for _, index := range table.Indexes {
		field := table.Fields[index]
		value, ok := row[field.Name]
		if !ok {
			continue
		}
		index_map := table.IndexMap(index)
		index_map[formatIndexValue(value)] = row[builder.SYS_PRIMARY_KEY].(int)
	}

	return row, nil
}

func Update(table *builder.Table, row, data map[string]any) (map[string]any, error) {
	res := make(map[string]any)
	for field_name, field := range table.Fields {
		input, ok := data[field_name]
		if !ok {
			continue
		}

		if field.IndexLevel() == builder.IndexLevelPrimary {
			return nil, NewQueryError(http.StatusForbidden, "primary key cannot be updated")
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
					_v, err := field.ValidateType(v, true)
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
			v, err := field.ValidateType(input, false)
			if err != nil {
				return nil, err
			}
			field_data = v
		}

		if _, ok := field.Properties[props.FieldPropRelation]; ok {
			id := pkg.NumToInt(row[builder.SYS_PRIMARY_KEY])
			err := validateRelation(table, field, &id, field_data)
			if err != nil {
				return nil, err
			}
		}

		if input != nil {
			err := validateUnique(table, field, field_data)
			if err != nil {
				return nil, err
			}
		}

		res[field_name] = field_data
	}

	for _, index := range table.Indexes {
		field := table.Fields[index]

		old_value, ok := row[field.Name]
		if ok {
			delete(table.IndexMap(index), formatIndexValue(old_value))
		}

		value, ok := res[field.Name]
		if !ok {
			continue
		}

		index_map := table.IndexMap(index)
		index_map[formatIndexValue(value)] = pkg.NumToInt(row[builder.SYS_PRIMARY_KEY])
	}

	return res, nil
}

// Note: returns a nil value when no row is found(does not throw errow).
// Always make sure to account for this case
func FindUnique(table *builder.Table, where map[string]any) (map[string]any, error) {
	if len(where) == 0 {
		return nil, fmt.Errorf("Where constraints cannot be empty")
	}

	for _, index := range table.Indexes {
		if input, ok := where[index]; ok {
			var id int
			if table.Fields[index].IndexLevel() == builder.IndexLevelPrimary {
				id = pkg.NumToInt(input)
			} else {
				index_map := table.IndexMap(index)
				id, ok = index_map[formatIndexValue(input)]
				if !ok {
					return nil, nil
				}
			}

			found := table.Data(id)
			if found != nil && compareUtil(table, found, where) {
				return found, nil
			}

			return nil, nil
		}
	}

	if len(table.Indexes) > 0 {
		return nil, fmt.Errorf("Unique fields not included in findUnique request")
	} else {
		return nil, fmt.Errorf("Table does not have any unique fields")
	}
}

func Find(table *builder.Table, where map[string]any, allow_empty_where bool) ([]map[string]any, error) {
	return findManyUtil(table, where, allow_empty_where)
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
func FindWithArgs(table *builder.Table, args FindArgs, allow_empty_where bool) ([]map[string]any, error) {
	return findManyUtil(table, args.Where, allow_empty_where)
}

func Delete(table *builder.Table, row map[string]any) {
	delete(table.Rows(), pkg.NumToInt(row[builder.SYS_PRIMARY_KEY]))
}
