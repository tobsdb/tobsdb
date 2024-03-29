package query

import (
	"fmt"
	"net/http"
	"slices"

	"github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"github.com/tobsdb/tobsdb/pkg"
)

type QueryArg map[string]any

func (q QueryArg) Has(key string) bool {
	_, ok := q[key]
	return ok
}

func (q QueryArg) Get(key string) any {
	val, ok := q[key]
	if !ok {
		return nil
	}
	return val
}

func Create(table *builder.Table, data QueryArg) (builder.TDBTableRow, error) {
	row := make(builder.TDBTableRow)
	for _, field := range table.Fields.Idx {
		input := data.Get(field.Name)
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

		if field.Properties.Has(props.FieldPropRelation) {
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

		row.Set(field.Name, res)
	}

	primary_key := table.CreateId()
	builder.SetPrimaryKey(row, primary_key)
	primary_key_field := table.PrimaryKey()
	if primary_key_field != nil {
		row.Set(primary_key_field.Name, primary_key)
	}

	for _, index := range table.Indexes {
		field := table.Fields.Get(index)
		if field.IndexLevel() == builder.IndexLevelPrimary {
			continue
		}
		value := row.Get(field.Name)
		if value == nil {
			continue
		}
		table.IndexMap(index).Set(value, primary_key)
	}

	table.Rows().Insert(primary_key, row)
	return row, nil
}

func Update(table *builder.Table, row builder.TDBTableRow, data QueryArg) (builder.TDBTableRow, error) {
	res := make(builder.TDBTableRow)
	for _, field := range table.Fields.Idx {
		if !data.Has(field.Name) {
			continue
		}

		if field.IndexLevel() == builder.IndexLevelPrimary {
			return nil, NewQueryError(http.StatusForbidden, "primary key cannot be updated")
		}

		input := data.Get(field.Name)

		field_data := row.Get(field.Name)

		switch input := input.(type) {
		case map[string]any:
			switch field.BuiltinType {
			case types.FieldTypeVector:
				if field_data == nil {
					field_data = []any{}
				}
				for k, v := range input {
					_v, err := field.ValidateType(v, true)
					if err != nil {
						return nil, err
					}

					v := _v.([]any)
					// TODO: consider pop, shift, unshift
					switch k {
					case "push":
						field_data = append(field_data.([]any), v...)
					}
				}
			case types.FieldTypeInt:
				if field_data == nil {
					field_data = 0
				}
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

		if field.Properties.Has(props.FieldPropRelation) {
			id := builder.GetPrimaryKey(row)
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

		res.Set(field.Name, field_data)
	}

	primary_key := builder.GetPrimaryKey(row)
	res = pkg.Map[string, any](pkg.MergeMaps(row, res))
	for _, index := range table.Indexes {
		field := table.Fields.Get(index)
		if field.IndexLevel() == builder.IndexLevelPrimary {
			continue
		}

		old_value := row.Get(field.Name)
		if old_value != nil {
			table.IndexMap(index).Delete(old_value)
		}

		value := res.Get(field.Name)
		if value == nil {
			continue
		}

		table.IndexMap(index).Set(value, primary_key)
	}

	table.Rows().Replace(primary_key, res)
	return res, nil
}

// Note: returns a nil value when no row is found(does not throw errow).
// Always make sure to account for this case
func FindUnique(table *builder.Table, where QueryArg) (builder.TDBTableRow, error) {
	if len(where) == 0 {
		return nil, fmt.Errorf("Where constraints cannot be empty")
	}

	for _, index := range table.Indexes {
		if !where.Has(index) {
			continue
		}

		input := where.Get(index)
		var id int
		if table.Fields.Get(index).IndexLevel() == builder.IndexLevelPrimary {
			id = pkg.NumToInt(input)
		} else {
			index_map := table.IndexMap(index)
			if !index_map.Has(input) {
				return nil, NewQueryError(404, fmt.Sprintf("No row found with constraint %v in table %s", where, table.Name))
			}
			id = pkg.NumToInt(index_map.Get(input))
		}

		found := table.Row(id)
		if found != nil && compareUtil(table, found, where) {
			return found, nil
		}

		return nil, NewQueryError(404, fmt.Sprintf("No row found with constraint %v in table %s", where, table.Name))
	}

	if len(table.Indexes) > 0 {
		return nil, fmt.Errorf("Unique fields not included in findUnique request")
	} else {
		return nil, fmt.Errorf("Table does not have any unique fields")
	}
}

func Find(table *builder.Table, where QueryArg, allow_empty_where bool) ([]builder.TDBTableRow, error) {
	return findManyUtil(table, where, allow_empty_where)
}

type FindArgs struct {
	Where   QueryArg
	Take    int
	Skip    int
	OrderBy map[string]OrderBy
	Cursor  QueryArg
}

func FindWithArgs(table *builder.Table, args FindArgs, allow_empty_where bool) ([]builder.TDBTableRow, error) {
	res, err := findManyUtil(table, args.Where, allow_empty_where)
	if err != nil {
		return []builder.TDBTableRow{}, nil
	}

	if args.OrderBy != nil {
		for field, order := range args.OrderBy {
			if !table.Fields.Has(field) {
				continue
			}
			res = sortRowsByField(table.Fields.Get(field), res, order)
		}
	}

	if args.Cursor != nil {
		cursor_idx := slices.IndexFunc(res, func(row builder.TDBTableRow) bool {
			return compareUtil(table, row, args.Cursor)
		})
		if cursor_idx > 0 {
			res = res[cursor_idx:]
		}
	}

	if args.Skip > 0 {
		// If skip is greater than the length of the array,
		// return an empty array
		if args.Skip > len(res) {
			return []builder.TDBTableRow{}, nil
		}
		res = res[args.Skip:]

	}

	if args.Take > 0 && len(res) > args.Take {
		res = res[:args.Take]
	}

	return res, nil
}

func Delete(table *builder.Table, row builder.TDBTableRow) {
	for _, index := range table.Indexes {
		if !row.Has(index) || table.Fields.Get(index).IndexLevel() < builder.IndexLevelUnique {
			continue
		}
		table.IndexMap(index).Delete(row.Get(index))
	}
	table.Rows().Delete(builder.GetPrimaryKey(row))
}
