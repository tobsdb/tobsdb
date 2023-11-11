package query

import (
	"fmt"

	"github.com/tobsdb/tobsdb/internal/parser"
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"github.com/tobsdb/tobsdb/pkg"
)

func findManyUtil(schema *Schema, t_schema *parser.Table, where map[string]any, allow_empty_where bool) ([]map[string]any, error) {
	if allow_empty_where && (where == nil || len(where) == 0) {
		// nil comparison works here
		return schema.filterRows(t_schema, "", nil, false), nil
	} else if where == nil || len(where) == 0 {
		return nil, fmt.Errorf("Where constraints cannot be empty")
	}

	found_rows := [](map[string]any){}
	contains_index := false

	// filter with indexes first
	for _, index := range t_schema.Indexes {
		input, ok := where[index]
		if !ok {
			continue
		}

		contains_index = true
		if len(found_rows) > 0 {
			s_field := t_schema.Fields[index]
			found_rows = pkg.Filter(found_rows, func(row map[string]any) bool {
				return t_schema.Compare(s_field, row[index], input)
			})
		} else {
			found_rows = schema.filterRows(t_schema, index, where[index], false)
		}
	}

	// filter with non-indexes
	for _, s_field := range t_schema.Fields {
		input, ok := where[s_field.Name]
		if s_field.IndexLevel() > parser.IndexLevelNone || !ok {
			continue
		}

		if len(found_rows) > 0 {
			found_rows = pkg.Filter(found_rows, func(row map[string]any) bool {
				return t_schema.Compare(s_field, row[s_field.Name], input)
			})
		} else if !contains_index && len(found_rows) == 0 {
			found_rows = schema.filterRows(t_schema, s_field.Name, input, false)
		}
	}

	return found_rows, nil
}

func (schema *Schema) filterRows(t_schema *parser.Table, field_name string, value any, exit_first bool) []map[string]any {
	found_rows := []map[string]any{}
	table := schema.Data[t_schema.Name].Rows

	s_field := t_schema.Fields[field_name]

	for _, row := range table {
		if t_schema.Compare(s_field, row[field_name], value) {
			found_rows = append(found_rows, row)
			if exit_first {
				return found_rows
			}
		}
	}

	return found_rows
}

// validateRelation() checks if the row implied by the relation exists
// before the new row is added
func (schema *Schema) validateRelation(table_name string, field *parser.Field, id *int, data any) error {
	relation := field.Properties[props.FieldPropRelation]
	rel_table_name, rel_field_name := parser.ParseRelationProp(relation)
	rel_table_schema := schema.Tables[rel_table_name]

	// TODO: validate many-to-one, many-to-many relations
	if field.BuiltinType == types.FieldTypeVector {
		return nil
	}

	rel_row, err := schema.FindUnique(rel_table_schema, map[string]any{rel_field_name: data})
	if err != nil {
		return err
	}

	if rel_row == nil {
		if is_opt, ok := field.Properties[props.FieldPropOptional]; !ok || is_opt != "true" {
			return fmt.Errorf("No row found for relation table %s", rel_table_name)
		}
	}

	if table_name == rel_table_name && id != nil && pkg.NumToInt(rel_row["id"]) == *id {
		return fmt.Errorf("Row cannot create a relation to itself")
	}

	return nil
}

type QueryError struct {
	msg    string
	status int
}

func NewQueryError(status int, msg string) *QueryError {
	return &QueryError{msg: msg, status: status}
}

func (e QueryError) Error() string { return e.msg }
func (e QueryError) Status() int   { return e.status }
