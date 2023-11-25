package query

import (
	"fmt"
	"net/http"

	"github.com/tobsdb/tobsdb/internal/parser"
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"github.com/tobsdb/tobsdb/pkg"
)

func findManyUtil(schema *Schema, t_schema *parser.Table, where map[string]any, allow_empty_where bool) ([]map[string]any, error) {
	if allow_empty_where && (where == nil || len(where) == 0) {
		// nil comparison works here
		return schema.filterRows(t_schema, "", nil), nil
	} else if where == nil || len(where) == 0 {
		return nil, fmt.Errorf("Where constraints cannot be empty")
	}

	found_rows := [](map[string]any){}
	contains_index := false
	has_searched := false

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
		} else if !has_searched {
			found_rows = schema.filterRows(t_schema, index, where[index])
		}
		has_searched = true
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
		} else if !contains_index && !has_searched {
			found_rows = schema.filterRows(t_schema, s_field.Name, input)
		}
		has_searched = true
	}

	return found_rows, nil
}

func compareUtil(t_schema *parser.Table, row, constraints map[string]any) bool {
	for _, field := range t_schema.Fields {
		constraint, ok := constraints[field.Name]
		if ok && !t_schema.Compare(field, row[field.Name], constraint) {
			return false
		}
	}
	return true
}

func (schema *Schema) findFirst(t_schema *parser.Table, field_name string, value any) map[string]any {
	found := schema._filterRows(t_schema, field_name, value, true)
	if len(found) == 0 {
		return nil
	}
	return found[0]
}

func (schema *Schema) filterRows(t_schema *parser.Table, field_name string, value any) []map[string]any {
	return schema._filterRows(t_schema, field_name, value, false)
}

func (schema *Schema) _filterRows(t_schema *parser.Table, field_name string, value any, exit_first bool) []map[string]any {
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

	rel_row := schema.findFirst(rel_table_schema, rel_field_name, data)

	// TODO: revisit this logic
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

func (schema *Schema) validateUnique(t_schema *parser.Table, field *parser.Field, data any) error {
	if idx_level := field.IndexLevel(); idx_level > parser.IndexLevelNone {
		check_row, err := schema.FindUnique(t_schema, map[string]any{field.Name: data})
		if err != nil {
			return err
		}

		if check_row != nil {
			if idx_level == parser.IndexLevelPrimary {
				return NewQueryError(http.StatusConflict, "Primary key already exists")
			}

			return NewQueryError(
				http.StatusConflict,
				fmt.Sprintf("Value for unique field %s already exists", field.Name),
			)
		}
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

func formatIndexValue(v any) string {
	return fmt.Sprintf("%v", v)
}
