package query

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/internal/parser"
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"github.com/tobsdb/tobsdb/pkg"
)

type OrderBy string

const (
	OrderByAsc  OrderBy = "asc"
	OrderByDesc OrderBy = "desc"
)

func sortRowsByField(field *builder.Field, rows []builder.TDBTableRow, order OrderBy) []builder.TDBTableRow {
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i].Get(field.Name), rows[j].Get(field.Name)
		if order == "desc" {
			return field.IsLess(b, a)
		}
		return field.IsLess(a, b)
	})
	return rows
}

func findManyUtil(table *builder.Table, where QueryArg, allow_empty_where bool) ([]builder.TDBTableRow, error) {
	if allow_empty_where && (where == nil || len(where) == 0) {
		// nil comparison works here
		return filterRows(table, "", nil), nil
	} else if where == nil || len(where) == 0 {
		return nil, fmt.Errorf("Where constraints cannot be empty")
	}

	found_rows := [](builder.TDBTableRow){}
	contains_index := false
	has_searched := false

	// filter with indexes first
	// TODO(???): use index map
	for _, index := range table.Indexes {
		if !where.Has(index) {
			continue
		}

		input := where.Get(index)

		contains_index = true
		if len(found_rows) > 0 {
			s_field := table.Fields[index]
			found_rows = pkg.Filter(found_rows, func(row builder.TDBTableRow) bool {
				return s_field.Compare(row.Get(index), input)
			})
		} else if !has_searched {
			found_rows = filterRows(table, index, input)
		}
		has_searched = true
	}

	// filter with non-indexes
	for _, field := range table.Fields {
		if field.IndexLevel() > builder.IndexLevelNone || !where.Has(field.Name) {
			continue
		}

		input := where.Get(field.Name)

		if len(found_rows) > 0 {
			found_rows = pkg.Filter(found_rows, func(row builder.TDBTableRow) bool {
				return field.Compare(row.Get(field.Name), input)
			})
		} else if !contains_index && !has_searched {
			found_rows = filterRows(table, field.Name, input)
		}
		has_searched = true
	}

	return found_rows, nil
}

func compareUtil(t_schema *builder.Table, row builder.TDBTableRow, constraints QueryArg) bool {
	for _, field := range t_schema.Fields {
		if !constraints.Has(field.Name) {
			continue
		}
		constraint := constraints.Get(field.Name)
		if !field.Compare(row.Get(field.Name), constraint) {
			return false
		}
	}
	return true
}

func findFirst(table *builder.Table, field_name string, value any) builder.TDBTableRow {
	found := _filterRows(table, field_name, value, true)
	if len(found) == 0 {
		return nil
	}
	return found[0]
}

func filterRows(table *builder.Table, field_name string, value any) []builder.TDBTableRow {
	return _filterRows(table, field_name, value, false)
}

func _filterRows(t_schema *builder.Table, field_name string, value any, exit_first bool) []builder.TDBTableRow {
	found_rows := []builder.TDBTableRow{}
	t_schema.Rows().Locker.RLock()
	defer t_schema.Rows().Locker.RUnlock()
	iterCh, err := t_schema.Rows().Map.IterCh()
	if err != nil {
		return found_rows
	}

	s_field := t_schema.Fields[field_name]

	for row := range iterCh.Records() {
		if s_field.Compare(row.Val.Get(field_name), value) {
			found_rows = append(found_rows, row.Val)
			if exit_first {
				return found_rows
			}
		}
	}

	return found_rows
}

// validateRelation() checks if the row implied by the relation exists
// before the new row is added
func validateRelation(table *builder.Table, field *builder.Field, id *int, data any) error {
	relation := field.Properties.Get(props.FieldPropRelation)
	rel_table_name, rel_field_name := parser.ParseRelationProp(relation.(string))
	rel_table_schema := table.Schema.Tables.Get(rel_table_name)

	// TODO: validate many-to-one, many-to-many relations
	if field.BuiltinType == types.FieldTypeVector {
		return nil
	}

	rel_row := findFirst(rel_table_schema, rel_field_name, data)

	if rel_row == nil {
		if !field.Properties.Has(props.FieldPropOptional) {
			return fmt.Errorf("No row found for relation %s.%s -> %s.%s",
				table.Name, field.Name, rel_table_name, rel_field_name)
		}

		is_opt := field.Properties.Get(props.FieldPropOptional)
		if is_opt.(bool) && data != nil {
			return fmt.Errorf("No row found for relation %s.%s -> %s.%s",
				table.Name, field.Name, rel_table_name, rel_field_name)
		}
	}

	if table.Name == rel_table_name && id != nil && builder.GetPrimaryKey(rel_row) == *id {
		return fmt.Errorf("Row cannot create a relation to itself")
	}

	return nil
}

func validateUnique(t_schema *builder.Table, field *builder.Field, data any) error {
	if idx_level := field.IndexLevel(); idx_level > builder.IndexLevelNone {
		_, err := FindUnique(t_schema, QueryArg{field.Name: data})

		if err == nil {
			if idx_level == builder.IndexLevelPrimary {
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
