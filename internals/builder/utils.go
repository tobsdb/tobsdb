package builder

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/tobshub/tobsdb/internals/parser"
	"github.com/tobshub/tobsdb/internals/types"
	"github.com/tobshub/tobsdb/pkg"
)

func (schema *Schema) filterRows(t_schema *parser.Table, field_name string, value any, exit_first bool) []map[string]any {
	found_rows := []map[string]any{}
	table := schema.Data[t_schema.Name]

	s_field := t_schema.Fields[field_name]

	for _, row := range table {
		if row[field_name] == nil && value == nil {
			found_rows = append(found_rows, row)
		} else if t_schema.Compare(&s_field, row[field_name], value) {
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
func (schema *Schema) validateRelation(field *parser.Field, res any) error {
	relation := field.Properties[types.FieldPropRelation]
	rel_schema_name, rel_field_name := parser.ParseRelationProp(relation)
	rel_schema := schema.Tables[rel_schema_name]
	rel_row, err := schema.FindUnique(&rel_schema, map[string]any{rel_field_name: res})
	if err != nil {
		return err
	} else if rel_row == nil {
		if is_opt, ok := field.Properties[types.FieldPropOptional]; !ok || is_opt != "true" {
			return fmt.Errorf("No row found for relation table %s", rel_schema_name)
		}
	}

	return nil
}

func HttpError(w http.ResponseWriter, status int, err string) {
	pkg.InfoLog("http error:", err)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(Response{
		Message: err,
		Status:  status,
	})
}
