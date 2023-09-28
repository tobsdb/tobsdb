package builder

import (
	"encoding/json"
	"fmt"
	"net/http"

	TDBParser "github.com/tobshub/tobsdb/internals/parser"
	TDBTypes "github.com/tobshub/tobsdb/internals/types"
	TDBPkg "github.com/tobshub/tobsdb/pkg"
)

func (schema *Schema) filterRows(t_schema *TDBParser.Table, field_name string, value any, exit_first bool) []map[string]any {
	found_rows := []map[string]any{}
	table := schema.Data[t_schema.Name]

	s_field := t_schema.Fields[field_name]

	for _, row := range table {
		if row[field_name] == nil && value == nil {
			found_rows = append(found_rows, row)
		} else if s_field.Compare(t_schema, row[field_name], value) {
			found_rows = append(found_rows, row)
			if exit_first {
				return found_rows
			}
		}
	}

	return found_rows
}

func (schema *Schema) validateRelation(field *TDBParser.Field, res any) error {
	relation := field.Properties[TDBTypes.FieldPropRelation]
	rel_schema_name, rel_field_name := TDBParser.ParseRelationProp(relation)
	rel_schema := schema.Tables[rel_schema_name]
	rel_row, err := schema.FindUnique(&rel_schema, map[string]any{rel_field_name: res})
	if err != nil {
		return err
	} else if rel_row == nil {
		if is_opt, ok := field.Properties[TDBTypes.FieldPropOptional]; !ok || is_opt != "true" {
			return fmt.Errorf("No row found for relation table %s", rel_schema_name)
		}
	}

	return nil
}

func HttpError(w http.ResponseWriter, status int, err string) {
	TDBPkg.InfoLog("http error:", err)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(Response{
		Message: err,
		Status:  status,
	})
}
