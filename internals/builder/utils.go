package builder

import (
	"fmt"

	TDBParser "github.com/tobshub/tobsdb/internals/parser"
	TDBTypes "github.com/tobshub/tobsdb/internals/types"
)

func (db *TobsDB) filterRows(schema *TDBParser.Table, field_name string, value any, exit_first bool) []map[string]any {
	found_rows := []map[string]any{}
	table := db.data[schema.Name]

	s_field := schema.Fields[field_name]

	for _, row := range table {
		if row[field_name] == nil && value == nil {
			found_rows = append(found_rows, row)
		} else if s_field.Compare(schema, row[field_name], value) {
			found_rows = append(found_rows, row)
			if exit_first {
				return found_rows
			}
		}
	}

	return found_rows
}

func (db *TobsDB) validateRelation(field *TDBParser.Field, res any) error {
	relation := field.Properties[TDBTypes.FieldPropRelation]
	rel_schema_name, rel_field_name := TDBParser.ParseRelation(relation)
	rel_schema := db.schema.Tables[rel_schema_name]
	rel_row, err := db.FindUnique(&rel_schema, map[string]any{rel_field_name: res})
	if err != nil {
		return err
	} else if rel_row == nil {
		if is_opt, ok := field.Properties[TDBTypes.FieldPropOptional]; !ok || is_opt != "true" {
			return fmt.Errorf("No row found for relation table %s", rel_schema_name)
		}
	}

	return nil
}
