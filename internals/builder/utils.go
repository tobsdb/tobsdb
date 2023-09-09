package builder

import "github.com/tobshub/tobsdb/internals/parser"

func (db *TobsDB) FilterRows(schema *parser.Table, field_name string, value any, exit_first bool) []map[string]any {
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
