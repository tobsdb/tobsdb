package builder

import "github.com/tobshub/tobsdb/cmd/parser"

func (db *TobsDB) FilterRows(schema *parser.Table, field_name string, value any) []map[string]any {
	found_rows := []map[string]any{}
	table := db.data[schema.Name]

	for _, row := range table {
		if row[field_name] == value {
			found_rows = append(found_rows, row)
		}
	}

	return found_rows
}
