package builder

func (db *TobsDB) FilterRows(table_name string, field_name string, value any) []map[string]any {
	found_rows := []map[string]any{}
	table := db.data[table_name]

	for _, row := range table {
		if row[field_name] == value {
			found_rows = append(found_rows, row)
		}
	}

	return found_rows
}
