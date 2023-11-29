package builder

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/tobsdb/tobsdb/internal/parser"
)

func ParseSchema(schema_data string) (*Schema, error) {
	schema := Schema{Tables: make(map[string]*Table)}

	scanner := bufio.NewScanner(strings.NewReader(schema_data))
	line_idx := 0

	current_table := &Table{IdTracker: 0, Schema: &schema}

	for scanner.Scan() {
		line_idx++
		line := strings.TrimSpace(scanner.Text())

		// Ignore empty lines & comments
		if len(line) == 0 || strings.HasPrefix(line, "//") {
			continue
		}

		state, data, err := parser.LineParser(line)
		if err != nil {
			return nil, ParseLineError(line_idx, err.Error())
		}

		switch state {
		case parser.ParserStateTableStart:
			if _, exists := schema.Tables[data.Name]; exists {
				return nil, ParseLineError(line_idx, fmt.Sprintf("Duplicate table %s", data.Name))
			}
			current_table.Name = data.Name
			current_table.Fields = make(map[string]*Field)
			current_table.Indexes = []string{}
		case parser.ParserStateTableEnd:
			schema.Tables[current_table.Name] = current_table
			current_table = &Table{Schema: &schema}
		case parser.ParserStateNewField:
			if _, exists := current_table.Fields[data.Name]; exists {
				return nil, ParseLineError(line_idx, fmt.Sprintf("Duplicate field %s", data.Name))
			}
			new_field := Field{
				Name:             data.Name,
				Properties:       data.Properties,
				BuiltinType:      data.Builtin_type,
				IncrementTracker: 0,
				Table:            current_table,
			}

			index_level := new_field.IndexLevel()
			if index_level == IndexLevelPrimary && current_table.PrimaryKey() != nil {
				return nil, ParseLineError(line_idx, "Table can't have multiple primary keys")
			}

			if err := CheckFieldRules(&new_field); err != nil {
				return nil, ParseLineError(line_idx, err.Error())
			}

			current_table.Fields[new_field.Name] = &new_field

			if index_level > IndexLevelNone {
				current_table.Indexes = append(current_table.Indexes, new_field.Name)
			}
		}
	}

	err := ValidateSchemaRelations(&schema)
	if err != nil {
		return nil, err
	}

	return &schema, nil
}

func ParseLineError(line int, reason string) error {
	return fmt.Errorf("Error parsing line %d: %s", line, reason)
}
