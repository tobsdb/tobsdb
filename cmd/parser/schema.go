package parser

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"golang.org/x/exp/slices"
)

type Schema struct {
	tables map[string]Table
}

type Table struct {
	name   string
	fields map[string]Field
}

type Field struct {
	name         string
	builtin_type string
	properties   []string
}

func SchemaParser(path string) (Schema, error) {
	schema := Schema{tables: make(map[string]Table)}

	f, err := os.Open(path)
	if err != nil {
		log.Fatal("Error reading schema file: ", err)
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)
	line_idx := 0

	current_table := Table{}

	for scanner.Scan() {
		line_idx++
		line := strings.TrimSpace(scanner.Text())

		// Ignore empty lines & comments
		if len(line) == 0 || strings.HasPrefix(line, "//") {
			continue
		}

		state, data, err := LineParser(line)
		if err != nil {
			log.Fatalf("Error parsing line %d: %s", line_idx, err)
		}

		switch state {
		case TableStart:
			current_table.name = data.name
			current_table.fields = make(map[string]Field)
		case TableEnd:
			schema.tables[current_table.name] = current_table
			current_table = Table{}
		case NewField:
			current_table.fields[data.name] = Field{
				name:         data.name,
				properties:   data.properties,
				builtin_type: data.builtin_type,
			}
		}
	}

	err = ValidateSchemaRelations(schema)
	if err != nil {
		log.Fatal(err)
	}

	return schema, nil
}

type LineParserState int

const (
	TableStart LineParserState = iota
	TableEnd
	NewField
	Idle
)

type ParserData struct {
	name         string
	builtin_type string // TODO: implement types
	properties   []string
}

func LineParser(line string) (LineParserState, ParserData, error) {
	if strings.HasPrefix(line, "$TABLE") {
		name := line[7:]
		name_end := strings.Index(name, " ")

		if name_end > 0 {
			name = name[:name_end]
			return TableStart, ParserData{name: name}, nil
		}
	} else if line == "}" {
		return TableEnd, ParserData{}, nil
	} else {
		splits := CleanLineSplit(strings.Split(line, " "))
		builtin_type := splits[1]
		field_props := splits[2:]
		err := ValidateFielddType(builtin_type)
		// defer error if field if marked as a relation to another table
		if err != nil && !slices.Contains(field_props, "relation") {
			return Idle, ParserData{}, err
		}
		return NewField, ParserData{name: splits[0], builtin_type: builtin_type, properties: field_props}, nil
	}
	return Idle, ParserData{}, errors.New("Invalid line")
}

var valid_builtin_types = []string{
	"Int", "String", "Date", "Float", "Bool", "Bytes",
}

func ValidateFielddType(builtin_type string) error {
	if slices.Contains(valid_builtin_types, builtin_type) {
		return nil
	}
	return fmt.Errorf("Invalid field type %s", builtin_type)
}

func CleanLineSplit(splits []string) []string {
	for i := 0; i < len(splits); i++ {
		if len(splits[i]) == 0 {
			splits = append(splits[:i], splits[i+1:]...)
			i--
		}
	}
	return splits
}

func ValidateSchemaRelations(schema Schema) error {
	for table_key, table := range schema.tables {
		for field_key, field := range table.fields {
			if slices.Contains(field.properties, "relation") {
				if _, ok := schema.tables[field.builtin_type]; !ok {
					return fmt.Errorf(
						"Invalid relation between %s and %s; %s is not a valid table",
						table_key,
						field.builtin_type,
						field.builtin_type,
					)
				} else if field.builtin_type == table_key {
					return fmt.Errorf(
						"Invalid relation between %s and %s in field %s; %s and %s are the same table",
						table_key,
						field.builtin_type,
						field_key,
						table_key,
						field.builtin_type,
					)
				}
			}
		}
	}
	return nil
}
