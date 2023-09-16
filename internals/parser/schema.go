package parser

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	TDBTypes "github.com/tobshub/tobsdb/internals/types"
	"golang.org/x/exp/slices"
)

type Schema struct {
	Tables map[string]Table
}

type Table struct {
	Name      string
	Fields    map[string]Field
	Indexes   []string
	IdTracker int
}

type Field struct {
	Name        string
	BuiltinType TDBTypes.FieldType
	Properties  map[TDBTypes.FieldProp]string
}

func NewSchema(path string) Schema {
	schema := Schema{Tables: make(map[string]Table)}

	f, err := os.Open(path)
	if err != nil {
		log.Fatal("Error reading schema file: ", err)
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)
	line_idx := 0

	current_table := Table{IdTracker: 0}

	for scanner.Scan() {
		line_idx++
		line := strings.TrimSpace(scanner.Text())

		// Ignore empty lines & comments
		if len(line) == 0 || strings.HasPrefix(line, "//") {
			continue
		}

		state, data, err := lineParser(line)
		if err != nil {
			log.Fatalf("Error parsing line %d: %s", line_idx, err)
		}

		switch state {
		case parserStateTableStart:
			current_table.Name = data.name
			current_table.Fields = make(map[string]Field)
			current_table.Indexes = []string{}
		case parserStateTableEnd:
			schema.Tables[current_table.Name] = current_table
			current_table = Table{}
		case parserStateNewField:
			current_table.Fields[data.name] = Field{
				Name:        data.name,
				Properties:  data.properties,
				BuiltinType: data.builtin_type,
			}

			// added unique fields and primary keys to table indexes
			if is_unique, ok := data.properties[TDBTypes.FieldPropUnique]; ok && is_unique == "true" {
				current_table.Indexes = append(current_table.Indexes, data.name)
			} else if key_type, ok := data.properties[TDBTypes.FieldPropKey]; ok && key_type == "primary" {
				current_table.Indexes = append(current_table.Indexes, data.name)
			}
		}
	}

	err = validateSchemaRelations(&schema)
	if err != nil {
		log.Fatal(err)
	}

	return schema
}

type lineParserState int

const (
	parserStateTableStart lineParserState = iota
	parserStateTableEnd
	parserStateNewField
	parserStateIdle
)

type parserData struct {
	name         string
	builtin_type TDBTypes.FieldType
	properties   map[TDBTypes.FieldProp]string
}

func lineParser(line string) (lineParserState, parserData, error) {
	if strings.HasPrefix(line, "$TABLE") {
		name := line[7:]
		name_end := strings.Index(name, " ")

		if name_end > 0 {
			open_bracket := strings.TrimSpace(name[name_end:])
			if open_bracket != "{" {
				return parserStateIdle, parserData{}, fmt.Errorf("Table name cannot include space")
			}
			name = name[:name_end]
			return parserStateTableStart, parserData{name: name}, nil
		}
	} else if line == "}" {
		return parserStateTableEnd, parserData{}, nil
	} else {
		splits := cleanLineSplit(strings.Split(line, " "))
		builtin_type := TDBTypes.FieldType(splits[1])

		field_props, err := parseRawFieldProps(strings.Join(splits[2:], " "))
		err = validateFieldType(builtin_type)

		if err != nil {
			return parserStateIdle, parserData{}, err
		}

		return parserStateNewField, parserData{name: splits[0], builtin_type: builtin_type, properties: field_props}, nil
	}
	return parserStateIdle, parserData{}, errors.New("Invalid line")
}

func parseRawFieldProps(raw string) (map[TDBTypes.FieldProp]string, error) {
	props := make(map[TDBTypes.FieldProp]string)

	r := regexp.MustCompile(`(?m)(\w+)\(([^)]+)\)`)

	for _, entry := range r.FindAllString(raw, -1) {
		split := strings.Split(entry, "(")
		prop, value := split[0], strings.TrimRight(split[1], ")")
		props[TDBTypes.FieldProp(prop)] = value
	}

	return props, nil
}

func validateFieldType(builtin_type TDBTypes.FieldType) error {
	if slices.Contains(TDBTypes.VALID_BUILTIN_TYPES, builtin_type) {
		return nil
	}
	return fmt.Errorf("Invalid field type %s", builtin_type)
}

func cleanLineSplit(splits []string) []string {
	for i := 0; i < len(splits); i++ {
		if len(splits[i]) == 0 {
			splits = append(splits[:i], splits[i+1:]...)
			i--
		}
	}
	return splits
}

func validateSchemaRelations(schema *Schema) error {
	for table_key, table := range schema.Tables {
		for field_key, field := range table.Fields {
			relation, is_relation := field.Properties[TDBTypes.FieldPropRelation]
			if !is_relation {
				continue
			}

			rel_table_name, rel_field_name := ParseRelation(relation)

			if len(rel_table_name) == 0 || len(rel_field_name) == 0 {
				return fmt.Errorf(
					"Invalid relation syntax on table %s in field %s",
					table_key,
					field_key,
				)
			}

			if rel_table, ok := schema.Tables[rel_table_name]; !ok {
				return fmt.Errorf(
					"Invalid relation between %s and %s in field %s; \"%s\" is not a valid table",
					table_key,
					rel_table_name,
					field_key,
					rel_table_name,
				)
			} else if relation == table_key {
				return fmt.Errorf(
					"Invalid relation between %s and %s in field %s; %s and %s are the same table",
					table_key,
					rel_table_name,
					field_key,
					table_key,
					rel_table_name,
				)
			} else {
				if rel_field, ok := rel_table.Fields[rel_field_name]; !ok {
					return fmt.Errorf(
						"Invalid relation between %s and %s in field %s; \"%s\" is not a valid field on table %s",
						table_key,
						rel_table_name,
						field_key,
						rel_field_name,
						rel_table_name,
					)
				} else {
					if rel_field.BuiltinType != field.BuiltinType {
						return fmt.Errorf(
							"Invalid relation between %s and %s in field %s; field types must match",
							table_key,
							rel_table_name,
							field_key,
						)
					}
				}
			}
		}
	}

	return nil
}
