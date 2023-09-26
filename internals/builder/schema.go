package builder

import (
	"bufio"
	"fmt"
	"log"
	"net/url"
	"strings"

	. "github.com/tobshub/tobsdb/internals/parser"
	TDBTypes "github.com/tobshub/tobsdb/internals/types"
)

func NewSchemaFromURL(input *url.URL, data TDBData) Schema {
	params, err := url.ParseQuery(input.RawQuery)
	if err != nil {
		log.Fatal(err)
	}
	schema_data := params.Get("schema")

	schema := Schema{Tables: make(map[string]Table), Data: data}

	if schema.Data == nil {
		schema.Data = make(TDBData)
	}

	scanner := bufio.NewScanner(strings.NewReader(schema_data))
	line_idx := 0

	current_table := Table{IdTracker: 0}

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
		case ParserStateTableStart:
			current_table.Name = data.Name
			current_table.Fields = make(map[string]Field)
			current_table.Indexes = []string{}
		case ParserStateTableEnd:
			schema.Tables[current_table.Name] = current_table
			current_table = Table{}
		case ParserStateNewField:
			current_table.Fields[data.Name] = Field{
				Name:        data.Name,
				Properties:  data.Properties,
				BuiltinType: data.Builtin_type,
			}

			// added unique fields and primary keys to table indexes
			if is_unique, ok := data.Properties[TDBTypes.FieldPropUnique]; ok && is_unique == "true" {
				current_table.Indexes = append(current_table.Indexes, data.Name)
			} else if key_type, ok := data.Properties[TDBTypes.FieldPropKey]; ok && key_type == "primary" {
				current_table.Indexes = append(current_table.Indexes, data.Name)
			}
		}
	}

	err = ValidateSchemaRelations(&schema)
	if err != nil {
		log.Fatal(err)
	}

	return schema
}

func ValidateSchemaRelations(schema *Schema) error {
	for table_key, table := range schema.Tables {
		for field_key, field := range table.Fields {
			relation, is_relation := field.Properties[TDBTypes.FieldPropRelation]
			if !is_relation {
				continue
			}

			rel_table_name, rel_field_name := ParseRelationProp(relation)

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
