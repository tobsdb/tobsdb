package builder

import (
	"bufio"
	"fmt"
	"net/url"
	"strings"

	. "github.com/tobsdb/tobsdb/internal/parser"
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/query"
	"github.com/tobsdb/tobsdb/internal/types"
	"github.com/tobsdb/tobsdb/pkg"
)

func ParseSchema(schema_data string) (*query.Schema, error) {
	schema := query.Schema{Tables: make(map[string]*Table)}

	scanner := bufio.NewScanner(strings.NewReader(schema_data))
	line_idx := 0

	current_table := &Table{IdTracker: 0}

	for scanner.Scan() {
		line_idx++
		line := strings.TrimSpace(scanner.Text())

		// Ignore empty lines & comments
		if len(line) == 0 || strings.HasPrefix(line, "//") {
			continue
		}

		state, data, err := LineParser(line)
		if err != nil {
			return nil, ParseLineError(line_idx, err.Error())
		}

		switch state {
		case ParserStateTableStart:
			if _, exists := schema.Tables[data.Name]; exists {
				return nil, ParseLineError(line_idx, fmt.Sprintf("Duplicate table %s", data.Name))
			}
			current_table.Name = data.Name
			current_table.Fields = make(map[string]*Field)
			current_table.Indexes = []string{}
		case ParserStateTableEnd:
			schema.Tables[current_table.Name] = current_table
			current_table = &Table{}
		case ParserStateNewField:
			if _, exists := current_table.Fields[data.Name]; exists {
				return nil, ParseLineError(line_idx, fmt.Sprintf("Duplicate field %s", data.Name))
			}
			new_field := Field{
				Name:             data.Name,
				Properties:       data.Properties,
				BuiltinType:      data.Builtin_type,
				IncrementTracker: 0,
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

func NewSchemaFromURL(input *url.URL, data query.TDBData, build_only bool) (*query.Schema, error) {
	params, err := url.ParseQuery(input.RawQuery)
	if err != nil {
		return nil, err
	}
	schema_data := params.Get("schema")

	if len(schema_data) == 0 {
		return nil, fmt.Errorf("No schema provided")
	}

	schema, err := ParseSchema(schema_data)
	if err != nil {
		return nil, err
	}

	if build_only {
		return schema, nil
	}

	if data == nil {
		schema.Data = make(map[string]*query.TDBTableData)
	} else {
		schema.Data = data
	}

	for t_name, t_schema := range schema.Tables {
		t_data, ok := schema.Data[t_name]
		if !ok {
			schema.Data[t_name] = &query.TDBTableData{
				Rows:    make(query.TDBTableRows),
				Indexes: make(query.TDBTableIndexes),
			}
			continue
		}
		for key, t_data := range t_data.Rows {
			if key > t_schema.IdTracker {
				t_schema.IdTracker = key
			}

			for f_name, field := range t_schema.Fields {
				if field.BuiltinType != types.FieldTypeInt {
					continue
				}

				_f_data := t_data[f_name]
				if _f_data == nil {
					continue
				}

				f_data := pkg.NumToInt(_f_data)
				if f_data > field.IncrementTracker {
					field.IncrementTracker = f_data
				}
				t_schema.Fields[f_name] = field
			}
		}
		schema.Tables[t_name] = t_schema
	}

	return schema, nil
}

// ValidateSchemaRelations() allows relations to be defined with non-unique fields.
//
// This logic means that relations defined with unqiue fields are 1-to-1 relations,
// while relations defined with non-unique fields are 1-to-many.
//
// vector -> non-vector type relations are one-to-many;
// non-vector -> vector type relations are many-to-one;
// vector -> vector type relations are many-to-many;
//
// it is assumed that a vector field that is a relation is a vector of individual relations
// and not a relation as a vector itself
func ValidateSchemaRelations(schema *query.Schema) error {
	for table_key, table := range schema.Tables {
		for field_key, field := range table.Fields {
			relation, is_relation := field.Properties[props.FieldPropRelation]
			if !is_relation {
				continue
			}

			rel_table_name, rel_field_name := ParseRelationProp(relation.(string))

			invalidRelationError := ThrowInvalidRelationError(table_key, rel_table_name, field_key)

			rel_table, rel_table_exists := schema.Tables[rel_table_name]

			if !rel_table_exists {
				return invalidRelationError(fmt.Sprintf("%s is not a valid table", rel_table_name))
			}

			// (???) allow same-table relations
			// if relation == table_key {
			// 	return fmt.Errorf(
			// 		"Invalid relation between %s and %s in field %s; %s and %s are the same table",
			// 		table_key,
			// 		rel_table_name,
			// 		field_key,
			// 		table_key,
			// 		rel_table_name,
			// 	)
			// }

			rel_field, rel_field_ok := rel_table.Fields[rel_field_name]
			if !rel_field_ok {
				return invalidRelationError(
					fmt.Sprintf("%s is not a valid field on table %s", rel_field_name, rel_table_name),
				)
			}

			if rel_field.BuiltinType != field.BuiltinType {
				// check vector <-> non-vector relations
				if field.BuiltinType == types.FieldTypeVector {
					vector_type, v_level := ParseVectorProp(field.Properties[props.FieldPropVector].(string))
					if v_level > 1 {
						return invalidRelationError("nested vector fields cannot be relations")
					}
					if rel_field.BuiltinType != vector_type {
						return invalidRelationError("field types must match")
					}
				} else if rel_field.BuiltinType == types.FieldTypeVector {
					vector_type, _ := ParseVectorProp(rel_field.Properties[props.FieldPropVector].(string))
					if field.BuiltinType != vector_type {
						return invalidRelationError("field types must match")
					}
				} else {
					return invalidRelationError("field types must match")
				}
			}

			// check vector types & levels are the same
			if field.BuiltinType == types.FieldTypeVector && rel_field.BuiltinType == types.FieldTypeVector {
				field_v_type, field_v_level := ParseVectorProp(field.Properties[props.FieldPropVector].(string))
				rel_field_v_type, rel_field_v_level := ParseVectorProp(rel_field.Properties[props.FieldPropVector].(string))

				if field_v_type != rel_field_v_type || field_v_level != rel_field_v_level {
					return invalidRelationError("field types must match")
				}
			}
		}
	}

	return nil
}

func ThrowInvalidRelationError(table_name, rel_table_name, field_name string) func(string) error {
	return func(reason string) error {
		return fmt.Errorf(
			"Invalid relation between %s and %s in field %s; %s",
			table_name, rel_table_name, field_name, reason,
		)
	}
}
