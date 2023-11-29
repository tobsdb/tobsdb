package builder

import (
	"fmt"
	"net/url"

	"github.com/tobsdb/tobsdb/internal/parser"
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"github.com/tobsdb/tobsdb/pkg"
)

type (
	// Maps row field name to its saved data
	TDBTableRow = map[string]any
	// Maps row id to its saved data
	TDBTableRows = map[int](TDBTableRow)
	// index field name -> index value -> row id
	TDBTableIndexes = map[string]map[string]int
	TDBTableData    struct {
		Rows    TDBTableRows
		Indexes TDBTableIndexes
	}
	// Maps table name to its saved data
	TDBData = map[string]*TDBTableData
)

type Schema struct {
	Tables map[string]*Table
	// table_name -> row_id -> field_name -> value
	Data TDBData
}

const SYS_PRIMARY_KEY = "__tdb_id__"

func NewSchemaFromURL(input *url.URL, data TDBData, build_only bool) (*Schema, error) {
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
		schema.Data = make(map[string]*TDBTableData)
	} else {
		schema.Data = data
	}

	for t_name, t_schema := range schema.Tables {
		t_schema.Schema = schema
		t_data, ok := schema.Data[t_name]
		if !ok {
			schema.Data[t_name] = &TDBTableData{
				Rows:    make(TDBTableRows),
				Indexes: make(TDBTableIndexes),
			}
			continue
		}
		for key, t_data := range t_data.Rows {
			if key > t_schema.IdTracker {
				t_schema.IdTracker = key
			}

			for f_name, field := range t_schema.Fields {
				field.Table = t_schema
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
func ValidateSchemaRelations(schema *Schema) error {
	for table_key, table := range schema.Tables {
		for field_key, field := range table.Fields {
			relation, is_relation := field.Properties[props.FieldPropRelation]
			if !is_relation {
				continue
			}

			rel_table_name, rel_field_name := parser.ParseRelationProp(relation.(string))

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
					vector_type, v_level := parser.ParseVectorProp(field.Properties[props.FieldPropVector].(string))
					if v_level > 1 {
						return invalidRelationError("nested vector fields cannot be relations")
					}
					if rel_field.BuiltinType != vector_type {
						return invalidRelationError("field types must match")
					}
				} else if rel_field.BuiltinType == types.FieldTypeVector {
					vector_type, _ := parser.ParseVectorProp(rel_field.Properties[props.FieldPropVector].(string))
					if field.BuiltinType != vector_type {
						return invalidRelationError("field types must match")
					}
				} else {
					return invalidRelationError("field types must match")
				}
			}

			// check vector types & levels are the same
			if field.BuiltinType == types.FieldTypeVector && rel_field.BuiltinType == types.FieldTypeVector {
				field_v_type, field_v_level := parser.ParseVectorProp(field.Properties[props.FieldPropVector].(string))
				rel_field_v_type, rel_field_v_level := parser.ParseVectorProp(rel_field.Properties[props.FieldPropVector].(string))

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
