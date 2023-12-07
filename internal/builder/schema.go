package builder

import (
	"fmt"
	"net/url"

	"github.com/tobsdb/tobsdb/internal/parser"
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"github.com/tobsdb/tobsdb/pkg"
	sorted "github.com/tobshub/go-sortedmap"
)

// Maps row field name to its saved data
type TDBTableRow = pkg.Map[string, any]

func GetPrimaryKey(r TDBTableRow) int {
	return pkg.NumToInt(r.Get(SYS_PRIMARY_KEY))
}

func SetPrimaryKey(r TDBTableRow, key int) {
	r.Set(SYS_PRIMARY_KEY, key)
}

// Maps row id to its saved data
type TDBTableRows = *sorted.SortedMap[int, TDBTableRow]

func NewTDBTableRows() TDBTableRows {
	return sorted.New[int, TDBTableRow](0, func(a, b TDBTableRow) bool {
		return GetPrimaryKey(a) < GetPrimaryKey(b)
	})
}

type (
	TDBTableIndexMap map[string]int
	// index field name -> index value -> row id
	TDBTableIndexes = pkg.Map[string, TDBTableIndexMap]
	TDBTableData    struct {
		Rows    TDBTableRows
		Indexes TDBTableIndexes
	}
	// Maps table name to its saved data
	TDBData = pkg.Map[string, *TDBTableData]
)

func formatIndexValue(v any) string {
	return fmt.Sprintf("%v", v)
}

func (m TDBTableIndexMap) Has(key any) bool {
	_, ok := m[formatIndexValue(key)]
	return ok
}

func (m TDBTableIndexMap) Get(key any) int {
	val, ok := m[formatIndexValue(key)]
	if !ok {
		return 0
	}
	return val
}

func (m TDBTableIndexMap) Set(key any, value int) {
	m[formatIndexValue(key)] = value
}

func (m TDBTableIndexMap) Delete(key any) {
	delete(m, formatIndexValue(key))
}

type Schema struct {
	Tables pkg.Map[string, *Table]
	// table_name -> row_id -> field_name -> value
	Data TDBData
}

const SYS_PRIMARY_KEY = "__tdb_id__"

func NewSchemaFromString(input string, data TDBData, build_only bool) (*Schema, error) {
	if len(input) == 0 {
		return nil, fmt.Errorf("No schema provided")
	}

	schema, err := ParseSchema(input)
	if err != nil {
		return nil, err
	}

	if build_only {
		return schema, nil
	}

	if data == nil {
		schema.Data = make(TDBData)
	} else {
		schema.Data = data
	}

	for t_name, t_schema := range schema.Tables {
		if !schema.Data.Has(t_name) {
			schema.Data[t_name] = &TDBTableData{
				Rows:    NewTDBTableRows(),
				Indexes: make(TDBTableIndexes),
			}
			continue
		}
		iterCh, err := schema.Data.Get(t_name).Rows.IterCh()
		if err != nil {
			continue
		}
		for rec := range iterCh.Records() {
			if rec.Key > t_schema.IdTracker {
				t_schema.IdTracker = rec.Key
			}

			for f_name, field := range t_schema.Fields {
				if field.BuiltinType != types.FieldTypeInt {
					continue
				}

				if default_val := field.Properties.Get(props.FieldPropDefault); default_val != nil {
					if default_val != "autoincrement" {
						continue
					}
				}

				_f_data := rec.Val.Get(f_name)
				if _f_data == nil {
					continue
				}

				f_data := pkg.NumToInt(_f_data)
				if f_data > field.IncrementTracker {
					field.IncrementTracker = f_data
				}
				t_schema.Fields.Set(f_name, field)
			}
		}
		schema.Tables.Set(t_name, t_schema)
	}
	return schema, nil
}

func NewSchemaFromURL(input *url.URL, data TDBData, build_only bool) (*Schema, error) {
	params, err := url.ParseQuery(input.RawQuery)
	if err != nil {
		return nil, err
	}
	schema_data := params.Get("schema")
	return NewSchemaFromString(schema_data, data, build_only)
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
			if !field.Properties.Has(props.FieldPropRelation) {
				continue
			}
			rel_table_name, rel_field_name := parser.ParseRelationProp(field.Properties.Get(props.FieldPropRelation).(string))

			invalidRelationError := ThrowInvalidRelationError(table_key, rel_table_name, field_key)

			if !schema.Tables.Has(rel_table_name) {
				return invalidRelationError(fmt.Sprintf("%s is not a valid table", rel_table_name))
			}

			rel_table := schema.Tables.Get(rel_table_name)

			if !rel_table.Fields.Has(rel_field_name) {
				return invalidRelationError(
					fmt.Sprintf("%s is not a valid field on table %s", rel_field_name, rel_table_name),
				)
			}

			rel_field := rel_table.Fields.Get(rel_field_name)

			if rel_field.BuiltinType != field.BuiltinType {
				// check vector <-> non-vector relations
				if field.BuiltinType == types.FieldTypeVector {
					vector_type, v_level := parser.ParseVectorProp(field.Properties.Get(props.FieldPropVector).(string))
					if v_level > 1 {
						return invalidRelationError("nested vector fields cannot be relations")
					}
					if rel_field.BuiltinType != vector_type {
						return invalidRelationError("field types must match")
					}
				} else if rel_field.BuiltinType == types.FieldTypeVector {
					vector_type, _ := parser.ParseVectorProp(rel_field.Properties.Get(props.FieldPropVector).(string))
					if field.BuiltinType != vector_type {
						return invalidRelationError("field types must match")
					}
				} else {
					return invalidRelationError("field types must match")
				}
			}

			// check vector types & levels are the same
			if field.BuiltinType == types.FieldTypeVector && rel_field.BuiltinType == types.FieldTypeVector {
				field_v_type, field_v_level := parser.ParseVectorProp(field.Properties.Get(props.FieldPropVector).(string))
				rel_field_v_type, rel_field_v_level := parser.ParseVectorProp(rel_field.Properties.Get(props.FieldPropVector).(string))

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
