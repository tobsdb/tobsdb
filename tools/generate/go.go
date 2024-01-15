package generate

import (
	"fmt"

	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"github.com/tobsdb/tobsdb/pkg"
)

func SchemaToGo(s []ParsedTable) []byte {
	res := `package schema

import . "github.com/tobsdb/tobsdb/tools/client/go"
`

	for _, t := range s {
		table := fmt.Sprintf("\ntype %s struct {\n%s\n}\n",
			toPascalCase(t.Name), fieldsToGo(t.Fields))
		res += table
	}

	return []byte(res)
}

func fieldsToGo(fields []ParsedField) string {
	res := ""
	for i, f := range fields {
		res += fmt.Sprintf("\t%s %s", toPascalCase(f.Name),
			tdbTypeToGo(f.BuiltinType, f.Properties))
		if i < len(fields)-1 {
			res += "\n"
		}
	}
	return res
}

func tdbTypeToGo(t types.FieldType, p pkg.Map[props.FieldProp, any]) string {
	res := ""
	switch t {
	case types.FieldTypeInt:
		res = "TdbInt"
	case types.FieldTypeFloat:
		res = "TdbFloat"
	case types.FieldTypeString:
		res = "TdbString"
	case types.FieldTypeBool:
		res = "TdbBool"
	case types.FieldTypeDate:
		res = "TdbDate"
	case types.FieldTypeBytes:
		res = "TdbBytes"
	case types.FieldTypeVector:
		t, level, _ := props.ParseVectorPropSafe(p.Get(props.FieldPropVector).(string))
		if level > 1 {
			res = fmt.Sprintf("TdbVector[%s]",
				tdbTypeToGo(types.FieldTypeVector, pkg.Map[props.FieldProp, any]{
					props.FieldPropVector: fmt.Sprintf("%s, %d", t, level-1),
				}))
		} else {
			res = fmt.Sprintf("TdbVector[%s]", tdbTypeToGo(t, p))
		}
	}

	return res
}
