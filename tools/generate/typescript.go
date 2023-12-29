package generate

import (
	"fmt"

	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"github.com/tobsdb/tobsdb/pkg"
)

func SchemaToTypescript(s []JsonTable) []byte {
	res := `import { PrimaryKey, Unique, Default } from "tobsdb";

export type Schema = {
`
	for _, t := range s {
		table := fmt.Sprintf("\t%s: {\n%s\n\t};\n", t.Name, fieldsToTypescript(t.Fields))
		res += table
	}
	res += "\n}"
	return []byte(res)
}

func fieldsToTypescript(fields []JsonField) string {
	res := ""
	for i, f := range fields {
		res += fmt.Sprintf("\t\t%s%s: %s;",
			f.Name, typescriptOptional(f.Properties),
			tdbTypeToTypescript(f.BuiltinType, f.Properties))
		if i < len(fields)-1 {
			res += "\n"
		}
	}
	return res
}

func typescriptOptional(p pkg.Map[props.FieldProp, any]) string {
	if p.Has(props.FieldPropOptional) && p.Get(props.FieldPropOptional).(bool) {
		return "?"
	}
	return ""
}

func tdbTypeToTypescript(t types.FieldType, p pkg.Map[props.FieldProp, any]) string {
	res := ""
	switch t {
	case types.FieldTypeInt:
		res = "number"
	case types.FieldTypeFloat:
		res = "number"
	case types.FieldTypeString:
		res = "string"
	case types.FieldTypeBool:
		res = "boolean"
	case types.FieldTypeDate:
		res = "Date"
	case types.FieldTypeBytes:
		res = "Buffer"
	case types.FieldTypeVector:
		t, level, _ := props.ParseVectorPropSafe(p.Get(props.FieldPropVector).(string))
		if level > 1 {
			res = fmt.Sprintf("%s[]",
				tdbTypeToTypescript(types.FieldTypeVector, pkg.Map[props.FieldProp, any]{
					props.FieldPropVector: fmt.Sprintf("%s, %d", t, level-1),
				}))
		} else {
			res = fmt.Sprintf("%s[]", tdbTypeToTypescript(t, p))
		}
	}

	if p.Has(props.FieldPropKey) && p.Get(props.FieldPropKey) == "primary" {
		res = fmt.Sprintf("PrimaryKey<%s>", res)
	}

	if p.Has(props.FieldPropUnique) && p.Get(props.FieldPropUnique).(bool) {
		res = fmt.Sprintf("Unique<%s>", res)
	}

	if p.Has(props.FieldPropDefault) {
		res = fmt.Sprintf("Default<%s>", res)
	}

	return res
}
