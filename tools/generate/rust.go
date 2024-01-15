package generate

import (
	"fmt"

	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"github.com/tobsdb/tobsdb/pkg"
)

func SchemaToRust(s []ParsedTable) []byte {
	res := `use tobsdb::types::*;
use serde::{Deserialize, Serialize};
`

	for _, t := range s {
		table := fmt.Sprintf("\n#[derive(Serialize, Deserialize)]\npub struct %s {\n%s\n}\n",
			toPascalCase(t.Name), fieldsToRust(t.Fields))
		res += table
	}

	return []byte(res)
}

func fieldsToRust(fields []ParsedField) string {
	res := ""
	for i, f := range fields {
		res += fmt.Sprintf("\tpub %s: %s;",
			f.Name, tdbTypeToRust(f.BuiltinType, f.Properties))
		if i < len(fields)-1 {
			res += "\n"
		}
	}
	return res
}

func tdbTypeToRust(t types.FieldType, p pkg.Map[props.FieldProp, any]) string {
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
			res = fmt.Sprintf("TdbVector<%s>",
				tdbTypeToRust(types.FieldTypeVector, pkg.Map[props.FieldProp, any]{
					props.FieldPropVector: fmt.Sprintf("%s, %d", t, level-1),
				}))
		} else {
			res = fmt.Sprintf("TdbVector<%s>", tdbTypeToRust(t, p))
		}
	}

	if p.Has(props.FieldPropDefault) {
		res = fmt.Sprintf("Option<%s>", res)
	} else if p.Has(props.FieldPropOptional) && p.Get(props.FieldPropOptional).(bool) {
		res = fmt.Sprintf("Option<%s>", res)
	}

	return res
}
