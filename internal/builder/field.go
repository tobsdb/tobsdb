package builder

import (
	"fmt"
	"strconv"

	"github.com/tobsdb/tobsdb/internal/parser"
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
)

// field local rules:
// - primary key field must be type int
// - can't have key primary and optional prop true
// - can't have Vector type and unique prop true
// - can't have Vector/Bytes type and default prop
// - can't have vector prop on non-vector type
func CheckFieldRules(field *parser.Field) error {
	if key, ok := field.Properties[props.FieldPropKey]; ok && key == props.KeyPropPrimary {
		if field.BuiltinType != types.FieldTypeInt {
			return fmt.Errorf("field(%s %s key(primary)) must be type Int", field.Name, field.BuiltinType)
		}

		if _opt, ok := field.Properties[props.FieldPropOptional]; ok {
			opt, _ := strconv.ParseBool(_opt)
			if opt {
				return fmt.Errorf("field(%s %s key(primary)) cannot be optional", field.Name, field.BuiltinType)
			}
		}
	}

	if field.BuiltinType == types.FieldTypeVector || field.BuiltinType == types.FieldTypeBytes {
		if _, ok := field.Properties[props.FieldPropDefault]; ok {
			return fmt.Errorf("field(%s %s) cannot have default prop", field.Name, field.BuiltinType)
		}
	}

	if field.BuiltinType == types.FieldTypeVector {
		if _unique, ok := field.Properties[props.FieldPropUnique]; ok {
			unique, _ := strconv.ParseBool(_unique)
			if unique {
				return fmt.Errorf("field(%s %s) cannot have unique prop", field.Name, field.BuiltinType)
			}
		}
	}

	if _, ok := field.Properties[props.FieldPropVector]; ok {
		if field.BuiltinType != types.FieldTypeVector {
			return fmt.Errorf("field(%s %s) cannot have vector prop", field.Name, field.BuiltinType)
		}
	}

	return nil
}
