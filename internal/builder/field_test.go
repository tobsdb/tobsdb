package builder_test

import (
	"testing"

	. "github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/internal/parser"
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"gotest.tools/assert"
)

func TestNonIntPrimaryKey(t *testing.T) {
	f := parser.Field{
		Name:        "a",
		BuiltinType: types.FieldTypeString,
		Properties:  map[props.FieldProp]any{props.FieldPropKey: props.KeyPropPrimary},
	}
	err := CheckFieldRules(&f)
	assert.ErrorContains(t, err, "field(a String key(primary)) must be type Int")
}

func TestOptionalPrimaryKey(t *testing.T) {
	f := parser.Field{
		Name:        "a",
		BuiltinType: types.FieldTypeInt,
		Properties: map[props.FieldProp]any{
			props.FieldPropKey:      props.KeyPropPrimary,
			props.FieldPropOptional: true,
		},
	}
	err := CheckFieldRules(&f)
	assert.ErrorContains(t, err, "field(a Int key(primary)) cannot be optional")
}

func TestDefaultBytesField(t *testing.T) {
	f := parser.Field{
		Name:        "a",
		BuiltinType: types.FieldTypeBytes,
		Properties: map[props.FieldProp]any{
			props.FieldPropDefault: "0110111",
		},
	}
	err := CheckFieldRules(&f)
	assert.ErrorContains(t, err, "field(a Bytes) cannot have default prop")
}

func TestCheckVectorField(t *testing.T) {
	f := parser.Field{
		Name:        "a",
		BuiltinType: types.FieldTypeVector,
		Properties: map[props.FieldProp]any{
			props.FieldPropUnique: true,
		},
	}
	err := CheckFieldRules(&f)
	assert.ErrorContains(t, err, "field(a Vector) cannot have unique prop")

	f = parser.Field{
		Name:        "a",
		BuiltinType: types.FieldTypeVector,
		Properties: map[props.FieldProp]any{
			props.FieldPropUnique: false,
		},
	}
	err = CheckFieldRules(&f)
	assert.ErrorContains(t, err, "field(a Vector) must have vector prop")
}

func TestCheckVectorProp(t *testing.T) {
	f := parser.Field{
		Name:        "a",
		BuiltinType: types.FieldTypeInt,
		Properties: map[props.FieldProp]any{
			props.FieldPropVector: "String, 2",
		},
	}
	err := CheckFieldRules(&f)
	assert.ErrorContains(t, err, "field(a Int) cannot have vector prop")
}

func TestCheckVectorPropWithVectorType(t *testing.T) {
	f := parser.Field{
		Name:        "a",
		BuiltinType: types.FieldTypeVector,
		Properties: map[props.FieldProp]any{
			props.FieldPropVector: "Vector",
		},
	}
	err := CheckFieldRules(&f)
	assert.ErrorContains(t, err, "vector(Vector) is not allowed")
}
