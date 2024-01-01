package builder_test

import (
	"fmt"
	"testing"
	"time"

	. "github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"gotest.tools/assert"
)

func TestCheckFieldRules(t *testing.T) {
	t.Run("non int primary key", func(t *testing.T) {
		f := Field{
			Name:        "a",
			BuiltinType: types.FieldTypeString,
			Properties:  map[props.FieldProp]any{props.FieldPropKey: props.KeyPropPrimary},
		}
		err := CheckFieldRules(&f)
		assert.ErrorContains(t, err, "field(a String key(primary)) must be type Int")
	})

	t.Run("optional primary key", func(t *testing.T) {
		f := Field{
			Name:        "a",
			BuiltinType: types.FieldTypeInt,
			Properties: map[props.FieldProp]any{
				props.FieldPropKey:      props.KeyPropPrimary,
				props.FieldPropOptional: true,
			},
		}
		err := CheckFieldRules(&f)
		assert.ErrorContains(t, err, "field(a Int key(primary)) cannot be optional")
	})

	t.Run("default prop on bytes field", func(t *testing.T) {
		f := Field{
			Name:        "a",
			BuiltinType: types.FieldTypeBytes,
			Properties: map[props.FieldProp]any{
				props.FieldPropDefault: "0110111",
			},
		}
		err := CheckFieldRules(&f)
		assert.ErrorContains(t, err, "field(a Bytes) cannot have default prop")
	})

	t.Run("unique prop on vector field", func(t *testing.T) {
		f := Field{
			Name:        "a",
			BuiltinType: types.FieldTypeVector,
			Properties: map[props.FieldProp]any{
				props.FieldPropUnique: true,
			},
		}
		err := CheckFieldRules(&f)
		assert.ErrorContains(t, err, "field(a Vector) cannot have unique prop")
	})

	t.Run("missing vector prop on vector field", func(t *testing.T) {
		f := Field{
			Name:        "a",
			BuiltinType: types.FieldTypeVector,
			Properties: map[props.FieldProp]any{
				props.FieldPropUnique: false,
			},
		}
		err := CheckFieldRules(&f)
		assert.ErrorContains(t, err, "field(a Vector) must have vector prop")
	})

	t.Run("vector prop on non vector field", func(t *testing.T) {
		f := Field{
			Name:        "a",
			BuiltinType: types.FieldTypeInt,
			Properties: map[props.FieldProp]any{
				props.FieldPropVector: "String, 2",
			},
		}
		err := CheckFieldRules(&f)
		assert.ErrorContains(t, err, "field(a Int) cannot have vector prop")
	})

	t.Run("vector prop with vector type", func(t *testing.T) {
		f := Field{
			Name:        "a",
			BuiltinType: types.FieldTypeVector,
			Properties: map[props.FieldProp]any{
				props.FieldPropVector: "Vector",
			},
		}
		err := CheckFieldRules(&f)
		assert.ErrorContains(t, err, "vector(Vector) is not allowed")
	})
}

func TestFieldValidateType(t *testing.T) {
	t.Run("type mismatch", func(t *testing.T) {
		f := Field{Name: "a"}
		cases := []struct {
			Type  types.FieldType
			Props map[props.FieldProp]any
			Input any
		}{
			{types.FieldTypeInt, nil, "a"},
			{types.FieldTypeString, nil, 1},
			{types.FieldTypeBool, nil, "truthy"},
			{types.FieldTypeBytes, nil, 0110111},
			{types.FieldTypeVector, map[props.FieldProp]any{props.FieldPropVector: "Int"}, "[1, 2, 3]"},
			{types.FieldTypeDate, nil, true},
		}

		for _, tt := range cases {
			f.BuiltinType = tt.Type
			f.Properties = tt.Props
			_, err := f.ValidateType(tt.Input, false)
			assert.ErrorContains(t, err, "Invalid field type",
				fmt.Sprintf("Unexpected validation result for %v", tt))
		}
	})

	t.Run("vector level mismatch", func(t *testing.T) {
		f := Field{
			Name:        "a",
			BuiltinType: types.FieldTypeVector,
			Properties: map[props.FieldProp]any{
				props.FieldPropVector: "Int, 2",
			},
		}
		_, err := f.ValidateType([]int{1, 2, 3}, false)
		assert.ErrorContains(t, err, "Invalid field type")
	})

	t.Run("date now", func(t *testing.T) {
		f := Field{
			Name:        "a",
			BuiltinType: types.FieldTypeDate,
			Properties: map[props.FieldProp]any{
				props.FieldPropDefault: "now",
			},
		}
		d, err := f.ValidateType(nil, true)
		_, ok := d.(time.Time)
		assert.NilError(t, err)
		assert.Assert(t, ok)
	})
}
