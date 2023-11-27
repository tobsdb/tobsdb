package builder_test

import (
	"testing"

	. "github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/internal/parser"
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"gotest.tools/assert"
)

func TestParseSchema(t *testing.T) {
	s, err := ParseSchema("$TABLE a {\n a Int\n }")
	assert.NilError(t, err)
	assert.Equal(t, len(s.Tables), 1, "expected only one table")
}

func TestParseSchemaIndexes(t *testing.T) {
	s, err := ParseSchema(`
$TABLE a {
    a Int key(primary)
    b String unique(true)
    c Bytes
}

$TABLE b {
    d Vector vector(String)
}
        `)
	assert.NilError(t, err)
	assert.Equal(t, len(s.Tables), 2, "expected two tables")
	table_a, ok := s.Tables["a"]
	assert.Assert(t, ok, "expected table a")
	assert.Equal(t, len(table_a.Indexes), 2, "expected to indexes")
	assert.Equal(t, table_a.PrimaryKey().Name, "a", "expected a primary key named 'a'")
}

func TestDuplicateTable(t *testing.T) {
	_, err := ParseSchema(`
$TABLE a {
    a Int
}

$TABLE a {
    b Int
}
        `)

	assert.ErrorContains(t, err, "Duplicate table a")
}

func TestDuplicateField(t *testing.T) {
	_, err := ParseSchema(`
$TABLE a {
    a Int
    a String
}
        `)

	assert.ErrorContains(t, err, "Duplicate field a")
}

func TestMultiplePrimaryKey(t *testing.T) {
	_, err := ParseSchema(`
$TABLE a {
    a Int key(primary)
    b Int key(primary)
}
        `)
	assert.ErrorContains(t, err, "Table can't have multiple primary keys")
}

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

func TestSimpleRelation(t *testing.T) {
	_, err := ParseSchema(`
$TABLE a {
    // self relation
    id Int relation(a.id)
}

$TABLE b {
    id Int relation(a.id)
}
        `)
	assert.NilError(t, err)
}

func TestRelationTableAbsent(t *testing.T) {
	_, err := ParseSchema(`
$TABLE a {
    id Int relation(b.id)
}
        `)
	assert.ErrorContains(t, err, "b is not a valid table")
}

func TestRelationFieldAbsent(t *testing.T) {
	_, err := ParseSchema(`
$TABLE a {
    id Int
}

$TABLE b {
    id Int relation(a.field)
}
        `)
	assert.ErrorContains(t, err, "field is not a valid field on table a")
}

func TestRelationTypeMismatch(t *testing.T) {
	_, err := ParseSchema(`
$TABLE a {
    id Int
}

$TABLE b {
    id String relation(a.id)
}
        `)
	assert.ErrorContains(t, err, "field types must match")
}
