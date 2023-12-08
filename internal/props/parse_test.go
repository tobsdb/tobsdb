package props_test

import (
	"testing"

	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"gotest.tools/assert"
)

func TestParseRelationPropSafe(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		table, field, err := props.ParseRelationPropSafe("table.field")
		assert.NilError(t, err)
		assert.Equal(t, "table", table)
		assert.Equal(t, "field", field)
	})

	t.Run("bad syntax", func(t *testing.T) {
		_, _, err := props.ParseRelationPropSafe("table:field")
		assert.ErrorContains(t, err, "Invalid syntax: relation(table:field)")
	})

	t.Run("missing field", func(t *testing.T) {
		_, _, err := props.ParseRelationPropSafe("table.")
		assert.ErrorContains(t, err, "Invalid syntax: relation(table.)")
	})
}

func TestParseVectorPropSafe(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		v_type, v_level, err := props.ParseVectorPropSafe("String, 4")
		assert.NilError(t, err)
		assert.Equal(t, v_type, types.FieldTypeString)
		assert.Equal(t, v_level, 4)
	})

	t.Run("bad syntax", func(t *testing.T) {
		_, _, err := props.ParseVectorPropSafe("String, 4, 1")
		assert.ErrorContains(t, err, "Invalid syntax: vector(String, 4, 1)")
	})

	t.Run("invalid type", func(t *testing.T) {
		_, _, err := props.ParseVectorPropSafe("Number")
		assert.ErrorContains(t, err, "Number is not a valid type")
	})

	t.Run("invalid level format", func(t *testing.T) {
		_, _, err := props.ParseVectorPropSafe("String, one")
		assert.ErrorContains(t, err, "vector(String, one) is not a valid prop")
	})

	t.Run("invalid level value", func(t *testing.T) {
		_, _, err := props.ParseVectorPropSafe("Int, 0")
		assert.ErrorContains(t, err, "vector(Int, 0) is not a valid prop")
	})
}
