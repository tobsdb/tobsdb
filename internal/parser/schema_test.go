package parser_test

import (
	"testing"

	. "github.com/tobsdb/tobsdb/internal/parser"
	"github.com/tobsdb/tobsdb/internal/types"
	"gotest.tools/assert"
)

func TestLineParser(t *testing.T) {
	t.Run("table declaration", func(t *testing.T) {
		state, data, err := LineParser("$TABLE a {")

		assert.NilError(t, err)
		assert.Equal(t, state, ParserStateTableStart)
		assert.Equal(t, data.Name, "a")
	})

	t.Run("table missing name", func(t *testing.T) {
		state, _, err := LineParser("$TABLE {")

		assert.ErrorContains(t, err, "Invalid line")
		assert.Equal(t, state, ParserStateIdle)
	})

	t.Run("table declaration missing opening bracket", func(t *testing.T) {
		state, _, err := LineParser("$TABLE a")

		assert.ErrorContains(t, err, "Invalid line")
		assert.Equal(t, state, ParserStateIdle)
	})

	t.Run("table name with space", func(t *testing.T) {
		state, _, err := LineParser("$TABLE a b {")

		assert.ErrorContains(t, err, "Table name cannot include space")
		assert.Equal(t, state, ParserStateIdle)
	})

	t.Run("table name invalid character", func(t *testing.T) {
		state, _, err := LineParser("$TABLE a-b {")

		assert.ErrorContains(t, err, "Table name contains invalid characters")
		assert.Equal(t, state, ParserStateIdle)
	})

	t.Run("table declaration end", func(t *testing.T) {
		state, _, err := LineParser("}")

		assert.NilError(t, err)
		assert.Equal(t, state, ParserStateTableEnd)
	})

	t.Run("field declaration", func(t *testing.T) {
		state, data, err := LineParser("a Int unique(true)")

		assert.NilError(t, err)
		assert.Equal(t, state, ParserStateNewField)
		assert.Equal(t, data.Name, "a")
		assert.Equal(t, data.Builtin_type, types.FieldTypeInt)
	})

	t.Run("field name invalid character", func(t *testing.T) {
		state, _, err := LineParser("a-b Int")

		assert.ErrorContains(t, err, "Field name contains invalid characters")
		assert.Equal(t, state, ParserStateIdle)
	})

	t.Run("field declaration without type", func(t *testing.T) {
		state, _, err := LineParser("a")

		assert.ErrorContains(t, err, "Field a does not have a type")
		assert.Equal(t, state, ParserStateIdle)
	})

	t.Run("field declaration with unknown type", func(t *testing.T) {
		state, _, err := LineParser("a Number")

		assert.ErrorContains(t, err, "Invalid field type: Number")
		assert.Equal(t, state, ParserStateIdle)
	})

	t.Run("unknown field prop", func(t *testing.T) {
		state, _, err := LineParser("a Int x(true)")

		assert.ErrorContains(t, err, "Invalid field prop: x")
		assert.Equal(t, state, ParserStateIdle)
	})

	t.Run("field prop with no value", func(t *testing.T) {
		state, _, err := LineParser("a Int unique()")

		assert.ErrorContains(t, err, "No value for prop: unique")
		assert.Equal(t, state, ParserStateIdle)
	})

	t.Run("invalid field prop value", func(t *testing.T) {
		state, _, err := LineParser("a Int optional(x)")

		assert.ErrorContains(t, err, "optional(x) is not a valid prop")
		assert.Equal(t, state, ParserStateIdle)
	})
}
