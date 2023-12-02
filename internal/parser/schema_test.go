package parser_test

import (
	"testing"

	. "github.com/tobsdb/tobsdb/internal/parser"
	"github.com/tobsdb/tobsdb/internal/types"
	"gotest.tools/assert"
)

func TestLineParserStart(t *testing.T) {
	state, data, err := LineParser("$TABLE a {")

	assert.NilError(t, err)
	assert.Equal(t, state, ParserStateTableStart)
	assert.Equal(t, data.Name, "a")
}

func TestLineParserStartMissingName(t *testing.T) {
	state, _, err := LineParser("$TABLE {")

	assert.ErrorContains(t, err, "Invalid line")
	assert.Equal(t, state, ParserStateIdle)
}

func TestLineParserStartMissingBracket(t *testing.T) {
	state, _, err := LineParser("$TABLE a")

	assert.ErrorContains(t, err, "Invalid line")
	assert.Equal(t, state, ParserStateIdle)
}

func TestLineParserStartSpaceInName(t *testing.T) {
	state, _, err := LineParser("$TABLE a b {")

	assert.ErrorContains(t, err, "Table name cannot include space")
	assert.Equal(t, state, ParserStateIdle)
}

func TestLineParserEnd(t *testing.T) {
	state, _, err := LineParser("}")

	assert.NilError(t, err)
	assert.Equal(t, state, ParserStateTableEnd)
}

func TestLineParserField(t *testing.T) {
	state, data, err := LineParser("a Int unique(true)")

	assert.NilError(t, err)
	assert.Equal(t, state, ParserStateNewField)
	assert.Equal(t, data.Name, "a")
	assert.Equal(t, data.Builtin_type, types.FieldTypeInt)
}

func TestLineParserFieldMissingType(t *testing.T) {
	state, _, err := LineParser("a")

	assert.ErrorContains(t, err, "Field a does not have a type")
	assert.Equal(t, state, ParserStateIdle)
}

func TestLineParserFieldUnknownType(t *testing.T) {
	state, _, err := LineParser("a Number")

	assert.ErrorContains(t, err, "Invalid field type: Number")
	assert.Equal(t, state, ParserStateIdle)
}

func TestLineParserFieldUnknownProp(t *testing.T) {
	state, _, err := LineParser("a Int x(true)")

	assert.ErrorContains(t, err, "Invalid field prop: x")
	assert.Equal(t, state, ParserStateIdle)
}

func TestLineParserFieldEmptyProp(t *testing.T) {
	state, _, err := LineParser("a Int unique()")

	assert.ErrorContains(t, err, "No value for prop: unique")
	assert.Equal(t, state, ParserStateIdle)
}

func TestLineParserFieldInvalidPropValue(t *testing.T) {
	state, _, err := LineParser("a Int optional(x)")

	assert.ErrorContains(t, err, "optional(x) is not a valid prop")
	assert.Equal(t, state, ParserStateIdle)
}
