package builder_test

import (
	"encoding/json"
	"testing"

	. "github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/internal/query"
	"gotest.tools/assert"
)

func TestParseSchema(t *testing.T) {
	t.Run("simple parse", func(t *testing.T) {
		s, err := ParseSchema("$TABLE a {\n a Int\n }")
		assert.NilError(t, err)
		assert.Equal(t, len(s.Tables), 1, "expected only one table")
	})

	t.Run("parse indexes", func(t *testing.T) {
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
		assert.Assert(t, s.Tables.Has("a"), "expected table a")

		table_a := s.Tables.Get("a")
		assert.Equal(t, len(table_a.Indexes), 2, "expected to indexes")
		assert.Equal(t, table_a.PrimaryKey().Name, "a", "expected a primary key named 'a'")
	})

	t.Run("duplicate table", func(t *testing.T) {
		_, err := ParseSchema(`
$TABLE a {
    a Int
}

$TABLE a {
    b Int
}
        `)

		assert.ErrorContains(t, err, "Duplicate table a")
	})

	t.Run("duplicate field", func(t *testing.T) {
		_, err := ParseSchema(`
$TABLE a {
    a Int
    a String
}
        `)

		assert.ErrorContains(t, err, "Duplicate field a")
	})

	t.Run("multiple primary key", func(t *testing.T) {
		_, err := ParseSchema(`
$TABLE a {
    a Int key(primary)
    b Int key(primary)
}
        `)
		assert.ErrorContains(t, err, "Table can't have multiple primary keys")
	})

	t.Run("simple relation", func(t *testing.T) {
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
	})

	t.Run("vector vector relation", func(t *testing.T) {
		_, err := ParseSchema(`
$TABLE a {
    id Vector vector(Int)
}

$TABLE b {
    arr Vector vector(Int) relation(a.id)
}
        `)
		assert.NilError(t, err)
	})

	t.Run("vector non vector relation", func(t *testing.T) {
		_, err := ParseSchema(`
$TABLE a {
    id Int
}

$TABLE b {
    arr Vector vector(Int) relation(a.id)
}
        `)
		assert.NilError(t, err)
	})

	t.Run("relation table absent", func(t *testing.T) {
		_, err := ParseSchema(`
$TABLE a {
    id Int relation(b.id)
}
        `)
		assert.ErrorContains(t, err, "b is not a valid table")
	})

	t.Run("relation field absent", func(t *testing.T) {
		_, err := ParseSchema(`
$TABLE a {
    id Int
}

$TABLE b {
    id Int relation(a.field)
}
        `)
		assert.ErrorContains(t, err, "field is not a valid field on table a")
	})

	t.Run("relation type mismatch", func(t *testing.T) {
		_, err := ParseSchema(`
$TABLE a {
    id Int
}

$TABLE b {
    id String relation(a.id)
}
        `)
		assert.ErrorContains(t, err, "field types must match")
	})

	t.Run("vector relation type mismatch", func(t *testing.T) {
		_, err := ParseSchema(`
$TABLE a {
    id Vector vector(String)
}

$TABLE b {
    arr Vector vector(Int) relation(a.id)
}
        `)

		assert.ErrorContains(t, err, "field types must match")
	})

	t.Run("vector non vector relation type mismatch", func(t *testing.T) {
		_, err := ParseSchema(`
$TABLE a {
    id Int
}

$TABLE b {
    arr Vector vector(String) relation(a.id)
}
        `)

		assert.ErrorContains(t, err, "field types must match")
	})
}

func TestSchemaJSON(t *testing.T) {
	s, err := NewSchemaFromString(`
$TABLE a {
    a Int
    b String
}
        `, nil, false)
	assert.NilError(t, err)

	query.Create(
		s.Tables.Get("a"),
		query.QueryArg{"a": map[string]any{"a": 69, "b": "hello world"}},
	)

	data, err := json.Marshal(s)
	assert.NilError(t, err)

	var new_s Schema
	json.Unmarshal(data, &new_s)

	new_table := new_s.Tables.Get("a")
	new_table.Schema = &new_s
	table := s.Tables.Get("a")

	assert.DeepEqual(t, new_table.Row(1), table.Row(1))
	assert.DeepEqual(t, new_table.Rows().Map.Idx, table.Rows().Map.Idx)
	assert.DeepEqual(t, new_table.Rows().Map.Sorted, table.Rows().Map.Sorted)
}
