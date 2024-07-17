package builder_test

import (
	"encoding/gob"
	"encoding/json"
	"testing"

	"github.com/tobsdb/tobsdb/internal/auth"
	. "github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/internal/query"
	"gotest.tools/assert"
)

func TestCheckUserAccess(t *testing.T) {
	s := &Schema{}
	u := auth.NewUser("test", "password")
	s.AddUser(u, auth.TdbUserRole(0))
	assert.Equal(t, s.CheckUserAccess(u), auth.TdbUserRole(0))
}

func TestParseSchema(t *testing.T) {
	t.Run("simple parse", func(t *testing.T) {
		s, err := ParseSchema("$TABLE a {\n a Int\n }")
		assert.NilError(t, err)
		assert.Equal(t, s.Tables.Len(), 1, "expected only one table")
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
		assert.Equal(t, s.Tables.Len(), 2, "expected two tables")
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
    id Int
}

$TABLE b {
    id Int relation(a.id)
}
        `)
		assert.NilError(t, err)
	})

	t.Run("good self relation", func(t *testing.T) {
		_, err := ParseSchema(`
$TABLE a {
    a Int
    b Int relation(a.a)
}
        `)
		assert.NilError(t, err)
	})

	t.Run("bad self relation", func(t *testing.T) {
		_, err := ParseSchema(`
$TABLE a {
    a Int relation(a.a)
}
        `)
		assert.ErrorContains(t, err, "invalid self-relation")
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

	data, err := s.MetaData()
	assert.NilError(t, err)

	var new_s Schema
	json.Unmarshal(data, &new_s)

	new_table := new_s.Tables.Get("a")
	assert.Assert(t, new_table != nil)
}

func TestTableIndexesToBytes(t *testing.T) {
	s, err := NewSchemaFromString(`
$TABLE a {
    a Int key(primary)
    b String unique(true)
}
        `, nil, false)
	assert.NilError(t, err)

	table := s.Tables.Get("a")
	row, err := query.Create(table, query.QueryArg{"b": "b"})
	assert.NilError(t, err)

	table_data, err := table.IndexBytes()
	assert.NilError(t, err)
	indexes := TdbIndexesBuilder{}
	err = gob.NewDecoder(table_data.IndexBuf).Decode(&indexes.Indexes)
	assert.NilError(t, err)
	err = gob.NewDecoder(table_data.PrimaryIndexBuf).Decode(&indexes.PrimaryIndexes)
	assert.NilError(t, err)
	assert.Equal(t, indexes.Indexes.Get("b").Get("b"), GetPrimaryKey(row))
}
