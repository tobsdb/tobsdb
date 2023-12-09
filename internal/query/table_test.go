package query_test

import (
	"net/http"
	"sync"
	"testing"

	"github.com/tobsdb/tobsdb/internal/builder"
	. "github.com/tobsdb/tobsdb/internal/query"
	"gotest.tools/assert"
)

func TestCreate(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		schema, _ := builder.NewSchemaFromString(`
$TABLE a {
    b String unique(true)
}
        `, nil, false)
		table := schema.Tables.Get("a")
		row, err := Create(table, QueryArg{"b": "hello"})

		assert.NilError(t, err)
		assert.Equal(t, row.Get("b"), "hello")
		assert.Equal(t, table.IndexMap("b").Get("hello"), builder.GetPrimaryKey(row))
	})

	t.Run("create with primary key", func(t *testing.T) {
		schema, _ := builder.NewSchemaFromString(`
$TABLE a {
    b Int key(primary)
}
        `, nil, false)
		table := schema.Tables.Get("a")
		Create(table, QueryArg{})
		row, err := Create(table, QueryArg{})

		assert.NilError(t, err)
		assert.Equal(t, row.Get("b"), builder.GetPrimaryKey(row))
		assert.DeepEqual(t, table.IndexMap("b"), (*builder.TDBTableIndexMap)(nil))
	})

	t.Run("duplicate unique field", func(t *testing.T) {
		schema, _ := builder.NewSchemaFromString(`
$TABLE a {
    b String unique(true)
}
        `, nil, false)
		table := schema.Tables.Get("a")
		Create(table, QueryArg{"b": "hello"})

		_, err := Create(table, QueryArg{"b": "hello"})
		assert.ErrorContains(t, err, "already exists")
	})
}

func TestUpdate(t *testing.T) {
	t.Run("update", func(t *testing.T) {
		schema, _ := builder.NewSchemaFromString(`
$TABLE a {
    b String unique(true)
    c Int optional(true)
}
        `, nil, false)
		table := schema.Tables.Get("a")
		row, _ := Create(table, QueryArg{"b": "hello"})

		assert.Equal(t, row.Get("b"), "hello")

		new_row, err := Update(table, row, QueryArg{"c": 69})

		assert.NilError(t, err)
		assert.Equal(t, new_row.Get("b"), "hello")
		assert.Equal(t, new_row.Get("c"), 69)
		assert.Equal(t, table.IndexMap("b").Get("hello"), new_row.Get(builder.SYS_PRIMARY_KEY))
	})

	t.Run("duplicate unique field", func(t *testing.T) {
		schema, _ := builder.NewSchemaFromString(`
$TABLE a {
    b String unique(true)
}
        `, nil, false)
		table := schema.Tables.Get("a")
		Create(table, QueryArg{"b": "hello"})
		row, _ := Create(table, QueryArg{"b": "world"})

		assert.Equal(t, row.Get("b"), "world")

		_, err := Update(table, row, QueryArg{"b": "hello"})

		assert.ErrorContains(t, err, "already exists")
		assert.Equal(t, err.(*QueryError).Status(), http.StatusConflict)
	})
}

func TestFindUnique(t *testing.T) {
	t.Run("find unique", func(t *testing.T) {
		schema, _ := builder.NewSchemaFromString(`
$TABLE a {
    b String unique(true)
}
        `, nil, false)
		table := schema.Tables.Get("a")
		row, err := Create(table, QueryArg{"b": "hello"})

		assert.NilError(t, err)
		assert.Equal(t, row.Get("b"), "hello")
		assert.Equal(t, table.IndexMap("b").Get("hello"), builder.GetPrimaryKey(row))

		found, err := FindUnique(table, QueryArg{"b": "hello"})
		assert.NilError(t, err)
		assert.Equal(t, found.Get("b"), "hello")
		assert.Equal(t, builder.GetPrimaryKey(found), builder.GetPrimaryKey(row))
	})

	t.Run("not found", func(t *testing.T) {
		schema, _ := builder.NewSchemaFromString(`
$TABLE a {
    b String unique(true)
}
        `, nil, false)
		table := schema.Tables.Get("a")
		_, err := FindUnique(table, QueryArg{"b": "hello"})

		assert.ErrorContains(t, err, "No row found")
		assert.Equal(t, err.(*QueryError).Status(), http.StatusNotFound)
	})
}

func TestFind(t *testing.T) {
	t.Run("find", func(t *testing.T) {
		schema, _ := builder.NewSchemaFromString(`
$TABLE a {
    b String unique(true)
    c Int optional(true)
}
    `, nil, false)
		table := schema.Tables.Get("a")
		Create(table, QueryArg{"b": "b"})
		Create(table, QueryArg{"b": "b1", "c": 1})
		Create(table, QueryArg{"b": "b2", "c": 2})
		Create(table, QueryArg{"b": "b3", "c": 3})

		found, err := Find(table, QueryArg{"c": map[string]any{"gte": 2}}, false)

		assert.NilError(t, err)
		assert.Equal(t, len(found), 2)
	})

	t.Run("empty where err", func(t *testing.T) {
		schema, _ := builder.NewSchemaFromString(`
$TABLE a {
    b String unique(true)
}
    `, nil, false)
		table := schema.Tables.Get("a")

		_, err := Find(table, QueryArg{}, false)

		assert.Error(t, err, "Where constraints cannot be empty")
	})
}

func TestDelete(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		schema, _ := builder.NewSchemaFromString(`
$TABLE a {
    b String unique(true)
}
    `, nil, false)
		table := schema.Tables.Get("a")
		row, _ := Create(table, QueryArg{"b": "hello"})

		assert.Equal(t, len(table.IndexMap("b").Map), 1)

		Delete(table, row)

		assert.Equal(t, len(table.IndexMap("b").Map), 0)
		assert.Equal(t, table.Rows().Len(), 0)
	})

	t.Run("noop", func(t *testing.T) {
		schema, _ := builder.NewSchemaFromString(`
$TABLE a {
    b String
}
    `, nil, false)
		table := schema.Tables.Get("a")
		Create(table, QueryArg{"b": "hello"})

		Delete(table, builder.TDBTableRow{"b": "world"})

		assert.Equal(t, table.Rows().Len(), 1)
	})
}

func TestConcurrentWrites(t *testing.T) {
	s, err := builder.NewSchemaFromString(`
$TABLE a {
    b String unique(true)
}
        `, nil, false)
	assert.NilError(t, err)

	table := s.Tables.Get("a")

	wg := sync.WaitGroup{}

	wg.Add(2)
	go func() {
		defer wg.Done()
		row, err := Create(table, QueryArg{"b": "hello"})
		if row != nil {
			assert.NilError(t, err)
			assert.Equal(t, row.Get("b"), "hello")
		} else {
			_, err := Create(table, QueryArg{"b": "hello"})
			assert.ErrorContains(t, err, "already exists")
		}
	}()

	go func() {
		defer wg.Done()
		row, err := Create(table, QueryArg{"b": "hello"})
		if row != nil {
			assert.NilError(t, err)
			assert.Equal(t, row.Get("b"), "hello")
		} else {
			_, err := Create(table, QueryArg{"b": "hello"})
			assert.ErrorContains(t, err, "already exists")
		}
	}()

	wg.Wait()
}
