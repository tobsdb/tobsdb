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

	t.Run("simple relation", func(t *testing.T) {
		schema, _ := builder.NewSchemaFromString(`
$TABLE a {
    b Int
}
$TABLE b {
    a Int relation(a.b)
}
            `, nil, false)
		Create(schema.Tables.Get("a"), QueryArg{"b": 1})
		row, err := Create(schema.Tables.Get("b"), QueryArg{"a": 1})

		assert.NilError(t, err)
		assert.Equal(t, row.Get("a"), 1)
	})

	t.Run("self relation", func(t *testing.T) {
		schema, _ := builder.NewSchemaFromString(`
$TABLE a {
    a Int
    b Int relation(a.a) optional(true)
}
            `, nil, false)
		table := schema.Tables.Get("a")
		Create(table, QueryArg{"a": 1})
		row, err := Create(table, QueryArg{"a": 2, "b": 1})

		assert.NilError(t, err)
		assert.Equal(t, row.Get("b"), 1)
	})

	t.Run("relation not found", func(t *testing.T) {
		schema, _ := builder.NewSchemaFromString(`
$TABLE a {
    b Int
}
$TABLE b {
    a Int relation(a.b)
}
            `, nil, false)
		_, err := Create(schema.Tables.Get("b"), QueryArg{"a": 1})
		assert.ErrorContains(t, err, "No row found for relation b.a -> a.b")
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

func TestFindWithArgs(t *testing.T) {
	schema, _ := builder.NewSchemaFromString(`
$TABLE a {
    b Int
}
    `, nil, false)
	table := schema.Tables.Get("a")
	for i := 1; i <= 20; i++ {
		Create(table, QueryArg{"b": i})
	}

	t.Run("order by desc", func(t *testing.T) {
		found, err := FindWithArgs(table, FindArgs{
			Where:   QueryArg{"b": map[string]any{"gt": 5, "lte": 10}},
			OrderBy: map[string]OrderBy{"b": OrderByDesc},
		}, false)

		assert.NilError(t, err)
		assert.Equal(t, len(found), 5)
		for i, row := range found {
			v := row.Get("b").(int)
			if i > 0 {
				prev := found[i-1].Get("b").(int)
				assert.Assert(t, prev > v)
			}
			assert.Assert(t, v > 5)
		}
	})

	t.Run("cursor", func(t *testing.T) {
		found, err := FindWithArgs(table, FindArgs{
			Where:  QueryArg{"b": map[string]any{"lt": 15}},
			Cursor: QueryArg{"b": 10},
		}, false)

		assert.NilError(t, err)
		assert.Equal(t, len(found), 5)
		for _, row := range found {
			v := row.Get("b").(int)
			assert.Assert(t, v >= 10 && v < 15)
		}
	})

	t.Run("take", func(t *testing.T) {
		found, err := FindWithArgs(table, FindArgs{
			Take: 5,
		}, true)

		assert.NilError(t, err)
		assert.Equal(t, len(found), 5)
		for _, row := range found {
			v := row.Get("b").(int)
			assert.Assert(t, v <= 5)
		}
	})

	t.Run("order by and cursor and take", func(t *testing.T) {
		found, err := FindWithArgs(table, FindArgs{
			OrderBy: map[string]OrderBy{"b": OrderByDesc},
			Cursor:  QueryArg{"b": 10},
			Take:    5,
		}, true)

		assert.NilError(t, err)
		assert.Equal(t, len(found), 5)
		for _, row := range found {
			v := row.Get("b").(int)
			assert.Assert(t, v <= 10 && v > 5)
		}
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
