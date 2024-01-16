package conn_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/tobsdb/tobsdb/internal/builder"
	. "github.com/tobsdb/tobsdb/internal/conn"
	"github.com/tobsdb/tobsdb/pkg"
	"gotest.tools/assert"
)

func reqEncode(table string, data map[string]any, where map[string]any) []byte {
	v, _ := json.Marshal(map[string]any{"table": table, "data": data, "where": where})
	return v
}

func newTestSchema() *builder.Schema {
	schema, _ := builder.NewSchemaFromString(`
$TABLE a {
    b Int unique(true)
}`, nil, false)
	return schema
}

func newPopulatedTestSchema(n int) *builder.Schema {
	schema := newTestSchema()
	for i := 1; i <= n; i++ {
		CreateReqHandler(schema, reqEncode("a", map[string]any{"b": i}, nil))
	}
	return schema
}

func TestCreateReqHandler(t *testing.T) {
	t.Run("table not found", func(t *testing.T) {
		schema := newTestSchema()
		raw := reqEncode("b", map[string]any{"a": 1}, nil)
		res := CreateReqHandler(schema, raw)

		assert.Equal(t, res.Status, http.StatusNotFound, res.Message)
		assert.Equal(t, res.Message, "Table not found")
	})

	t.Run("simple create", func(t *testing.T) {
		schema := newTestSchema()
		raw := reqEncode("a", map[string]any{"b": 1}, nil)
		res := CreateReqHandler(schema, raw)

		assert.Equal(t, res.Status, http.StatusCreated, res.Message)
		assert.Equal(t, res.Message, "Created new row in table a")
	})

	t.Run("duplicate error", func(t *testing.T) {
		schema := newTestSchema()
		raw := reqEncode("a", map[string]any{"b": 1}, nil)
		CreateReqHandler(schema, raw)
		res := CreateReqHandler(schema, raw)

		assert.Equal(t, res.Status, http.StatusConflict, res.Message)
		assert.ErrorContains(t, fmt.Errorf(res.Message), "already exists")
	})
}

func TestCreateManyReqHandler(t *testing.T) {}

func TestFindReqHandler(t *testing.T) {
	schema := newPopulatedTestSchema(10)

	t.Run("simple find", func(t *testing.T) {
		res := FindReqHandler(schema, reqEncode("a", nil, map[string]any{"b": 5}))

		assert.Equal(t, res.Status, http.StatusOK, res.Message)
		assert.Equal(t, res.Data.(pkg.Map[string, any])["b"], 5)
	})

	t.Run("not found", func(t *testing.T) {
		res := FindReqHandler(schema, reqEncode("a", nil, map[string]any{"b": 100}))

		assert.Equal(t, res.Status, http.StatusNotFound, res.Message)
		assert.ErrorContains(t, fmt.Errorf(res.Message), "No row found")
	})

	t.Run("type mismatch", func(t *testing.T) {
		res := FindReqHandler(schema, reqEncode("a", nil, map[string]any{"b": "10"}))

		assert.Equal(t, res.Status, http.StatusNotFound, res.Message)
		assert.ErrorContains(t, fmt.Errorf(res.Message), "No row found")
	})

	t.Run("invaild field", func(t *testing.T) {
		res := FindReqHandler(schema, reqEncode("a", nil, map[string]any{"c": 10}))

		assert.Equal(t, res.Status, http.StatusBadRequest, res.Message)
		assert.ErrorContains(t, fmt.Errorf(res.Message), "Unique fields not included")
	})
}

func TestFindManyReqHandler(t *testing.T) {}

func TestUpdateReqHandler(t *testing.T) {
	schema := newPopulatedTestSchema(10)

	t.Run("simple update", func(t *testing.T) {
		res := UpdateReqHandler(schema,
			reqEncode("a", map[string]any{"b": 15}, map[string]any{"b": 5}))

		assert.Equal(t, res.Status, http.StatusOK, res.Message)
		assert.ErrorContains(t, fmt.Errorf(res.Message), "Updated row")
	})

	t.Run("duplicate update", func(t *testing.T) {
		res := UpdateReqHandler(schema,
			reqEncode("a", map[string]any{"b": 7}, map[string]any{"b": 6}))

		assert.Equal(t, res.Status, http.StatusConflict, res.Message)
		assert.ErrorContains(t, fmt.Errorf(res.Message), "already exists")
	})
}

func TestUpdateManyReqHandler(t *testing.T) {}

func TestDeleteReqHandler(t *testing.T) {
	schema := newPopulatedTestSchema(10)

	t.Run("simple delete", func(t *testing.T) {
		res := DeleteReqHandler(schema, reqEncode("a", nil, map[string]any{"b": 5}))

		assert.Equal(t, res.Status, http.StatusOK, res.Message)
		assert.ErrorContains(t, fmt.Errorf(res.Message), "Deleted row")
	})

	t.Run("not found", func(t *testing.T) {
		res := DeleteReqHandler(schema, reqEncode("a", nil, map[string]any{"b": 100}))

		assert.Equal(t, res.Status, http.StatusNotFound, res.Message)
		assert.ErrorContains(t, fmt.Errorf(res.Message), "No row found")
	})
}

func TestDeleteManyReqHandler(t *testing.T) {}
