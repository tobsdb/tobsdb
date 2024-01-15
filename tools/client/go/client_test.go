package main_test

import (
	"strings"
	"testing"

	"gotest.tools/assert"

	client "github.com/tobsdb/tobsdb/tools/client/go"
)

func TestNewTdbClient(t *testing.T) {
	tdb, err := client.NewTdbClient("ws://localhost:7085",
		"go_client_test",
		client.TdbClientOptions{
			Username: "user",
			Password: "pass", SchemaPath: "../js/schema.tdb",
		})
	assert.NilError(t, err)
	assert.Assert(t, strings.HasPrefix(tdb.Url.String(), "ws://localhost:7085"))
	assert.Assert(t, strings.Contains(tdb.Url.String(), "db=go_client_test"))
}

func TestConnect(t *testing.T) {
	tdb, err := client.NewTdbClient("ws://localhost:7085",
		"go_client_test",
		client.TdbClientOptions{
			Username: "user",
			Password: "pass", SchemaPath: "../js/schema.tdb",
		})
	assert.NilError(t, err)
	assert.NilError(t, tdb.Connect())
	assert.NilError(t, tdb.Disconnect())
}

func TestCreate(t *testing.T) {
	tdb, err := client.NewTdbClient("ws://localhost:7085",
		"go_client_test",
		client.TdbClientOptions{
			Username: "user",
			Password: "pass", SchemaPath: "../js/schema.tdb",
		})
	assert.NilError(t, err)
	assert.NilError(t, tdb.Connect())
	res, err := tdb.Create("example", map[string]any{"vector": []int{1, 2, 3}})
	t.Log(res)
	assert.NilError(t, err)
	assert.Equal(t, res.Data.(map[string]any)["name"], "Hello world")
	assert.DeepEqual(t, res.Data.(map[string]any)["vector"].([]any), []any{1., 2., 3.})
	assert.NilError(t, tdb.Disconnect())
}
