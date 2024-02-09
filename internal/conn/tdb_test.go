package conn_test

import (
	"net/http"
	"testing"

	"github.com/tobsdb/tobsdb/internal/conn"
	"gotest.tools/assert"
)

func TestCreateUser(t *testing.T) {
	tdb := conn.NewTobsDB(conn.AuthSettings{}, conn.NewWriteSettings("", true, 0), conn.LogOptions{})
	res := conn.CreateUserReqHandler(tdb, []byte(`{
        "name": "test",
        "password": "test",
        "role": 0
        }`))
	assert.Equal(t, res.Status, http.StatusCreated)
	assert.Equal(t, tdb.Users.Get(1).Name, "test")
}

func TestCreateDB(t *testing.T) {
	tdb := conn.NewTobsDB(conn.AuthSettings{}, conn.NewWriteSettings("", true, 0), conn.LogOptions{})
	res := conn.CreateDBReqHandler(tdb, []byte(`{
        "name": "test",
        "schema": "$TABLE a {\n b Int\n}"
    }`))
	assert.Equal(t, res.Status, http.StatusCreated, res.Message)
	assert.Equal(t, len(tdb.Data), 1)
	assert.Assert(t, tdb.Data.Has("test"))
	assert.Equal(t, string(tdb.Data.Get("test").Tables.Get("a").Fields.Get("b").BuiltinType), "Int")
}
