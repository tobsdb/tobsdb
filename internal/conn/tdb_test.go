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
