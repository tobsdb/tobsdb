package conn_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/tobsdb/tobsdb/internal/auth"
	"github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/internal/conn"
	"gotest.tools/assert"
)

func TestCreateUser(t *testing.T) {
	tdb := builder.NewTobsDB(builder.AuthSettings{}, builder.NewWriteSettings("", true, 0), builder.LogOptions{})

	t.Run("create user", func(t *testing.T) {
		res := conn.CreateUserReqHandler(tdb, []byte(`{
        "name": "test",
        "password": "test",
        "role": 0
        }`))
		assert.Equal(t, res.Status, http.StatusCreated)
		id := strings.TrimPrefix(res.Message, "Created new user ")
		assert.Equal(t, tdb.Users.Get(id).Name, "test")
	})

	t.Run("create duplicate user", func(t *testing.T) {
		res := conn.CreateUserReqHandler(tdb, []byte(`{
        "name": "test",
        "password": "test",
        "role": 0
        }`))
		assert.Equal(t, res.Status, http.StatusConflict)
	})
}

func TestDeleteUser(t *testing.T) {
	tdb := builder.NewTobsDB(builder.AuthSettings{}, builder.NewWriteSettings("", true, 0), builder.LogOptions{})
	conn.CreateUserReqHandler(tdb, []byte(`{
        "name": "test",
        "password": "test",
        "role": 0
        }`))
	res := conn.DeleteUserReqHandler(tdb, []byte(`{"name": "test", "password": "test"}`))
	assert.Equal(t, res.Status, http.StatusOK)
}

func TestUpdateUserRole(t *testing.T) {
	tdb := builder.NewTobsDB(builder.AuthSettings{}, builder.NewWriteSettings("", true, 0), builder.LogOptions{})
	createUser := conn.CreateUserReqHandler(tdb, []byte(`{
        "name": "test",
        "password": "test"
	}`))
	assert.Equal(t, createUser.Status, http.StatusCreated)
	uId := strings.TrimPrefix(createUser.Message, "Created new user ")
	conn.CreateDBReqHandler(tdb, []byte(`{
		"name": "test",
		"schema": "$TABLE a {\n b Int\n}"
	}`))
	assert.Equal(t, tdb.Data.Get("test").CheckUserAccess(tdb.Users.Get(uId)), auth.TdbUserRole(-1))
	res := conn.UpdateUserRoleReqHandler(tdb, []byte(`{
		"name": "test",
		"role": 0,
		"db": "test"
	}`))
	t.Log(res.Message)
	assert.Equal(t, res.Status, http.StatusOK)
	assert.Equal(t, tdb.Data.Get("test").CheckUserAccess(tdb.Users.Get(uId)), auth.TdbUserRoleAdmin)
}

func TestCreateDB(t *testing.T) {
	tdb := builder.NewTobsDB(builder.AuthSettings{}, builder.NewWriteSettings("", true, 0), builder.LogOptions{})
	res := conn.CreateDBReqHandler(tdb, []byte(`{
        "name": "test",
        "schema": "$TABLE a {\n b Int\n}"
    }`))
	assert.Equal(t, res.Status, http.StatusCreated, res.Message)
	assert.Equal(t, len(tdb.Data), 1)
	assert.Assert(t, tdb.Data.Has("test"))
	assert.Equal(t, string(tdb.Data.Get("test").Tables.Get("a").Fields.Get("b").BuiltinType), "Int")
}

func TestDropDB(t *testing.T) {
	tdb := builder.NewTobsDB(builder.AuthSettings{}, builder.NewWriteSettings("", true, 0), builder.LogOptions{})
	conn.CreateDBReqHandler(tdb, []byte(`{
        "name": "test",
        "schema": "$TABLE a {\n b Int\n}"
    }`))
	res := conn.DropDBReqHandler(tdb, []byte(`{"name": "test"}`))
	assert.Equal(t, res.Status, http.StatusOK)
	assert.Equal(t, len(tdb.Data), 0)
}

func TestUseDB(t *testing.T) {
	tdb := builder.NewTobsDB(builder.AuthSettings{}, builder.NewWriteSettings("", true, 0), builder.LogOptions{})
	conn.CreateDBReqHandler(tdb, []byte(`{
        "name": "a",
        "schema": "$TABLE a {\n b Int\n}"
    }`))
	conn.CreateDBReqHandler(tdb, []byte(`{
        "name": "b",
        "schema": "$TABLE b {\n a String\n}"
    }`))
	conn.CreateDBReqHandler(tdb, []byte(`{
        "name": "d",
        "schema": "$TABLE d {\n e Date\n}"
    }`))

	assert.Equal(t, len(tdb.Data), 3)

	ctx := &conn.ConnCtx{User: auth.NewUser("test", "test")}
	t.Run("use a", func(t *testing.T) {
		res := conn.UseDBReqHandler(tdb, []byte(`{"name": "a"}`), ctx)
		assert.Equal(t, res.Status, http.StatusOK)
		assert.Equal(t, string(ctx.Schema.Tables.Get("a").Fields.Get("b").BuiltinType), "Int")
	})

	t.Run("use b", func(t *testing.T) {
		res := conn.UseDBReqHandler(tdb, []byte(`{"name": "b"}`), ctx)
		assert.Equal(t, res.Status, http.StatusOK)
		assert.Equal(t, string(ctx.Schema.Tables.Get("b").Fields.Get("a").BuiltinType), "String")
	})

	t.Run("use unknown", func(t *testing.T) {
		res := conn.UseDBReqHandler(tdb, []byte(`{"name": "c"}`), nil)
		assert.Equal(t, res.Status, http.StatusNotFound)
		// failed change should not change connected db
		assert.Equal(t, string(ctx.Schema.Tables.Get("b").Fields.Get("a").BuiltinType), "String")
	})

	t.Run("use d(action handler)", func(t *testing.T) {
		res := conn.ActionHandler(tdb, conn.RequestActionUseDB, ctx, []byte(`{"name": "d"}`))
		assert.Equal(t, res.Status, http.StatusOK)
		assert.Equal(t, string(ctx.Schema.Tables.Get("d").Fields.Get("e").BuiltinType), "Date")
	})
}
