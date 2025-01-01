package conn

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/tobsdb/tobsdb/internal/auth"
	"github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/internal/query"
	"github.com/tobsdb/tobsdb/internal/transaction"
)

type Response struct {
	Data    any    `json:"data"`
	Message string `json:"message"`
	Status  int    `json:"status"`
	// don't manually set this. it comes from the client
	ReqId int `json:"__tdb_client_req_id__"`
}

func (r Response) Marshal() []byte {
	res, _ := json.Marshal(r)
	return res
}

func NewErrorResponse(status int, err string) Response {
	return Response{Message: err, Status: status}
}

func NewResponse(status int, message string, data any) Response {
	return Response{Data: data, Message: message, Status: status}
}

type CreateRequest struct {
	Data  query.QueryArg `json:"data"`
	Table string         `json:"table"`
}

func CreateReqHandler(schema *builder.Schema, raw []byte) Response {
	var req CreateRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if !schema.Tables.Has(req.Table) {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	}

	// TODO(Tobshub): make snapshot of schema
	table := schema.Tables.Get(req.Table)
	res, err := query.Create(table, req.Data)
	if err != nil {
		if query_error, ok := err.(*query.QueryError); ok {
			return NewErrorResponse(query_error.Status(), query_error.Error())
		}
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	// TODO(Tobshub): apply snapshot to schema

	schema.UpdateLastChange()
	return NewResponse(
		http.StatusCreated,
		fmt.Sprintf("Created new row in table %s",
			table.Name),
		res,
	)
}

type CreateManyRequest struct {
	Table string           `json:"table"`
	Data  []query.QueryArg `json:"data"`
}

func CreateManyReqHandler(schema *builder.Schema, raw []byte) Response {
	var req CreateManyRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if !schema.Tables.Has(req.Table) {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	}

	table := schema.Tables.Get(req.Table)
	created_rows := []map[string]any{}
	for _, row := range req.Data {
		res, err := query.Create(table, row)
		if err != nil {
			if query_error, ok := err.(*query.QueryError); ok {
				return NewErrorResponse(query_error.Status(), query_error.Error())
			}
			return NewErrorResponse(http.StatusBadRequest, err.Error())
		}
		created_rows = append(created_rows, res)
	}

	schema.UpdateLastChange()
	return NewResponse(
		http.StatusCreated,
		fmt.Sprintf("Created %d new rows in table %s", len(created_rows), table.Name),
		created_rows,
	)
}

type FindRequest struct {
	Table string         `json:"table"`
	Where query.QueryArg `json:"where"`
}

func FindReqHandler(schema *builder.Schema, raw []byte) Response {
	var req FindRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if !schema.Tables.Has(req.Table) {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	}

	table := schema.Tables.Get(req.Table)
	res, err := query.FindUnique(table, req.Where)
	if err != nil {
		if query_error, ok := err.(*query.QueryError); ok {
			return NewErrorResponse(query_error.Status(), query_error.Error())
		}
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	return NewResponse(
		http.StatusOK,
		fmt.Sprintf("Found row with in table %s", table.Name),
		res,
	)
}

type FindManyRequest struct {
	Table   string                   `json:"table"`
	Where   query.QueryArg           `json:"where"`
	OrderBy map[string]query.OrderBy `json:"orderBy"`
	Take    int                      `json:"take"`
	Skip    int                      `json:"skip"`
	Cursor  map[string]any           `json:"cursor"`
}

func FindManyReqHandler(schema *builder.Schema, raw []byte) Response {
	var req FindManyRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if !schema.Tables.Has(req.Table) {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	}

	table := schema.Tables.Get(req.Table)
	res, err := query.FindWithArgs(table, query.FindArgs{
		Where:   req.Where,
		Take:    req.Take,
		OrderBy: req.OrderBy,
		Cursor:  req.Cursor,
		Skip:    req.Skip,
	}, true)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	return NewResponse(
		http.StatusOK,
		fmt.Sprintf("Found %d rows in table %s", len(res), table.Name),
		res,
	)
}

type DeleteRequest struct {
	Table string         `json:"table"`
	Where query.QueryArg `json:"where"`
}

func DeleteReqHandler(schema *builder.Schema, raw []byte) Response {
	var req DeleteRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if !schema.Tables.Has(req.Table) {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	}

	table := schema.Tables.Get(req.Table)
	row, err := query.FindUnique(table, req.Where)
	if err != nil {
		if query_error, ok := err.(*query.QueryError); ok {
			return NewErrorResponse(query_error.Status(), query_error.Error())
		}
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	query.Delete(table, row)
	schema.UpdateLastChange()
	return NewResponse(
		http.StatusOK,
		fmt.Sprintf("Deleted row in table %s", table.Name),
		row,
	)
}

func DeleteManyReqHandler(schema *builder.Schema, raw []byte) Response {
	var req DeleteRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if !schema.Tables.Has(req.Table) {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	}

	table := schema.Tables.Get(req.Table)
	rows, err := query.Find(table, req.Where, false)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	for _, row := range rows {
		query.Delete(table, row)
	}

	schema.UpdateLastChange()
	return NewResponse(
		http.StatusOK,
		fmt.Sprintf("Deleted %d rows in table %s", len(rows), table.Name),
		rows,
	)
}

type UpdateRequest struct {
	Table string         `json:"table"`
	Where query.QueryArg `json:"where"`
	Data  query.QueryArg `json:"data"`
}

func UpdateReqHandler(schema *builder.Schema, raw []byte) Response {
	var req UpdateRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if !schema.Tables.Has(req.Table) {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	}

	table := schema.Tables.Get(req.Table)
	row, err := query.FindUnique(table, req.Where)
	if err != nil {
		if query_error, ok := err.(*query.QueryError); ok {
			return NewErrorResponse(query_error.Status(), query_error.Error())
		}
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	res, err := query.Update(table, row, req.Data)
	if err != nil {
		if query_error, ok := err.(*query.QueryError); ok {
			return NewErrorResponse(query_error.Status(), query_error.Error())
		}
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	schema.UpdateLastChange()
	return NewResponse(
		http.StatusOK,
		fmt.Sprintf("Updated row in table %s", table.Name),
		res,
	)
}

func UpdateManyReqHandler(schema *builder.Schema, raw []byte) Response {
	var req UpdateRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if !schema.Tables.Has(req.Table) {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	}
	table := schema.Tables.Get(req.Table)
	rows, err := query.Find(table, query.QueryArg(req.Where), false)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	for i := 0; i < len(rows); i++ {
		row := rows[i]
		res, err := query.Update(table, row, query.QueryArg(req.Data))
		if err != nil {
			if query_error, ok := err.(*query.QueryError); ok {
				return NewErrorResponse(query_error.Status(), query_error.Error())
			}
			return NewErrorResponse(http.StatusBadRequest, err.Error())
		}
		rows[i] = res
	}

	schema.UpdateLastChange()
	return NewResponse(
		http.StatusOK,
		fmt.Sprintf("Updated %d rows in table %s", len(rows), table.Name),
		rows,
	)
}

type CreateUserRequest struct {
	Name     string `json:"name"`
	Password string `json:"password"`
}

func CreateUserReqHandler(tdb *builder.TobsDB, raw []byte) Response {
	var req CreateUserRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	for _, u := range tdb.Users {
		if req.Name == u.Name {
			return NewErrorResponse(http.StatusConflict, "User exists with that name")
		}
	}
	user := auth.NewUser(req.Name, req.Password)
	tdb.Users.Set(user.Id, user)
	tdb.WriteToFile()
	return NewResponse(http.StatusCreated, fmt.Sprintf("Created new user %s", user.Id), nil)
}

type DeleteUserRequest struct {
	Name     string `json:"name"`
	Password string `json:"password"`
}

func DeleteUserReqHandler(tdb *builder.TobsDB, raw []byte) Response {
	var req DeleteUserRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	var found_user *auth.TdbUser
	for _, u := range tdb.Users {
		if req.Name == u.Name {
			found_user = u
		}
	}

	if found_user == nil {
		return NewErrorResponse(http.StatusNotFound, "User not found")
	}

	tdb.Users.Delete(found_user.Id)
	tdb.WriteToFile()
	return NewResponse(http.StatusOK, fmt.Sprintf("Deleted user %s", found_user.Id), nil)
}

type UpdateUserRoleRequest struct {
	Name string `json:"name"`
	Role int    `json:"role"`
	DB   string `json:"db"`
}

func UpdateUserRoleReqHandler(tdb *builder.TobsDB, raw []byte) Response {
	var req UpdateUserRoleRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	var u *auth.TdbUser
	for _, u = range tdb.Users {
		if req.Name == u.Name {
			break
		}
		u = nil
	}
	if u == nil {
		return NewErrorResponse(http.StatusNotFound, "User not found")
	}

	s := tdb.Data.Get(req.DB)
	if s == nil {
		return NewErrorResponse(http.StatusNotFound, "Database not found")
	}

	if s.CheckUserAccess(u) != -1 {
		s.RemoveUser(u)
	}
	s.AddUser(u, auth.TdbUserRole(req.Role))
	tdb.Data.Set(s.Name, s)
	tdb.WriteToFile()
	return NewResponse(http.StatusOK,
		fmt.Sprintf("Updated user %s role on %s to %d", req.Name, req.DB, req.Role), nil)
}

type CreateDBRequest struct {
	Name   string `json:"name"`
	Schema string `json:"schema"`
}

func CreateDBReqHandler(tdb *builder.TobsDB, raw []byte) Response {
	var req CreateDBRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if tdb.Data.Has(req.Name) {
		return NewErrorResponse(http.StatusConflict,
			fmt.Sprintf("Database already exists with name %s", req.Name))
	}

	schema, err := builder.NewSchemaFromString(req.Schema, nil, false)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	schema.Name = req.Name
	tdb.Data.Set(req.Name, schema)
	return NewResponse(http.StatusCreated, fmt.Sprintf("Created new database %s", req.Name), nil)
}

type DropDBRequest struct {
	Name string `json:"name"`
}

func DropDBReqHandler(tdb *builder.TobsDB, raw []byte) Response {
	var req DropDBRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	tdb.Data.Delete(req.Name)
	return NewResponse(http.StatusOK, fmt.Sprintf("Dropped database %s", req.Name), nil)
}

type UseDBRequest struct {
	Name string `json:"name"`
}

func UseDBReqHandler(tdb *builder.TobsDB, raw []byte, ctx *ConnCtx) Response {
	var req UseDBRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if !tdb.Data.Has(req.Name) {
		return NewErrorResponse(http.StatusNotFound,
			fmt.Sprintf("Database not found with name %s", req.Name))
	}

	ctx.Schema = tdb.Data.Get(req.Name)
	if !ctx.Schema.UserHasClearance(ctx.User, auth.TdbUserRoleReadOnly) {
		return NewErrorResponse(http.StatusForbidden,
			fmt.Sprintf("Insufficient permissions on database %s", req.Name))
	}

	return NewResponse(http.StatusOK, fmt.Sprintf("Connected to database %s", req.Name), nil)
}

func ListDBReqHandler(tdb *builder.TobsDB) Response {
	return NewResponse(http.StatusOK, "List of databases", tdb.Data.Keys())
}

func DBStatReqHandler(tdb *builder.TobsDB, ctx *ConnCtx) Response {
	return NewResponse(http.StatusOK, "Database stats", ctx.Schema)
}

func StartTransactionReqHandler(ctx *ConnCtx) Response {
	if ctx.TxCtx != nil && !ctx.TxCtx.Persisted {
		return NewErrorResponse(http.StatusBadRequest, "Transaction already started")
	}

    if ctx.TxCtx == nil {
        ctx.TxCtx = transaction.NewTransactionCtx(ctx.Schema)
    }
    ctx.TxCtx.Persisted = true;
	return NewResponse(http.StatusOK, "Started transaction", nil)
}

func CommitTransactionReqHandler(ctx *ConnCtx) Response {
	ctx.TxCtx.Commit(ctx.Schema)
	ctx.TxCtx = nil
	return NewResponse(http.StatusOK, "Committed transaction", nil)
}

func RollbackTransactionReqHandler(ctx *ConnCtx) Response {
	ctx.TxCtx.Rollback()
	ctx.TxCtx = nil
	return NewResponse(http.StatusOK, "Rolled back transaction", nil)
}
