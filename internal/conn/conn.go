package conn

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/pkg"
)

type RequestAction string

const (
	// rows actions
	RequestActionCreate     RequestAction = "create"
	RequestActionCreateMany RequestAction = "createMany"
	RequestActionFind       RequestAction = "findUnique"
	RequestActionFindMany   RequestAction = "findMany"
	RequestActionDelete     RequestAction = "deleteUnique"
	RequestActionDeleteMany RequestAction = "deleteMany"
	RequestActionUpdate     RequestAction = "updateUnique"
	RequestActionUpdateMany RequestAction = "updateMany"

	// database actions
	RequestActionCreateDB RequestAction = "createDatabase"
	RequestActionUseDB    RequestAction = "useDatabase"
	RequestActionDropDB   RequestAction = "dropDatabase"

	// table actions
	RequestActionDropTable RequestAction = "dropTable"
	ReuqestActionMigration RequestAction = "migration"

	// user actions
	RequestActionCreateUser RequestAction = "createUser"
	RequestActionDeleteUser RequestAction = "deleteUser"

	// TODO: transaction actions
	ReuqestActionTransaction RequestAction = "transaction"
	ReuqestActionCommit      RequestAction = "commit"
	ReuqestActionRollback    RequestAction = "rollback"
)

func (action RequestAction) IsReadOnly() bool {
	return action == RequestActionFind || action == RequestActionFindMany
}

func (action RequestAction) IsDBAction() bool {
	return action == RequestActionCreateDB || action == RequestActionUseDB ||
		action == RequestActionDropDB || action == RequestActionDropTable ||
		action == RequestActionCreateUser
}

type WsRequest struct {
	Action RequestAction `json:"action"`
	ReqId  int           `json:"__tdb_client_req_id__"` // used in tdb clients
}

var Upgrader = websocket.Upgrader{
	WriteBufferSize: 1024 * 10,
	ReadBufferSize:  1024 * 10,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func (tdb *TobsDB) ConnValidate(q url.Values) *TdbUser {
	username := q.Get("username")
	password := q.Get("password")
	if username == "" {
		return nil
	}
	for _, u := range tdb.Users {
		if u.Name == username && u.ValidateUser(password) {
			return u
		}
	}
	return nil
}

func (tdb *TobsDB) HandleConnection(w http.ResponseWriter, r *http.Request) {
	url_query := r.URL.Query()
	db_name := url_query.Get("db")
	check_schema_only, check_schema_only_err := strconv.ParseBool(r.URL.Query().Get("check_schema"))

	user := tdb.ConnValidate(url_query)
	if user == nil {
		ConnError(w, r, "Invalid auth")
		return
	}

	if r.URL.Query().Get("check_schema") == "" {
		check_schema_only = false
	} else if check_schema_only_err != nil {
		ConnError(w, r, "Invalid check_schema value")
		return
	}

	if check_schema_only {
		_, err := builder.NewSchemaFromURL(r.URL, nil, true)
		conn, upgrade_err := Upgrader.Upgrade(w, r, nil)
		if upgrade_err != nil {
			pkg.ErrorLog(err)
			return
		}

		message := "Schema is valid"
		if err != nil {
			message = err.Error()
		}

		pkg.InfoLog("Schema checks completed:", message)
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, message))
		conn.Close()
		return
	}

	ctx := ActionCtx{user, nil}
	if db_name != "" {
		s, err := tdb.ResolveSchema(db_name, r.URL)
		if err != nil {
			ConnError(w, r, err.Error())
			return
		}
		ctx.S = s
		pkg.InfoLog("Using database", db_name)
		send_schema, _ := json.Marshal(s.Tables)
		w.Header().Set("Schema", string(send_schema))
	}

	conn, err := Upgrader.Upgrade(w, r, nil)
	if err != nil {
		pkg.ErrorLog(err)
		return
	}
	pkg.InfoLog("New connection established")
	defer conn.Close()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				pkg.ErrorLog("unexpected close", err)
			} else {
				pkg.DebugLog("connection closed", err)
			}
			return
		}

		// reset write timer when a reqeuest is received
		if ctx.S != nil && ctx.S.WriteTicker != nil {
			pkg.LockWrap(ctx.S, func() {
				ctx.S.WriteTicker.Reset(tdb.write_settings.write_interval)
			})
		}

		var req WsRequest
		json.NewDecoder(bytes.NewReader(message)).Decode(&req)

		res := tdb.ActionHandler(req.Action, &ctx, message)
		res.ReqId = req.ReqId

		if err := conn.WriteJSON(res); err != nil {
			pkg.ErrorLog("writing response", err)
			return
		}

		if req.Action != RequestActionFind && req.Action != RequestActionFindMany {
			pkg.LockWrap(tdb, func() {
				tdb.last_change = time.Now()
			})
		}
	}
}

type ActionCtx struct {
	U *TdbUser
	S *builder.Schema
}

func (tdb *TobsDB) ActionHandler(action RequestAction, ctx *ActionCtx, message []byte) Response {
	if action.IsReadOnly() {
		if !ctx.U.HasClearance(TdbUserRoleReadOnly) {
			return NewErrorResponse(http.StatusForbidden, "Insufficient role permissions")
		}
		if ctx.S != nil {
			ctx.S.GetLocker().RLock()
			defer ctx.S.GetLocker().RUnlock()
		}
	} else {
		if !ctx.U.HasClearance(TdbUserRoleReadWrite) {
			return NewErrorResponse(http.StatusForbidden, "Insufficient role permissions")
		}
		if ctx.S != nil {
			ctx.S.GetLocker().Lock()
			defer ctx.S.GetLocker().Unlock()
		}
	}

	if action.IsDBAction() {
		if !ctx.U.HasClearance(TdbUserRoleAdmin) {
			return NewErrorResponse(http.StatusForbidden, "Insufficient role permissions")
		}
		tdb.Locker.Lock()
		defer tdb.Locker.Unlock()
	} else if ctx.S == nil {
		return NewErrorResponse(http.StatusBadRequest, "no database selected")
	}

	switch action {
	case RequestActionCreateDB:
		return CreateDBReqHandler(tdb, message)
	case RequestActionDropDB:
		return DropDBReqHandler(tdb, message)
	case RequestActionUseDB:
		return UseDBReqHandler(tdb, message, ctx)
	case RequestActionCreateUser:
		return CreateUserReqHandler(tdb, message)
	case RequestActionCreate:
		return CreateReqHandler(ctx.S, message)
	case RequestActionCreateMany:
		return CreateManyReqHandler(ctx.S, message)
	case RequestActionFind:
		return FindReqHandler(ctx.S, message)
	case RequestActionFindMany:
		return FindManyReqHandler(ctx.S, message)
	case RequestActionDelete:
		return DeleteReqHandler(ctx.S, message)
	case RequestActionDeleteMany:
		return DeleteManyReqHandler(ctx.S, message)
	case RequestActionUpdate:
		return UpdateReqHandler(ctx.S, message)
	case RequestActionUpdateMany:
		return UpdateManyReqHandler(ctx.S, message)
	default:
		return NewErrorResponse(http.StatusBadRequest, fmt.Sprintf("unknown action: %s", action))
	}
}

func ConnError(w http.ResponseWriter, r *http.Request, conn_error string) {
	pkg.InfoLog("connection error:", conn_error)
	headers := http.Header{}
	headers.Set("tdb-error", conn_error)
	conn, err := Upgrader.Upgrade(w, r, headers)
	if err != nil {
		pkg.ErrorLog(err)
		return
	}

	conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseUnsupportedData, conn_error))
	conn.Close()
}
