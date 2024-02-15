package conn

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
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
	RequestActionListDB   RequestAction = "listDatabases"
	RequestActionDBStat   RequestAction = "databaseStats"

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
	return action == RequestActionFind || action == RequestActionFindMany ||
		action == RequestActionDBStat || action == RequestActionListDB || action == RequestActionUseDB
}

func (action RequestAction) IsDBAction() bool {
	return action == RequestActionCreateDB || action == RequestActionUseDB ||
		action == RequestActionDropDB || action == RequestActionListDB ||
		action == RequestActionDBStat || action == RequestActionDropTable || action == RequestActionCreateUser
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

func (tdb *TobsDB) ConnValidate(r ConnRequest) *TdbUser {
	if r.Username == "" {
		return nil
	}
	for _, u := range tdb.Users {
		if u.Name == r.Username && u.ValidateUser(r.Password) {
			return u
		}
	}
	return nil
}

type ConnRequest struct {
	TryConnect bool `json:"tryConnect"`

	Username string `json:"username"`
	Password string `json:"password"`

	DB     string `json:"db"`
	Schema string `json:"schema"`

	CheckOnly string `json:"checkOnly"`
}

type Conn struct{ conn net.Conn }

func (c *Conn) Read() ([]byte, error)               { return pkg.ConnReadBytes(c.conn) }
func (c *Conn) Write(buf []byte) (int, error)       { return pkg.ConnWriteBytes(c.conn, buf) }
func (c *Conn) WriteString(buf string) (int, error) { return pkg.ConnWriteBytes(c.conn, []byte(buf)) }
func (c *Conn) WriteResponse(r Response) (int, error) {
	return pkg.ConnWriteBytes(c.conn, r.Marshal())
}

func (tdb *TobsDB) tryConnect(conn Conn, ctx *ActionCtx, buf []byte) (connected bool, err error) {
	var r ConnRequest
	err = json.Unmarshal(buf, &r)
	if err != nil {
		conn.WriteResponse(NewErrorResponse(http.StatusBadRequest, err.Error()))
		return
	}

	if !r.TryConnect {
		conn.WriteResponse(NewErrorResponse(http.StatusUnauthorized, "Unauthorized"))
		return
	}

	ctx.U = tdb.ConnValidate(r)
	if ctx.U == nil {
		conn.WriteResponse(NewErrorResponse(http.StatusUnauthorized, "Invalid auth"))
		return
	}

	if r.CheckOnly == "true" {
		_, err = builder.NewSchemaFromString(r.Schema, nil, false)
		message := "Schema is valid"
		if err != nil {
			message = err.Error()
		}

		pkg.InfoLog("Schema checks completed:", message)
		conn.WriteString(message)
		return
	}

	if r.DB != "" {
		ctx.S, err = tdb.ResolveSchema(r)
		if err != nil {
			conn.WriteResponse(NewErrorResponse(http.StatusBadRequest, err.Error()))
			return
		}
		pkg.InfoLog("Using database", r.DB)
	}

	connected = true
	conn.WriteString("connected")
	return
}

func (tdb *TobsDB) HandleConnection(conn net.Conn) {
	c := Conn{conn}
	defer conn.Close()
	defer pkg.InfoLog("Connection closed from", conn.RemoteAddr())
	connected := false
	ctx := ActionCtx{nil, nil}
	for {
		buf, err := c.Read()
		if err != nil {
			pkg.ErrorLog("conn read error", err)
			return
		}

		if !connected {
			connected, err = tdb.tryConnect(c, &ctx, buf)
			if err != nil {
				pkg.ErrorLog("conn read error", err)
				return
			}
			continue
		}

		if ctx.S != nil && ctx.S.WriteTicker != nil {
			pkg.LockWrap(ctx.S, func() {
				ctx.S.WriteTicker.Reset(tdb.write_settings.write_interval)
			})
		}

		var req WsRequest
		if err := json.Unmarshal(buf, &req); err != nil {
			pkg.ErrorLog("parsing request", err)
			continue
		}

		res := tdb.ActionHandler(req.Action, &ctx, buf)
		res.ReqId = req.ReqId

		if _, err := c.WriteResponse(res); err != nil {
			pkg.ErrorLog("writing response", err)
			return
		}

		if !req.Action.IsReadOnly() {
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

func (tdb *TobsDB) ActionHandler(action RequestAction, ctx *ActionCtx, raw []byte) Response {
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
		return CreateDBReqHandler(tdb, raw)
	case RequestActionDropDB:
		return DropDBReqHandler(tdb, raw)
	case RequestActionUseDB:
		return UseDBReqHandler(tdb, raw, ctx)
	case RequestActionListDB:
		return ListDBReqHandler(tdb)
	case RequestActionDBStat:
		return DBStatReqHandler(tdb, ctx)
	case RequestActionCreateUser:
		return CreateUserReqHandler(tdb, raw)
	case RequestActionCreate:
		return CreateReqHandler(ctx.S, raw)
	case RequestActionCreateMany:
		return CreateManyReqHandler(ctx.S, raw)
	case RequestActionFind:
		return FindReqHandler(ctx.S, raw)
	case RequestActionFindMany:
		return FindManyReqHandler(ctx.S, raw)
	case RequestActionDelete:
		return DeleteReqHandler(ctx.S, raw)
	case RequestActionDeleteMany:
		return DeleteManyReqHandler(ctx.S, raw)
	case RequestActionUpdate:
		return UpdateReqHandler(ctx.S, raw)
	case RequestActionUpdateMany:
		return UpdateManyReqHandler(ctx.S, raw)
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
