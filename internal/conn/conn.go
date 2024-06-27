package conn

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tobsdb/tobsdb/internal/auth"
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
		action == RequestActionDBStat || action == RequestActionDropTable ||
		action == RequestActionCreateUser || action == RequestActionDeleteUser
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

func (tdb *TobsDB) ConnValidate(r ConnRequest) *auth.TdbUser {
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
	Username string `json:"username"`
	Password string `json:"password"`

	DB     string `json:"db"`
	Schema string `json:"schema"`

	CheckOnly bool `json:"checkOnly"`
}

type ConnCtx struct {
	conn        net.Conn
	attempts    int
	isAuthed    bool
	shouldClose bool

	User   *auth.TdbUser
	Schema *builder.Schema
}

// New connections have a 30 second deadline.
// If the deadline is reached, and the connection is not authenticated, the connection is closed.
func NewConnCtx(c net.Conn) *ConnCtx {
	c.SetDeadline(time.Now().Add(30 * time.Second))
	return &ConnCtx{c, 0, false, false, nil, nil}
}

// SetAuthed marks the connection as authenticated and removes the deadline.
func (ctx *ConnCtx) SetAuthed() {
	ctx.isAuthed = true
	ctx.conn.SetDeadline(time.Time{})
}

const (
	maxConnAttempts  = 3
	shouldCloseError = "connection no no wanna"
)

func (c *ConnCtx) Read() ([]byte, error) {
	if c.shouldClose {
		return nil, errors.New(shouldCloseError)
	}
	return pkg.ConnReadBytes(c.conn)
}

func (ctx *ConnCtx) Write(buf []byte) (int, error) {
	if ctx.shouldClose {
		return 0, errors.New(shouldCloseError)
	}
	return pkg.ConnWriteBytes(ctx.conn, buf)
}
func (ctx *ConnCtx) WriteString(buf string) (int, error)   { return ctx.Write([]byte(buf)) }
func (ctx *ConnCtx) WriteResponse(r Response) (int, error) { return ctx.Write(r.Marshal()) }

func (tdb *TobsDB) tryConnect(ctx *ConnCtx, buf []byte) error {
	var r ConnRequest
	if err := json.Unmarshal(buf, &r); err != nil {
		ctx.WriteResponse(NewErrorResponse(http.StatusBadRequest, err.Error()))
		return err
	}

	ctx.User = tdb.ConnValidate(r)
	if ctx.User == nil {
		ctx.WriteResponse(NewErrorResponse(http.StatusUnauthorized, "Invalid auth"))
		return nil
	}

	if r.CheckOnly {
		_, err := builder.NewSchemaFromString(r.Schema, nil, false)
		message := "Schema is valid"
		if err != nil {
			message = err.Error()
		}

		pkg.InfoLog("Schema checks completed:", message)
		ctx.WriteString(message)
		ctx.shouldClose = true
		return nil
	}

	if r.DB != "" {
		s, err := tdb.ResolveSchema(r)
		if err != nil {
			ctx.WriteResponse(NewErrorResponse(http.StatusBadRequest, err.Error()))
			return err
		}
		ctx.Schema = s
		pkg.InfoLog("Using database", r.DB)
	}

	ctx.SetAuthed()
	ctx.WriteString("connected")
	return nil
}

func (tdb *TobsDB) HandleConnection(conn net.Conn) {
	ctx := NewConnCtx(conn)
	defer conn.Close()
	defer pkg.InfoLog("Connection closed from", conn.RemoteAddr())
	for {
		buf, err := ctx.Read()
		if err != nil {
			pkg.ErrorLog("conn read error", err)
			return
		}

		if !ctx.isAuthed {
			if ctx.attempts == maxConnAttempts {
				pkg.ErrorLog("max connection attempts reached")
				return
			}

			err = tdb.tryConnect(ctx, buf)
			ctx.attempts += 1
			if err != nil {
				pkg.ErrorLog("conn attempt error", err)
				return
			}
			continue
		}

		if ctx.Schema != nil && ctx.Schema.WriteTicker != nil {
			pkg.LockWrap(ctx.Schema, func() {
				ctx.Schema.WriteTicker.Reset(tdb.write_settings.write_interval)
			})
		}

		var req WsRequest
		if err := json.Unmarshal(buf, &req); err != nil {
			pkg.ErrorLog("parsing request", err)
			continue
		}

		res := tdb.ActionHandler(req.Action, ctx, buf)
		res.ReqId = req.ReqId

		if _, err := ctx.WriteResponse(res); err != nil {
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

func (tdb *TobsDB) ActionHandler(action RequestAction, ctx *ConnCtx, raw []byte) Response {
	if action.IsReadOnly() {
		if !ctx.User.HasClearance(auth.TdbUserRoleReadOnly) {
			return NewErrorResponse(http.StatusForbidden, "Insufficient role permissions")
		}
		if ctx.Schema != nil {
			ctx.Schema.GetLocker().RLock()
			defer ctx.Schema.GetLocker().RUnlock()
		}
	} else {
		if !ctx.User.HasClearance(auth.TdbUserRoleReadWrite) {
			return NewErrorResponse(http.StatusForbidden, "Insufficient role permissions")
		}
		if ctx.Schema != nil {
			ctx.Schema.GetLocker().Lock()
			defer ctx.Schema.GetLocker().Unlock()
		}
	}

	if action.IsDBAction() {
		if !ctx.User.HasClearance(auth.TdbUserRoleAdmin) {
			return NewErrorResponse(http.StatusForbidden, "Insufficient role permissions")
		}
		tdb.Locker.Lock()
		defer tdb.Locker.Unlock()
	} else if ctx.Schema == nil {
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
	case RequestActionDeleteUser:
		return DeleteUserReqHandler(tdb, raw)
	case RequestActionCreate:
		return CreateReqHandler(ctx.Schema, raw)
	case RequestActionCreateMany:
		return CreateManyReqHandler(ctx.Schema, raw)
	case RequestActionFind:
		return FindReqHandler(ctx.Schema, raw)
	case RequestActionFindMany:
		return FindManyReqHandler(ctx.Schema, raw)
	case RequestActionDelete:
		return DeleteReqHandler(ctx.Schema, raw)
	case RequestActionDeleteMany:
		return DeleteManyReqHandler(ctx.Schema, raw)
	case RequestActionUpdate:
		return UpdateReqHandler(ctx.Schema, raw)
	case RequestActionUpdateMany:
		return UpdateManyReqHandler(ctx.Schema, raw)
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
