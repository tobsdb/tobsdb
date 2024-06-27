package conn

import (
	"encoding/json"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tobsdb/tobsdb/internal/auth"
	"github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/pkg"
)

type WsRequest struct {
	Action RequestAction `json:"action"`
	ReqId  int           `json:"__tdb_client_req_id__"` // used in tdb clients
}

var Upgrader = websocket.Upgrader{
	WriteBufferSize: 1024 * 10,
	ReadBufferSize:  1024 * 10,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func ConnValidate(tdb *builder.TobsDB, r ConnRequest) *auth.TdbUser {
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

func tryConnect(tdb *builder.TobsDB, ctx *ConnCtx, buf []byte) error {
	var r ConnRequest
	if err := json.Unmarshal(buf, &r); err != nil {
		ctx.WriteResponse(NewErrorResponse(http.StatusBadRequest, err.Error()))
		return err
	}

	ctx.User = ConnValidate(tdb, r)
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
		s, err := ResolveSchema(tdb, r)
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

func HandleConnection(tdb *builder.TobsDB, conn net.Conn) {
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

			err = tryConnect(tdb, ctx, buf)
			ctx.attempts += 1
			if err != nil {
				pkg.ErrorLog("conn attempt error", err)
				return
			}
			continue
		}

		if ctx.Schema != nil && ctx.Schema.WriteTicker != nil {
			pkg.LockWrap(ctx.Schema, func() {
				ctx.Schema.WriteTicker.Reset(tdb.WriteSettings.WriteInterval)
			})
		}

		var req WsRequest
		if err := json.Unmarshal(buf, &req); err != nil {
			pkg.ErrorLog("parsing request", err)
			continue
		}

		res := ActionHandler(tdb, req.Action, ctx, buf)
		res.ReqId = req.ReqId

		if _, err := ctx.WriteResponse(res); err != nil {
			pkg.ErrorLog("writing response", err)
			return
		}

		if !req.Action.IsReadOnly() {
			pkg.LockWrap(tdb, func() {
				tdb.LastChange = time.Now()
			})
		}
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
