package conn

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/pkg"
)

type RequestAction string

const (
	RequestActionCreate     RequestAction = "create"
	RequestActionCreateMany RequestAction = "createMany"
	RequestActionFind       RequestAction = "findUnique"
	RequestActionFindMany   RequestAction = "findMany"
	RequestActionDelete     RequestAction = "deleteUnique"
	RequestActionDeleteMany RequestAction = "deleteMany"
	RequestActionUpdate     RequestAction = "updateUnique"
	RequestActionUpdateMany RequestAction = "updateMany"
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

func (db *TobsDB) HandleConnection(w http.ResponseWriter, r *http.Request) {
	url_query := r.URL.Query()
	db_name := url_query.Get("db")
	check_schema_only, check_schema_only_err := strconv.ParseBool(r.URL.Query().Get("check_schema"))
	is_migration, is_migration_err := strconv.ParseBool(r.URL.Query().Get("migration"))

	env_auth := fmt.Sprintf("%s:%s", os.Getenv("TDB_USER"), os.Getenv("TDB_PASS"))
	var conn_auth string
	if url_query.Has("auth") {
		conn_auth = url_query.Get("auth")
	} else if url_query.Has("username") || url_query.Has("password") {
		conn_auth = url_query.Get("username") + ":" + url_query.Get("password")
	} else {
		conn_auth = r.Header.Get("Authorization")
	}
	if conn_auth != env_auth {
		ConnError(w, r, "connection unauthorized")
		return
	}

	if len(r.URL.Query().Get("check_schema")) == 0 {
		check_schema_only = false
	} else if check_schema_only_err != nil {
		ConnError(w, r, "Invalid check_schema value")
		return
	}

	if len(r.URL.Query().Get("migration")) == 0 {
		is_migration = false
	} else if is_migration_err != nil {
		ConnError(w, r, "Invalid migration value")
		return
	}

	if check_schema_only {
		_, err := builder.NewSchemaFromURL(r.URL, nil, true)
		conn, upgrade_err := Upgrader.Upgrade(w, r, nil)
		if upgrade_err != nil {
			pkg.ErrorLog(err)
			return
		}

		var message string
		if err != nil {
			message = err.Error()
		} else {
			message = "Schema is valid"
		}

		pkg.InfoLog("Schema checks completed:", message)
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, message))
		conn.Close()
		return
	}

	if len(db_name) == 0 {
		ConnError(w, r, "Missing db name")
		return
	}

	schema, err := db.ResolveSchema(db_name, r.URL, is_migration)
	if err != nil {
		ConnError(w, r, err.Error())
		return
	}

	send_schema, _ := json.Marshal(schema.Tables)
	w.Header().Set("Schema", string(send_schema))
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
		if db.write_settings.write_ticker != nil {
			db.Locker.Lock()
			db.write_settings.write_ticker.Reset(db.write_settings.write_interval)
			db.Locker.Unlock()
		}

		var req WsRequest
		json.NewDecoder(bytes.NewReader(message)).Decode(&req)

		res := db.ActionHandler(req.Action, schema, message)
		res.ReqId = req.ReqId

		if err := conn.WriteJSON(res); err != nil {
			pkg.ErrorLog("writing response", err)
			return
		}

		if req.Action != RequestActionFind && req.Action != RequestActionFindMany {
			db.Locker.Lock()
			db.last_change = time.Now()
			db.Locker.Unlock()
		}
	}
}

func (db *TobsDB) ActionHandler(action RequestAction, schema *builder.Schema, message []byte) Response {
	if action == RequestActionFind || action == RequestActionFindMany {
		db.Locker.RLock()
		defer db.Locker.RUnlock()
	} else {
		db.Locker.Lock()
		defer db.Locker.Unlock()
	}

	switch action {
	case RequestActionCreate:
		return CreateReqHandler(schema, message)
	case RequestActionCreateMany:
		return CreateManyReqHandler(schema, message)
	case RequestActionFind:
		return FindReqHandler(schema, message)
	case RequestActionFindMany:
		return FindManyReqHandler(schema, message)
	case RequestActionDelete:
		return DeleteReqHandler(schema, message)
	case RequestActionDeleteMany:
		return DeleteManyReqHandler(schema, message)
	case RequestActionUpdate:
		return UpdateReqHandler(schema, message)
	case RequestActionUpdateMany:
		return UpdateManyReqHandler(schema, message)
	default:
		return Response{
			Status:  http.StatusBadRequest,
			Message: fmt.Sprintf("unknown action: %s", action),
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
