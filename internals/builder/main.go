package builder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/gorilla/websocket"

	TDBParser "github.com/tobshub/tobsdb/internals/parser"
	TDBPkg "github.com/tobshub/tobsdb/pkg"
)

type (
	// Maps row field name to its saved data
	tdbDataRow = map[string]any
	// Maps row id to its saved data
	tdbDataTable = map[int](tdbDataRow)
	// Maps table name to its saved data
	TDBData = map[string]tdbDataTable
)

type Schema struct {
	Tables map[string]TDBParser.Table
	Data   TDBData
}

type TobsDB struct {
	// db_name -> table_name -> row_id -> field_name
	data       map[string]TDBData
	write_path string
	in_mem     bool
}

type LogOptions struct {
	Should_log      bool
	Show_debug_logs bool
}

func NewTobsDB(write_path string, in_mem bool, log_options LogOptions) *TobsDB {
	if log_options.Should_log {
		if log_options.Show_debug_logs {
			TDBPkg.SetLogLevel(TDBPkg.LogLevelDebug)
		} else {
			TDBPkg.SetLogLevel(TDBPkg.LogLevelErrOnly)
		}
	} else {
		TDBPkg.SetLogLevel(TDBPkg.LogLevelNone)
	}

	data := make(map[string](map[string](map[int](map[string]any))))
	if in_mem {
		return &TobsDB{data: data, in_mem: in_mem}
	} else if f, err := os.Open(write_path); err == nil {
		defer f.Close()
		err := json.NewDecoder(f).Decode(&data)
		if err != nil {
			if err == io.EOF {
				TDBPkg.WarnLog("read empty db file")
			} else {
				TDBPkg.FatalLog("failed to decode db from file", err)
			}
		}

		TDBPkg.InfoLog("loaded database from file", write_path)
	} else {
		TDBPkg.ErrorLog(err)
	}
	return &TobsDB{data: data, write_path: write_path, in_mem: in_mem}
}

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
}

func (db *TobsDB) Listen(port int) {
	exit := make(chan os.Signal, 2)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)

	s := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		ReadTimeout:  0,
		WriteTimeout: 0,
	}

	upgrader := websocket.Upgrader{
		WriteBufferSize: 1024 * 10,
		ReadBufferSize:  1024 * 10,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		db_name := r.URL.Query().Get("db")
		check_schema_only, check_schema_only_err := strconv.ParseBool(r.URL.Query().Get("check_schema"))

		if len(db_name) == 0 && !check_schema_only {
			HttpError(w, http.StatusBadRequest, "Missing db name")
			return
		}

		db_data := db.data[db_name]
		schema, err := NewSchemaFromURL(r.URL, db_data)
		if err != nil {
			HttpError(w, http.StatusBadRequest, err.Error())
			return
		}

		if check_schema_only_err == nil && check_schema_only {
			TDBPkg.InfoLog("Schema checks completed: Schema is valid")
			json.NewEncoder(w).Encode(Response{
				Status:  http.StatusOK,
				Data:    *schema,
				Message: "Schema checks completed: Schema is valid",
			})
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			TDBPkg.ErrorLog(err)
			return
		}
		defer conn.Close()

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					TDBPkg.ErrorLog("unexpected close", err)
				}
				return
			}

			var req WsRequest
			json.NewDecoder(bytes.NewReader(message)).Decode(&req)

			var res Response

			switch req.Action {
			case RequestActionCreate:
				res = CreateReqHandler(schema, message)
			case RequestActionCreateMany:
				res = CreateManyReqHandler(schema, message)
			case RequestActionFind:
				res = FindReqHandler(schema, message)
			case RequestActionFindMany:
				res = FindManyReqHandler(schema, message)
			case RequestActionDelete:
				res = DeleteReqHandler(schema, message)
			case RequestActionDeleteMany:
				res = DeleteManyReqHandler(schema, message)
			case RequestActionUpdate:
				res = UpdateReqHandler(schema, message)
			case RequestActionUpdateMany:
				res = UpdateManyReqHandler(schema, message)
			}

			if err := conn.WriteJSON(res); err != nil {
				TDBPkg.ErrorLog("writing response", err)
				return
			}
			db.data[db_name] = schema.Data
		}
	})

	// listen for requests on non-blocking thread
	go func() {
		err := s.ListenAndServe()
		if err != http.ErrServerClosed {
			TDBPkg.FatalLog(err)
		}
	}()

	TDBPkg.InfoLog("TobsDB listening on port", port)
	<-exit
	TDBPkg.DebugLog("Shutting down...")
	s.Shutdown(context.Background())
	db.writeToFile()
}

func (db *TobsDB) writeToFile() {
	if db.in_mem {
		return
	}

	data, err := json.Marshal(db.data)
	if err != nil {
		TDBPkg.FatalLog("marshalling database for write", err)
	}

	err = os.WriteFile(db.write_path, data, 0644)

	if err != nil {
		TDBPkg.FatalLog("writing database to file", err)
	}
}
