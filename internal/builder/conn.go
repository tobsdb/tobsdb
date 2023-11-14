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
	"reflect"
	"strconv"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tobsdb/tobsdb/internal/parser"
	"github.com/tobsdb/tobsdb/internal/query"
	"github.com/tobsdb/tobsdb/pkg"
)

type TDBWriteSettings struct {
	write_path     string
	in_mem         bool
	write_ticker   *time.Ticker
	write_interval time.Duration
}

func NewWriteSettings(write_path string, in_mem bool, write_interval_ms int) *TDBWriteSettings {
	var write_ticker *time.Ticker
	write_interval := time.Duration(write_interval_ms) * time.Millisecond
	if !in_mem {
		if len(write_path) == 0 {
			pkg.FatalLog("Must either provide db path or use in-memory mode")
		}
		write_ticker = time.NewTicker(write_interval)
	}
	return &TDBWriteSettings{write_path, in_mem, write_ticker, write_interval}
}

type TobsDB struct {
	// db_name -> schema
	data           map[string]*query.Schema
	write_settings *TDBWriteSettings
	last_change    time.Time
}

type LogOptions struct {
	Should_log      bool
	Show_debug_logs bool
}

func NewTobsDB(write_settings *TDBWriteSettings, log_options LogOptions) *TobsDB {
	if log_options.Should_log {
		if log_options.Show_debug_logs {
			pkg.SetLogLevel(pkg.LogLevelDebug)
		} else {
			pkg.SetLogLevel(pkg.LogLevelErrOnly)
		}
	} else {
		pkg.SetLogLevel(pkg.LogLevelNone)
	}

	data := make(map[string]*query.Schema)
	if len(write_settings.write_path) > 0 {
		f, open_err := os.Open(write_settings.write_path)
		if open_err != nil {
			pkg.ErrorLog(open_err)
		}
		defer f.Close()

		err := json.NewDecoder(f).Decode(&data)
		if err != nil {
			if err == io.EOF {
				pkg.WarnLog("read empty db file")
			} else {
				_, is_open_error := open_err.(*os.PathError)
				if !is_open_error {
					pkg.FatalLog("failed to decode db from file;", err)
				}
			}
		}

		pkg.InfoLog("loaded database from file", write_settings.write_path)
	}

	last_change := time.Now()
	return &TobsDB{data, write_settings, last_change}
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
	ReqId  string        `json:"__tdb_client_req_id__"` // used in tdb clients
}

func (db *TobsDB) Listen(port int) {
	exit := make(chan os.Signal, 2)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)

	s := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		ReadTimeout:  0,
		WriteTimeout: 0,
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	upgrader := websocket.Upgrader{
		WriteBufferSize: 1024 * 10,
		ReadBufferSize:  1024 * 10,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		url_query := r.URL.Query()
		db_name := url_query.Get("db")
		check_schema_only, check_schema_only_err := strconv.ParseBool(r.URL.Query().Get("check_schema"))
		is_migration, is_migration_err := strconv.ParseBool(r.URL.Query().Get("migration"))

		if len(r.URL.Query().Get("check_schema")) == 0 {
			check_schema_only = false
		} else if check_schema_only_err != nil {
			HttpError(w, http.StatusBadRequest, "Invalid check_schema value")
			return
		}

		if len(r.URL.Query().Get("migration")) == 0 {
			is_migration = false
		} else if is_migration_err != nil {
			HttpError(w, http.StatusBadRequest, "Invalid migration value")
			return
		}

		if len(db_name) == 0 && !check_schema_only {
			HttpError(w, http.StatusBadRequest, "Missing db name")
			return
		}

		schema := db.data[db_name]
		if schema == nil {
			// the db did not exist before
			_schema, err := NewSchemaFromURL(r.URL, nil, check_schema_only)
			if err != nil {
				HttpError(w, http.StatusBadRequest, err.Error())
				return
			}
			schema = _schema
		} else {
			// the db already exists
			// if no schema is provided use the saved schema
			// if a schema is provided check that it is the same as the saved schema
			// unless check_schema_only is set
			// or the migration option is set to true
			new_schema, err := NewSchemaFromURL(r.URL, schema.Data, check_schema_only)
			if err != nil {
				if err.Error() == "No schema provided" && !check_schema_only {
					pkg.InfoLog(err.Error(), "Using saved schema")
				} else {
					HttpError(w, http.StatusBadRequest, err.Error())
					return
				}
			}

			// at this point if err is not nil then we are using the old schema
			if err == nil {
				if !CompareSchemas(schema, new_schema) && !check_schema_only {
					if !is_migration {
						HttpError(w, http.StatusBadRequest, "Schema mismatch")
						return
					}

					pkg.InfoLog("Schema mismatch, migrating to provided schema")
					schema = new_schema
				}
			}
		}

		if check_schema_only {
			pkg.InfoLog("Schema checks completed: Schema is valid")
			json.NewEncoder(w).Encode(Response{
				Status:  http.StatusOK,
				Data:    schema.Tables,
				Message: "Schema checks completed: Schema is valid",
			})
			return
		}

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
			HttpError(w, http.StatusUnauthorized, "connection unauthorized")
			return
		}

		send_schema, _ := json.Marshal(schema.Tables)
		w.Header().Set("Schema", string(send_schema))
		conn, err := upgrader.Upgrade(w, r, nil)
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

			if db.write_settings.write_ticker != nil {
				// reset write timer when a reqeuest is received
				db.write_settings.write_ticker.Reset(db.write_settings.write_interval)
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

			res.ReqId = req.ReqId

			if err := conn.WriteJSON(res); err != nil {
				pkg.ErrorLog("writing response", err)
				return
			}

			if req.Action != RequestActionFind && req.Action != RequestActionFindMany {
				db.data[db_name] = schema
				db.last_change = time.Now()
			}
		}
	})

	// listen for requests on non-blocking thread
	go func() {
		err := s.ListenAndServe()
		if err != http.ErrServerClosed {
			pkg.FatalLog(err)
		}
	}()

	go func() {
		if db.write_settings.write_ticker == nil {
			return
		}

		last_write := db.last_change

		for {
			<-db.write_settings.write_ticker.C
			if db.last_change.After(last_write) {
				pkg.DebugLog("writing database to file")
				db.writeToFile()
				last_write = db.last_change
			}
		}
	}()

	pkg.InfoLog("TobsDB listening on port", port)
	<-exit
	pkg.DebugLog("Shutting down...")
	s.Shutdown(context.Background())
	db.writeToFile()
}

func (db *TobsDB) writeToFile() {
	if db.write_settings.in_mem {
		return
	}

	data, err := json.Marshal(db.data)
	if err != nil {
		pkg.FatalLog("marshalling database for write", err)
	}

	err = os.WriteFile(db.write_settings.write_path, data, 0644)

	if err != nil {
		pkg.FatalLog("writing database to file", err)
	}
}

func CompareSchemas(old_schema, new_schema *query.Schema) bool {
	if len(old_schema.Tables) != len(new_schema.Tables) {
		pkg.WarnLog(fmt.Sprintf(
			"table count mismatch %d vs %d",
			len(old_schema.Tables),
			len(new_schema.Tables)))
		return false
	}

	for key, new_table := range new_schema.Tables {
		old_table, ok := old_schema.Tables[key]
		if !ok {
			pkg.WarnLog("table in new schema but not in old schema:", key)
			return false
		}

		ok = CompareTables(old_table, new_table)
		if !ok {
			return false
		}
	}
	return true
}

func CompareTables(old_table, new_table *parser.Table) bool {
	if old_table.Name != new_table.Name {
		pkg.WarnLog("table name mismatch", old_table.Name, new_table.Name)
		return false
	}

	ok := reflect.DeepEqual(old_table.Indexes, new_table.Indexes)
	if !ok {
		pkg.WarnLog("table indexes mismatch", old_table.Indexes, new_table.Indexes)
		return false
	}

	if len(old_table.Fields) != len(new_table.Fields) {
		pkg.WarnLog(fmt.Sprintf(
			"field count mismatch on table %s: %d vs %d",
			old_table.Name,
			len(old_table.Fields),
			len(new_table.Fields)))
		return false
	}

	for key, new_field := range new_table.Fields {
		old_field, ok := old_table.Fields[key]
		if !ok {
			pkg.WarnLog(fmt.Sprintf("field in %s table in new schema but not in old schema:", old_table.Name), key)
			return false
		}
		ok = CompareFields(old_table.Name, old_field, new_field)
		if !ok {
			return false
		}
	}

	return true
}

func CompareFields(table_name string, old_field, new_field *parser.Field) bool {
	if old_field.Name != new_field.Name {
		pkg.WarnLog("field name mismatch", old_field.Name, new_field.Name)
		return false
	}

	if old_field.BuiltinType != new_field.BuiltinType {
		pkg.WarnLog("field type mismatch", old_field.BuiltinType, new_field.BuiltinType)
		return false
	}

	for key, new_prop := range new_field.Properties {
		old_prop, ok := old_field.Properties[key]
		if !ok {
			pkg.WarnLog(fmt.Sprintf(
				"field property on %s field in %s table in new schema but not in old schema:",
				old_field.Name,
				table_name),
				key)
			return reflect.DeepEqual(old_prop, new_prop)
		}

	}

	return true
}
