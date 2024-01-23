package conn

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"time"

	"github.com/tobsdb/tobsdb/internal/builder"
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
	Locker sync.RWMutex
	// db_name -> schema
	data           pkg.Map[string, *builder.Schema]
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

	data := ReadFromFile(write_settings)
	last_change := time.Now()

	return &TobsDB{sync.RWMutex{}, data, write_settings, last_change}
}

func (db *TobsDB) GetLocker() *sync.RWMutex { return &db.Locker }

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

	http.HandleFunc("/", db.HandleConnection)

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
				db.WriteToFile()
				last_write = db.last_change
			}
		}
	}()

	pkg.InfoLog("TobsDB listening on port", port)
	<-exit
	pkg.DebugLog("Shutting down...")
	s.Shutdown(context.Background())
	db.WriteToFile()
}

func (db *TobsDB) ResolveSchema(db_name string, Url *url.URL, is_migration bool) (*builder.Schema, error) {
	db.Locker.Lock()
	defer db.Locker.Unlock()
	schema := db.data.Get(db_name)
	if schema == nil {
		// the db did not exist before
		_schema, err := builder.NewSchemaFromURL(Url, nil, false)
		if err != nil {
			return nil, err
		}
		schema = _schema
	} else {
		// the db already exists
		// if no schema is provided use the saved schema
		// if a schema is provided check that it is the same as the saved schema
		// unless the migration option is set to true
		new_schema, err := builder.NewSchemaFromURL(Url, schema.Data, false)
		if err != nil {
			if err.Error() == "No schema provided" {
				pkg.InfoLog(err.Error(), "Using saved schema")
				for _, table := range schema.Tables.Idx {
					table.Rows().Locker = sync.RWMutex{}
					table.Rows().Map.SetComparisonFunc(func(a, b builder.TDBTableRow) bool {
						return builder.GetPrimaryKey(a) < builder.GetPrimaryKey(b)
					})
					table.Schema = schema
					for _, field := range table.Fields.Idx {
						field.Table = table
					}
				}
			} else {
				return nil, err
			}
		} else {
			// at this point if err is not nil then we have both the old schema and new schema
			if !CompareSchemas(schema, new_schema) {
				if !is_migration {
					return nil, fmt.Errorf("Schema mismatch")
				}

				pkg.InfoLog("Schema mismatch, migrating to provided schema")
			}
			schema = new_schema
		}
	}
	db.data.Set(db_name, schema)
	return schema, nil
}

func ReadFromFile(write_settings *TDBWriteSettings) pkg.Map[string, *builder.Schema] {
	data := make(pkg.Map[string, *builder.Schema])
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
	return data
}

func (db *TobsDB) WriteToFile() {
	if db.write_settings.in_mem {
		return
	}

	pkg.DebugLog("writing database to file")

	db.Locker.RLock()
	defer db.Locker.RUnlock()
	data, err := json.Marshal(db.data)
	if err != nil {
		pkg.FatalLog("marshalling database for write", err)
	}

	err = os.WriteFile(db.write_settings.write_path, data, 0644)

	if err != nil {
		pkg.FatalLog("writing database to file", err)
	}
}

// TODO: write tests
func CompareSchemas(old_schema, new_schema *builder.Schema) bool {
	if old_schema.Tables.Len() != new_schema.Tables.Len() {
		pkg.WarnLog(fmt.Sprintf("table count mismatch %d vs %d",
			old_schema.Tables.Len(), new_schema.Tables.Len()))
		return false
	}

	for key, new_table := range new_schema.Tables.Idx {
		if !old_schema.Tables.Has(key) {
			pkg.WarnLog("table in new schema but not in old schema:", key)
			return false
		}
		old_table := old_schema.Tables.Get(key)

		ok := CompareTables(old_table, new_table)
		if !ok {
			return false
		}
	}
	return true
}

func CompareTables(old_table, new_table *builder.Table) bool {
	if old_table.Name != new_table.Name {
		pkg.WarnLog("table name mismatch", old_table.Name, new_table.Name)
		return false
	}

	ok := reflect.DeepEqual(old_table.Indexes, new_table.Indexes)
	if !ok {
		pkg.WarnLog("table indexes mismatch", old_table.Indexes, new_table.Indexes)
		return false
	}

	if old_table.Fields.Len() != new_table.Fields.Len() {
		pkg.WarnLog(fmt.Sprintf("field count mismatch on table %s: %d vs %d",
			old_table.Name, old_table.Fields.Len(), new_table.Fields.Len()))
		return false
	}

	for key, new_field := range new_table.Fields.Idx {
		if !old_table.Fields.Has(key) {
			pkg.WarnLog(fmt.Sprintf("field in %s table in new schema but not in old schema:", old_table.Name), key)
			return false
		}
		old_field := old_table.Fields.Get(key)
		ok := CompareFields(old_table.Name, old_field, new_field)
		if !ok {
			return false
		}
	}

	return true
}

func CompareFields(table_name string, old_field, new_field *builder.Field) bool {
	if old_field.Name != new_field.Name {
		pkg.WarnLog("field name mismatch", old_field.Name, new_field.Name)
		return false
	}

	if old_field.BuiltinType != new_field.BuiltinType {
		pkg.WarnLog("field type mismatch", old_field.BuiltinType, new_field.BuiltinType)
		return false
	}

	for key, new_prop := range new_field.Properties {
		if !old_field.Properties.Has(key) {
			pkg.WarnLog(
				fmt.Sprintf("field property on %s field in %s table in new schema but not in old schema:",
					old_field.Name,
					table_name),
				key,
			)
			return false
		}
		old_prop := old_field.Properties.Get(key)
		if old_prop != new_prop {
			pkg.WarnLog("field property mismatch", old_prop, new_prop)
			return false
		}
	}

	return true
}
