package conn

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
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

	Users pkg.Map[int, *TdbUser]
}

type TdbMeta struct {
	SchemaKeys []string
	Users      pkg.Map[int, *TdbUser]
}

type LogOptions struct {
	Should_log      bool
	Show_debug_logs bool
}

type AuthSettings struct {
	Username string
	Password string
}

func GobRegisterTypes() {
	gob.Register(int(0))
	gob.Register(float64(0.))
	gob.Register(string(""))
	gob.Register(time.Time{})
	gob.Register(bool(false))
	gob.Register([]any{})
}

func NewTobsDB(auth AuthSettings, write_settings *TDBWriteSettings, log_options LogOptions) *TobsDB {
	GobRegisterTypes()
	if log_options.Should_log {
		if log_options.Show_debug_logs {
			pkg.SetLogLevel(pkg.LogLevelDebug)
		} else {
			pkg.SetLogLevel(pkg.LogLevelErrOnly)
		}
	} else {
		pkg.SetLogLevel(pkg.LogLevelNone)
	}

	data, users := ReadFromFile(write_settings)
	if len(users) == 0 {
		users.Set(1, NewUser(1, auth.Username, auth.Password, TdbUserRoleAdmin))
	}
	last_change := time.Now()

	return &TobsDB{sync.RWMutex{}, data, write_settings, last_change, users}
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

func (db *TobsDB) ResolveSchema(db_name string, Url *url.URL) (*builder.Schema, error) {
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
		schema.Name = db_name
		db.data.Set(db_name, schema)
	}

	return schema, nil
}

func ReadFromFile(write_settings *TDBWriteSettings) (data pkg.Map[string, *builder.Schema], users pkg.Map[int, *TdbUser]) {
	data = pkg.Map[string, *builder.Schema]{}
	users = pkg.Map[int, *TdbUser]{}
	if write_settings.write_path == "" {
		return
	}

	f, open_err := os.Open(path.Join(write_settings.write_path, "meta.tdb"))
	if open_err != nil {
		if !errors.Is(open_err, &os.PathError{}) {
			pkg.ErrorLog("failed to open db file;", open_err)
			return
		}
		pkg.ErrorLog(open_err)
	}
	defer f.Close()

	meta := &TdbMeta{[]string{}, pkg.Map[int, *TdbUser]{}}
	err := json.NewDecoder(f).Decode(meta)
	if err != nil {
		if err == io.EOF {
			pkg.WarnLog("read empty db file")
			return
		} else {
			pkg.FatalLog(err)
		}
	}

	users = meta.Users
	for _, key := range meta.SchemaKeys {
		s, err := builder.NewSchemaFromPath(write_settings.write_path, key)
		if err != nil {
			pkg.FatalLog(err)
		}
		data.Set(key, s)
	}

	pkg.InfoLog("loaded database from file", write_settings.write_path)
	return
}

func (db *TobsDB) WriteToFile() {
	if db.write_settings.in_mem {
		return
	}

	pkg.DebugLog("writing database to disk")

	db.Locker.RLock()
	defer db.Locker.RUnlock()

	meta_data, err := json.Marshal(TdbMeta{db.data.Keys(), db.Users})
	if err != nil {
		pkg.FatalLog(err)
	}

	if _, err := os.Stat(db.write_settings.write_path); os.IsNotExist(err) {
		os.Mkdir(db.write_settings.write_path, 0755)
	}

	if err := os.WriteFile(path.Join(db.write_settings.write_path, "meta.tdb"), meta_data, 0644); err != nil {
		pkg.FatalLog(err)
	}

	for _, schema := range db.data {
		err := schema.WriteToFile(db.write_settings.write_path)
		if err != nil {
			pkg.FatalLog(err)
		}
	}
}
