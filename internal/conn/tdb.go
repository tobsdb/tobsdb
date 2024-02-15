package conn

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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
	write_interval time.Duration
}

func NewWriteSettings(write_path string, in_mem bool, write_interval_ms int) *TDBWriteSettings {
	write_interval := time.Duration(write_interval_ms) * time.Millisecond
	if !in_mem {
		if len(write_path) == 0 {
			pkg.FatalLog("Must either provide db path or use in-memory mode")
		}
	}
	return &TDBWriteSettings{write_path, in_mem, write_interval}
}

type (
	TdbSchemaMap = pkg.Map[string, *builder.Schema]
	TdbUserMap   = pkg.Map[string, *TdbUser]
)

type TobsDB struct {
	Locker sync.RWMutex
	// db_name -> schema
	Data           TdbSchemaMap
	write_settings *TDBWriteSettings
	last_change    time.Time

	Users TdbUserMap
}

type TdbMeta struct {
	SchemaKeys []string
	Users      TdbUserMap
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
	if auth.Username != "" {
		user := NewUser(auth.Username, auth.Password, TdbUserRoleAdmin)
		users.Set(user.Id, user)
	}
	last_change := time.Now()

	return &TobsDB{sync.RWMutex{}, data, write_settings, last_change, users}
}

func (tdb *TobsDB) GetLocker() *sync.RWMutex { return &tdb.Locker }

func (tdb *TobsDB) Listen(port int) {
	exit := make(chan os.Signal, 2)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)

	listener, err := net.Listen("tcp4", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		pkg.FatalLog(err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				pkg.ErrorLog(err)
			}
			pkg.InfoLog("Connection from", conn.RemoteAddr())
			go tdb.HandleConnection(conn)
		}
	}()

	pkg.InfoLog("TobsDB listening on port", port)
	<-exit
	pkg.DebugLog("Shutting down...")
	listener.Close()
	tdb.WriteToFile()
}

func (tdb *TobsDB) ResolveSchema(r ConnRequest) (*builder.Schema, error) {
	tdb.Locker.Lock()
	defer tdb.Locker.Unlock()

	schema := tdb.Data.Get(r.DB)
	if schema == nil {
		// the db did not exist before
		_schema, err := builder.NewSchemaFromString(r.Schema, nil, false)
		if err != nil {
			return nil, err
		}
		schema = _schema
		schema.Name = r.DB
		tdb.Data.Set(r.DB, schema)
	}

	if !tdb.write_settings.in_mem {
		schema.WritePath = path.Join(tdb.write_settings.write_path, schema.Name)
		schema.WriteTicker = time.NewTicker(tdb.write_settings.write_interval)
		schema.LastChange = time.Now()

		go func() {
			if schema.WriteTicker == nil {
				return
			}
			last_write := schema.LastChange
			for {
				<-schema.WriteTicker.C
				if schema.LastChange.After(last_write) {
					pkg.DebugLog("writing database", schema.Name)
					schema.WriteToFile()
					last_write = schema.LastChange
				}
			}
		}()
	}

	return schema, nil
}

func ReadFromFile(write_settings *TDBWriteSettings) (data TdbSchemaMap, users TdbUserMap) {
	data = TdbSchemaMap{}
	users = TdbUserMap{}
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

	meta := &TdbMeta{[]string{}, TdbUserMap{}}
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

func (tdb *TobsDB) WriteToFile() {
	if tdb.write_settings.in_mem {
		return
	}

	pkg.DebugLog("writing database to disk")

	tdb.Locker.RLock()
	defer tdb.Locker.RUnlock()

	meta_data, err := json.Marshal(TdbMeta{tdb.Data.Keys(), tdb.Users})
	if err != nil {
		pkg.FatalLog(err)
	}

	if _, err := os.Stat(tdb.write_settings.write_path); os.IsNotExist(err) {
		os.Mkdir(tdb.write_settings.write_path, 0755)
	}

	if err := os.WriteFile(path.Join(tdb.write_settings.write_path, "meta.tdb"), meta_data, 0644); err != nil {
		pkg.FatalLog(err)
	}

	for _, schema := range tdb.Data {
		err := schema.WriteToFile()
		if err != nil {
			pkg.FatalLog(err)
		}
	}
}
