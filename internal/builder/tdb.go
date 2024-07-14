package builder

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/tobsdb/tobsdb/internal/auth"
	"github.com/tobsdb/tobsdb/pkg"
)

type TDBWriteSettings struct {
	WritePath     string
	InMem         bool
	WriteInterval time.Duration
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
	TdbSchemaMap = pkg.Map[string, *Schema]
	TdbUserMap   = pkg.Map[string, *auth.TdbUser]
)

type TobsDB struct {
	Locker sync.RWMutex
	// db_name -> schema
	Data          TdbSchemaMap
	WriteSettings *TDBWriteSettings
	LastChange    time.Time

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

func NewTobsDB(auth_settings AuthSettings, write_settings *TDBWriteSettings, log_options LogOptions) *TobsDB {
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

	tdb := &TobsDB{Locker: sync.RWMutex{}, WriteSettings: write_settings }
	tdb.Data, tdb.Users = ReadFromFile(tdb)
	if auth_settings.Username != "" {
		user := auth.NewUser(auth_settings.Username, auth_settings.Password)
		user.IsRoot = true
		tdb.Users.Set(user.Id, user)
	}
	tdb.LastChange = time.Now()
	return tdb
}

func (tdb *TobsDB) GetLocker() *sync.RWMutex { return &tdb.Locker }

func ReadFromFile(tdb *TobsDB) (data TdbSchemaMap, users TdbUserMap) {
	data = TdbSchemaMap{}
	users = TdbUserMap{}
	if tdb.WriteSettings.WritePath == "" {
		return
	}

	f, open_err := os.Open(path.Join(tdb.WriteSettings.WritePath, "meta.tdb"))
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
		s, err := NewSchemaFromPath(tdb.WriteSettings.WritePath, key)
		if err != nil {
			pkg.FatalLog(err)
		}
		s.tdb = tdb
		data.Set(key, s)
	}

	pkg.InfoLog("loaded database from file", tdb.WriteSettings.WritePath)
	return
}

func (tdb *TobsDB) WriteToFile() {
	if tdb.WriteSettings.InMem {
		return
	}

	pkg.DebugLog("writing database to disk", tdb.WriteSettings.WritePath)

	tdb.Locker.RLock()
	defer tdb.Locker.RUnlock()

	meta_data, err := json.Marshal(TdbMeta{tdb.Data.Keys(), tdb.Users})
	if err != nil {
		pkg.FatalLog(err)
	}

	if _, err := os.Stat(tdb.WriteSettings.WritePath); os.IsNotExist(err) {
		os.Mkdir(tdb.WriteSettings.WritePath, 0755)
	}

	if err := os.WriteFile(path.Join(tdb.WriteSettings.WritePath, "meta.tdb"), meta_data, 0644); err != nil {
		pkg.FatalLog(err)
	}

	for _, schema := range tdb.Data {
		err := schema.WriteToFile()
		if err != nil {
			pkg.FatalLog(err)
		}
	}
}
