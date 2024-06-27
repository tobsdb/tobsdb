package conn

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/pkg"
)

func Listen(tdb *builder.TobsDB, port int) {
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
				if errors.Is(err, net.ErrClosed) {
					return
				}
				pkg.ErrorLog(err)
			}
			pkg.InfoLog("Connection from", conn.RemoteAddr())
			go HandleConnection(tdb, conn)
		}
	}()

	pkg.InfoLog("TobsDB listening on port", port)
	<-exit
	pkg.DebugLog("Shutting down...")
	if err := listener.Close(); err != nil {
		pkg.ErrorLog("failed to close listener", err)
	}
	tdb.WriteToFile()
}

func ResolveSchema(tdb *builder.TobsDB, r ConnRequest) (*builder.Schema, error) {
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

	if !tdb.WriteSettings.InMem {
		schema.WriteTicker = time.NewTicker(tdb.WriteSettings.WriteInterval)
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
					schema.WriteToFile(tdb.WriteSettings.WritePath)
					last_write = schema.LastChange
				}
			}
		}()
	}

	return schema, nil
}
