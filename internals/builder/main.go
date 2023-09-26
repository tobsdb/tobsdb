package builder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gorilla/websocket"

	TDBParser "github.com/tobshub/tobsdb/internals/parser"
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

func NewTobsDB(write_path string, in_mem bool) *TobsDB {
	data := make(map[string](map[string](map[int](map[string]any))))
	if in_mem {
		return &TobsDB{data: data, in_mem: in_mem}
	} else if f, err := os.Open(write_path); err == nil {
		defer f.Close()
		err := json.NewDecoder(f).Decode(&data)
		if err != nil {
			if err == io.EOF {
				log.Println("Warning: read empty database")
			} else {
				log.Fatalln("Error decoding db from file:", err)
			}
		}

		// // update tables in the schema to have last ID
		// for t_name, table := range schema.Tables {
		// 	for key := range data[t_name] {
		// 		if key > table.IdTracker {
		// 			table.IdTracker = key
		// 		}
		// 	}
		// 	schema.Tables[t_name] = table
		// }
		log.Println("Loaded database from file:", write_path)
	} else {
		log.Println(err)
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
		if len(db_name) == 0 {
			HttpError(w, http.StatusBadRequest, "Missing db name")
			return
		}
		db_data := db.data[db_name]
		schema := NewSchemaFromURL(r.URL, db_data)

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println(err)
			return
		}
		defer conn.Close()

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					log.Println("Error reading message:", err)
				}
				return
			}

			var req WsRequest
			json.NewDecoder(bytes.NewReader(message)).Decode(&req)

			var res Response

			switch req.Action {
			case RequestActionCreate:
				res = CreateReqHandler(&schema, message)
			case RequestActionCreateMany:
				res = CreateManyReqHandler(&schema, message)
			case RequestActionFind:
				res = FindReqHandler(&schema, message)
			case RequestActionFindMany:
				res = FindManyReqHandler(&schema, message)
			case RequestActionDelete:
				res = DeleteReqHandler(&schema, message)
			case RequestActionDeleteMany:
				res = DeleteManyReqHandler(&schema, message)
			case RequestActionUpdate:
				res = UpdateReqHandler(&schema, message)
			case RequestActionUpdateMany:
				res = UpdateManyReqHandler(&schema, message)
			}

			if err := conn.WriteJSON(res); err != nil {
				log.Println("Error writing response:", err)
				return
			}
			db.data[db_name] = schema.Data
		}
	})

	// listen for requests on non-blocking thread
	go func() {
		err := s.ListenAndServe()
		if err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	log.Println("TobsDB listening on port", port)
	<-exit
	log.Println("Shutting down...")
	s.Shutdown(context.Background())
	db.writeToFile()
}

func (db *TobsDB) writeToFile() {
	if db.in_mem {
		return
	}

	data, err := json.Marshal(db.data)
	if err != nil {
		log.Fatalln("Error marshalling database for write:", err)
	}

	err = os.WriteFile(db.write_path, data, 0644)

	if err != nil {
		log.Fatalln("Error writing database to file:", err)
	}
}
