package builder

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	TobsdbParser "github.com/tobshub/tobsdb/cmd/parser"
)

type TobsDB struct {
	schema     *TobsdbParser.Schema
	data       map[string](map[int](map[string]any))
	write_path string
}

func NewTobsDB(schema *TobsdbParser.Schema, write_path string) *TobsDB {
	data := make(map[string](map[int](map[string]any)))
	if f, err := os.Open(write_path); err == nil {
		defer f.Close()
		json.NewDecoder(f).Decode(&data)
	}
	return &TobsDB{schema: schema, data: data, write_path: write_path}
}

func (db *TobsDB) Listen(port int) {
	s := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		ReadTimeout:  0,
		WriteTimeout: 0,
	}
	// http paths that call db methods
	http.HandleFunc("/create", func(w http.ResponseWriter, r *http.Request) {
		db.MutatingHandlerWrapper(w, r, db.CreateReqHandler)
	})
	// http.HandleFunc("/update", db.UpdateReqHandler)

	http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		db.MutatingHandlerWrapper(w, r, db.DeleteReqHandler)
	})
	// http.HandleFunc("/deepDelete", db.DeepReqDeleteHandler)

	http.HandleFunc("/find", db.FindReqHandler)

	// http.HandleFunc("/connect", db.ConnectReqHandler)
	// http.HandleFunc("/disconnect", db.DisconnectReqHandler)

	s.ListenAndServe()
}

func (db *TobsDB) MutatingHandlerWrapper(w http.ResponseWriter, r *http.Request, handler func(w http.ResponseWriter, r *http.Request)) {
	handler(w, r)
	db.WriteToFile()
}

func (db *TobsDB) WriteToFile() {
	data, err := json.Marshal(db.data)
	if err != nil {
		log.Fatalln("Error marshalling database for write:", err)
	}

	err = os.WriteFile(db.write_path, data, 0644)

	if err != nil {
		log.Fatalln("Error writing database to file:", err)
	}
}
