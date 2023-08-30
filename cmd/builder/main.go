package builder

import (
	"fmt"
	"net/http"

	TobsdbParser "github.com/tobshub/tobsdb/cmd/parser"
)

type TobsDB struct {
	schema *TobsdbParser.Schema
	data   map[string](map[int](map[string]any))
}

func NewTobsDB(schema *TobsdbParser.Schema) *TobsDB {
	return &TobsDB{schema: schema, data: make(map[string]map[int]map[string]any, 0)}
}

func (db *TobsDB) Listen(port int) {
	s := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		ReadTimeout:  0,
		WriteTimeout: 0,
	}
	// http paths that call db methods
	http.HandleFunc("/create", db.CreateReqHandler)
	// http.HandleFunc("/update", db.UpdateReqHandler)

	// http.HandleFunc("/delete", db.DeleteReqHandler)
	// http.HandleFunc("/deepDelete", db.DeepReqDeleteHandler)

	// http.HandleFunc("/find", db.FindReqHandler)

	// http.HandleFunc("/connect", db.ConnectReqHandler)
	// http.HandleFunc("/disconnect", db.DisconnectReqHandler)

	s.ListenAndServe()
}
