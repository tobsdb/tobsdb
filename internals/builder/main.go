package builder

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	TobsdbParser "github.com/tobshub/tobsdb/internals/parser"
)

type TobsDB struct {
	schema     *TobsdbParser.Schema
	data       map[string](map[int](map[string]any))
	write_path string
	in_mem     bool
}

func NewTobsDB(schema *TobsdbParser.Schema, write_path string, in_mem bool) *TobsDB {
	data := make(map[string](map[int](map[string]any)))
	if in_mem {
		return &TobsDB{schema: schema, data: data, in_mem: in_mem}
	} else if f, err := os.Open(write_path); err == nil {
		defer f.Close()
		err := json.NewDecoder(f).Decode(&data)
		if err != nil {
			log.Fatalln("Error decoding db from file:", err)
		}

		// update tables in the schema to have last ID
		for t_name, table := range schema.Tables {
			for key := range data[t_name] {
				if key > table.IdTracker {
					table.IdTracker = key
				}
			}
			schema.Tables[t_name] = table
		}
		log.Println("Loaded database from file:", write_path)
	} else {
		log.Println(err)
	}
	return &TobsDB{schema: schema, data: data, write_path: write_path, in_mem: in_mem}
}

func (db *TobsDB) Listen(port int) {
	s := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		ReadTimeout:  0,
		WriteTimeout: 0,
	}
	// http paths that call db methods
	http.HandleFunc("/createUnique", func(w http.ResponseWriter, r *http.Request) {
		db.MutatingHandlerWrapper(w, r, db.CreateReqHandler)
	})
	http.HandleFunc("/createMany", func(w http.ResponseWriter, r *http.Request) {
		db.MutatingHandlerWrapper(w, r, db.CreateManyReqHandler)
	})

	http.HandleFunc("/updateUnique", func(w http.ResponseWriter, r *http.Request) {
		db.MutatingHandlerWrapper(w, r, db.UpdateReqHandler)
	})

	http.HandleFunc("/updateMany", func(w http.ResponseWriter, r *http.Request) {
		db.MutatingHandlerWrapper(w, r, db.UpdateManyReqHandler)
	})

	http.HandleFunc("/deleteUnique", func(w http.ResponseWriter, r *http.Request) {
		db.MutatingHandlerWrapper(w, r, db.DeleteReqHandler)
	})
	http.HandleFunc("/deleteMany", func(w http.ResponseWriter, r *http.Request) {
		db.MutatingHandlerWrapper(w, r, db.DeleteManyReqHandler)
	})
	// http.HandleFunc("/deepDelete", db.DeepReqDeleteHandler)

	http.HandleFunc("/findUnique", db.FindReqHandler)
	http.HandleFunc("/findMany", db.FindManyReqHandler)

	// http.HandleFunc("/connect", db.ConnectReqHandler)
	// http.HandleFunc("/disconnect", db.DisconnectReqHandler)

	log.Fatal(s.ListenAndServe())
}

func (db *TobsDB) MutatingHandlerWrapper(w http.ResponseWriter, r *http.Request, handler func(w http.ResponseWriter, r *http.Request)) {
	handler(w, r)
	db.WriteToFile()
}

func (db *TobsDB) WriteToFile() {
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
