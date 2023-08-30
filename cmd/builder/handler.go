package builder

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type CreateRequest struct {
	Data  map[string]any `json:"data"`
	Table string         `json:"table"`
}

func (db *TobsDB) CreateReqHandler(w http.ResponseWriter, r *http.Request) {
	var d CreateRequest

	err := json.NewDecoder(r.Body).Decode(&d)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	table_name := d.Table

	if table, ok := db.schema.Tables[table_name]; !ok {
		http.Error(w, "Table not found", http.StatusNotFound)
	} else {
		res, err := table.Create(d.Data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			json.NewEncoder(w).Encode(res)
			if _, ok := db.data[table.Name]; !ok {
				db.data[table.Name] = make(map[int]map[string]any)
			}
			db.data[table.Name][res["id"].(int)] = res
			w.Write([]byte(fmt.Sprintf("Created new row in table %s with id %d", table.Name, res["id"].(int))))
		}
	}
}
