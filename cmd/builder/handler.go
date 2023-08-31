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
	var req CreateRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		http.Error(w, "Table not found", http.StatusNotFound)
	} else {
		res, err := db.Create(&table, req.Data)
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

type FindRequest struct {
	Table string         `json:"table"`
	Where map[string]any `json:"where"`
}

func (db *TobsDB) FindManyReqHandler(w http.ResponseWriter, r *http.Request) {
	var req FindRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		http.Error(w, "Table not found", http.StatusNotFound)
	} else {
		res, err := db.Find(&table, req.Where)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			json.NewEncoder(w).Encode(res)
			w.Write([]byte(fmt.Sprintf("Found %d rows in table %s", len(res), table.Name)))
		}
	}
}

type DeleteRequest struct {
	Table string         `json:"table"`
	Where map[string]any `json:"where"`
}

func (db *TobsDB) DeleteManyReqHandlert(w http.ResponseWriter, r *http.Request) {
	var req DeleteRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		http.Error(w, "Table not found", http.StatusNotFound)
	} else {
		delete_count := 0
		rows, err := db.Find(&table, req.Where)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			for _, row := range rows {
				db.Delete(&table, row, req.Where)
				delete_count++
			}
			w.Write([]byte(fmt.Sprintf("Deleted %d rows in table %s", delete_count, table.Name)))
		}
	}
}

type UpdateRequest struct {
	Table string         `json:"table"`
	Where map[string]any `json:"where"`
	Data  map[string]any `json:"data"`
}

func (db *TobsDB) UpdateManyReqHandler(w http.ResponseWriter, r *http.Request) {
	var req UpdateRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		http.Error(w, "Table not found", http.StatusNotFound)
	} else {
		rows, err := db.Find(&table, req.Where)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			for _, row := range rows {
				err := db.Update(&table, row, req.Data)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
			}
			json.NewEncoder(w).Encode(rows)
			w.Write([]byte(fmt.Sprintf("Updated %d rows in table %s", len(rows), table.Name)))
		}
	}
}
