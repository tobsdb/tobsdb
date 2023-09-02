package builder

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/tobshub/tobsdb/pkg"
)

type OkResponse struct {
	Data    any    `json:"data"`
	Message string `json:"message"`
	Status  int    `json:"status"`
}

type ErrResponse struct {
	Message string `json:"message"`
	Status  int    `json:"status"`
}

type CreateRequest struct {
	Data  map[string]any `json:"data"`
	Table string         `json:"table"`
}

func HttpError(w http.ResponseWriter, status int, err string) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ErrResponse{
		Message: err,
		Status:  status,
	})
}

func (db *TobsDB) CreateReqHandler(w http.ResponseWriter, r *http.Request) {
	var req CreateRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		HttpError(w, http.StatusBadRequest, err.Error())
		return
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		HttpError(w, http.StatusNotFound, "Table not found")
	} else {
		res, err := db.Create(&table, req.Data)
		if err != nil {
			HttpError(w, http.StatusBadRequest, err.Error())
		} else {
			if _, ok := db.data[table.Name]; !ok {
				db.data[table.Name] = make(map[int]map[string]any)
			}
			db.data[table.Name][res["id"].(int)] = res

			json.NewEncoder(w).Encode(OkResponse{
				Data:    res,
				Message: fmt.Sprintf("Created new row in table %s with id %d", table.Name, pkg.NumToInt(res["id"])),
				Status:  http.StatusCreated,
			})
		}
	}
}

type CreateManyRequest struct {
	Table string           `json:"table"`
	Data  []map[string]any `json:"data"`
}

func (db *TobsDB) CreateManyReqHandler(w http.ResponseWriter, r *http.Request) {
	var req CreateManyRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		HttpError(w, http.StatusBadRequest, err.Error())
		return
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		HttpError(w, http.StatusNotFound, "Table not found")
	} else {
		created_rows := []map[string]any{}
		for _, row := range req.Data {
			res, err := db.Create(&table, row)
			if err != nil {
				HttpError(w, http.StatusBadRequest, err.Error())
				return
			} else {
				created_rows = append(created_rows, res)
				if _, ok := db.data[table.Name]; !ok {
					db.data[table.Name] = make(map[int]map[string]any)
				}
				db.data[table.Name][res["id"].(int)] = res
			}
		}

		json.NewEncoder(w).Encode(OkResponse{
			Data:    created_rows,
			Message: fmt.Sprintf("Created %d new rows in table %s", len(created_rows), table.Name),
			Status:  http.StatusCreated,
		})
	}
}

type FindRequest struct {
	Table string         `json:"table"`
	Where map[string]any `json:"where"`
}

func (db *TobsDB) FindReqHandler(w http.ResponseWriter, r *http.Request) {
	var req FindRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		HttpError(w, http.StatusBadRequest, err.Error())
		return
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		HttpError(w, http.StatusNotFound, "Table not found")
	} else {
		res, err := db.FindUnique(&table, req.Where)
		if err != nil {
			HttpError(w, http.StatusBadRequest, err.Error())
		} else if res == nil {
			HttpError(
				w,
				http.StatusNotFound,
				fmt.Sprintf("No row found with constraint %v in table %s", req.Where, table.Name),
			)
			return
		} else {
			json.NewEncoder(w).Encode(OkResponse{
				Data:    res,
				Message: fmt.Sprintf("Found row with id %d in table %s", pkg.NumToInt(res["id"]), table.Name),
				Status:  http.StatusOK,
			})
		}
	}
}

func (db *TobsDB) FindManyReqHandler(w http.ResponseWriter, r *http.Request) {
	var req FindRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		HttpError(w, http.StatusBadRequest, err.Error())
		return
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		HttpError(w, http.StatusNotFound, "Table not found")
	} else {
		res, err := db.Find(&table, req.Where, true)
		if err != nil {
			HttpError(w, http.StatusBadRequest, err.Error())
		} else {
			json.NewEncoder(w).Encode(OkResponse{
				Data:    res,
				Message: fmt.Sprintf("Found %d rows in table %s", len(res), table.Name),
				Status:  http.StatusOK,
			})
		}
	}
}

type DeleteRequest struct {
	Table string         `json:"table"`
	Where map[string]any `json:"where"`
}

func (db *TobsDB) DeleteReqHandler(w http.ResponseWriter, r *http.Request) {
	var req DeleteRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		HttpError(w, http.StatusBadRequest, err.Error())
		return
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		HttpError(w, http.StatusNotFound, "Table not found")
	} else {
		row, err := db.FindUnique(&table, req.Where)
		if err != nil {
			HttpError(w, http.StatusBadRequest, err.Error())
		} else if row == nil {
			HttpError(
				w,
				http.StatusNotFound,
				fmt.Sprintf("No row found with constraint %v in table %s", req.Where, table.Name),
			)
			return
		} else {
			db.Delete(&table, row)
			json.NewEncoder(w).Encode(OkResponse{
				Data:    row,
				Message: fmt.Sprintf("Deleted row with id %d in table %s", pkg.NumToInt(row["id"]), table.Name),
				Status:  http.StatusOK,
			})
		}
	}
}

func (db *TobsDB) DeleteManyReqHandler(w http.ResponseWriter, r *http.Request) {
	var req DeleteRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		HttpError(w, http.StatusBadRequest, err.Error())
		return
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		HttpError(w, http.StatusNotFound, "Table not found")
	} else {
		rows, err := db.Find(&table, req.Where, false)
		if err != nil {
			HttpError(w, http.StatusBadRequest, err.Error())
		} else {
			for _, row := range rows {
				db.Delete(&table, row)
			}
			json.NewEncoder(w).Encode(OkResponse{
				Data:    rows,
				Message: fmt.Sprintf("Deleted %d rows in table %s", len(rows), table.Name),
				Status:  http.StatusOK,
			})
		}
	}
}

type UpdateRequest struct {
	Table string         `json:"table"`
	Where map[string]any `json:"where"`
	Data  map[string]any `json:"data"`
}

func (db *TobsDB) UpdateReqHandler(w http.ResponseWriter, r *http.Request) {
	var req UpdateRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		HttpError(w, http.StatusBadRequest, err.Error())
		return
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		HttpError(w, http.StatusNotFound, "Table not found")
	} else {
		row, err := db.FindUnique(&table, req.Where)
		if err != nil {
			HttpError(w, http.StatusBadRequest, err.Error())
			return
		} else if row == nil {
			HttpError(
				w,
				http.StatusNotFound,
				fmt.Sprintf("No row found with constraint %v in table %s", req.Where, table.Name),
			)
			return
		} else {
			err := db.Update(&table, row, req.Data)
			if err != nil {
				HttpError(w, http.StatusBadRequest, err.Error())
				return
			}
			json.NewEncoder(w).Encode(OkResponse{
				Data:    row,
				Message: fmt.Sprintf("Updated row with id %d in table %s", pkg.NumToInt(row["id"]), table.Name),
				Status:  http.StatusOK,
			})
		}
	}
}

func (db *TobsDB) UpdateManyReqHandler(w http.ResponseWriter, r *http.Request) {
	var req UpdateRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		HttpError(w, http.StatusBadRequest, err.Error())
		return
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		HttpError(w, http.StatusNotFound, "Table not found")
	} else {
		rows, err := db.Find(&table, req.Where, false)
		if err != nil {
			HttpError(w, http.StatusBadRequest, err.Error())
		} else {
			for _, row := range rows {
				err := db.Update(&table, row, req.Data)
				if err != nil {
					HttpError(w, http.StatusBadRequest, err.Error())
					return
				}
			}
			json.NewEncoder(w).Encode(OkResponse{
				Data:    rows,
				Message: fmt.Sprintf("Updated %d rows in table %s", len(rows), table.Name),
				Status:  http.StatusOK,
			})
		}
	}
}
