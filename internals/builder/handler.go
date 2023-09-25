package builder

import (
	"encoding/json"
	"fmt"
	"net/http"

	TDBLogger "github.com/tobshub/tobsdb/internals/logger"
	TDBPkg "github.com/tobshub/tobsdb/pkg"
)

type Response struct {
	Data    any    `json:"data"`
	Message string `json:"message"`
	Status  int    `json:"status"`
}

func HttpError(w http.ResponseWriter, status int, err string) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(Response{
		Message: err,
		Status:  status,
	})
}

func HttpErrorLogger(method, table string) func(w http.ResponseWriter, status int, err string) {
	return func(w http.ResponseWriter, status int, err string) {
		TDBLogger.ErrorLog(method, table, err)
		HttpError(w, status, err)
	}
}

func NewErrorResponse(status int, err string) Response {
	return Response{
		Message: err,
		Status:  status,
	}
}

func NewResponse(status int, message string, data any) Response {
	return Response{
		Data:    data,
		Message: message,
		Status:  status,
	}
}

type CreateRequest struct {
	Data  map[string]any `json:"data"`
	Table string         `json:"table"`
}

func (db *TobsDB) CreateReqHandler(raw []byte) Response {
	var req CreateRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	} else {
		res, err := db.Create(&table, req.Data)
		if err != nil {
			return NewErrorResponse(http.StatusBadRequest, err.Error())
		}

		if _, ok := db.data[table.Name]; !ok {
			db.data[table.Name] = make(map[int]map[string]any)
		}
		db.data[table.Name][res["id"].(int)] = res
		db.schema.Tables[table.Name] = table

		return NewResponse(
			http.StatusCreated,
			fmt.Sprintf(
				"Created new row in table %s with id %d",
				table.Name,
				TDBPkg.NumToInt(res["id"]),
			),
			res,
		)
	}
}

type CreateManyRequest struct {
	Table string           `json:"table"`
	Data  []map[string]any `json:"data"`
}

func (db *TobsDB) CreateManyReqHandler(raw []byte) Response {
	var req CreateManyRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	} else {
		created_rows := []map[string]any{}
		for _, row := range req.Data {
			res, err := db.Create(&table, row)
			if err != nil {
				return NewErrorResponse(http.StatusBadRequest, err.Error())
			}
			created_rows = append(created_rows, res)
			if _, ok := db.data[table.Name]; !ok {
				db.data[table.Name] = make(map[int]map[string]any)
			}
			db.data[table.Name][res["id"].(int)] = res
			db.schema.Tables[table.Name] = table
		}

		return NewResponse(
			http.StatusCreated,
			fmt.Sprintf("Created %d new rows in table %s", len(created_rows), table.Name),
			created_rows,
		)
	}
}

type FindRequest struct {
	Table string         `json:"table"`
	Where map[string]any `json:"where"`
}

func (db *TobsDB) FindReqHandler(raw []byte) Response {
	var req FindRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	} else {
		res, err := db.FindUnique(&table, req.Where)
		if err != nil {
			return NewErrorResponse(http.StatusBadRequest, err.Error())
		} else if res == nil {
			return NewErrorResponse(
				http.StatusNotFound,
				fmt.Sprintf("No row found with constraint %v in table %s", req.Where, table.Name),
			)
		}

		return NewResponse(
			http.StatusOK,
			fmt.Sprintf("Found row with id %d in table %s", TDBPkg.NumToInt(res["id"]), table.Name),
			res,
		)
	}
}

func (db *TobsDB) FindManyReqHandler(raw []byte) Response {
	var req FindRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	} else {
		res, err := db.Find(&table, req.Where, true)
		if err != nil {
			return NewErrorResponse(http.StatusBadRequest, err.Error())
		}

		return NewResponse(
			http.StatusOK,
			fmt.Sprintf("Found %d rows in table %s", len(res), table.Name),
			res,
		)
	}
}

type DeleteRequest struct {
	Table string         `json:"table"`
	Where map[string]any `json:"where"`
}

func (db *TobsDB) DeleteReqHandler(raw []byte) Response {
	var req DeleteRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	} else {
		row, err := db.FindUnique(&table, req.Where)
		if err != nil {
			return NewErrorResponse(http.StatusBadRequest, err.Error())
		} else if row == nil {
			return NewErrorResponse(
				http.StatusNotFound,
				fmt.Sprintf("No row found with constraint %v in table %s", req.Where, table.Name),
			)
		}

		db.Delete(&table, row)
		return NewResponse(
			http.StatusOK,
			fmt.Sprintf("Deleted row with id %d in table %s", TDBPkg.NumToInt(row["id"]), table.Name),
			row,
		)
	}
}

func (db *TobsDB) DeleteManyReqHandler(raw []byte) Response {
	var req DeleteRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	} else {
		rows, err := db.Find(&table, req.Where, false)
		if err != nil {
			return NewErrorResponse(http.StatusBadRequest, err.Error())
		}

		for _, row := range rows {
			db.Delete(&table, row)
		}

		return NewResponse(
			http.StatusOK,
			fmt.Sprintf("Deleted %d rows in table %s", len(rows), table.Name),
			rows,
		)
	}
}

type UpdateRequest struct {
	Table string         `json:"table"`
	Where map[string]any `json:"where"`
	Data  map[string]any `json:"data"`
}

func (db *TobsDB) UpdateReqHandler(raw []byte) Response {
	var req UpdateRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	} else {
		row, err := db.FindUnique(&table, req.Where)
		if err != nil {
			return NewErrorResponse(http.StatusBadRequest, err.Error())
		} else if row == nil {
			return NewErrorResponse(
				http.StatusNotFound,
				fmt.Sprintf("No row found with constraint %v in table %s", req.Where, table.Name),
			)
		}

		err = db.Update(&table, row, req.Data)
		if err != nil {
			return NewErrorResponse(http.StatusBadRequest, err.Error())
		}
		db.schema.Tables[table.Name] = table
		return NewResponse(
			http.StatusOK,
			fmt.Sprintf("Updated row with id %d in table %s", TDBPkg.NumToInt(row["id"]), table.Name),
			row,
		)
	}
}

func (db *TobsDB) UpdateManyReqHandler(raw []byte) Response {
	var req UpdateRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if table, ok := db.schema.Tables[req.Table]; !ok {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	} else {
		rows, err := db.Find(&table, req.Where, false)
		if err != nil {
			return NewErrorResponse(http.StatusBadRequest, err.Error())
		}

		for _, row := range rows {
			err := db.Update(&table, row, req.Data)
			if err != nil {
				return NewErrorResponse(http.StatusBadRequest, err.Error())
			}
		}
		db.schema.Tables[table.Name] = table
		return NewResponse(
			http.StatusOK,
			fmt.Sprintf("Updated %d rows in table %s", len(rows), table.Name),
			rows,
		)
	}
}
