package conn

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/internal/query"
	"github.com/tobsdb/tobsdb/pkg"
)

type Response struct {
	Data    any    `json:"data"`
	Message string `json:"message"`
	Status  int    `json:"status"`
	// don't manually set this. it comes from the client
	ReqId int `json:"__tdb_client_req_id__"`
}

func NewErrorResponse(status int, err string) Response {
	return Response{Message: err, Status: status}
}

func NewResponse(status int, message string, data any) Response {
	return Response{Data: data, Message: message, Status: status}
}

type CreateRequest struct {
	Data  map[string]any `json:"data"`
	Table string         `json:"table"`
}

func CreateReqHandler(schema *builder.Schema, raw []byte) Response {
	var req CreateRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	table, ok := schema.Tables[req.Table]
	if !ok {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	}

	res, err := query.Create(table, req.Data)
	if err != nil {
		if query_error, ok := err.(*query.QueryError); ok {
			return NewErrorResponse(query_error.Status(), query_error.Error())
		}
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	schema.Data[table.Name].Rows[res[builder.SYS_PRIMARY_KEY].(int)] = res

	return NewResponse(http.StatusCreated,
		fmt.Sprintf("Created new row in table %s",
			table.Name),
		res,
	)
}

type CreateManyRequest struct {
	Table string           `json:"table"`
	Data  []map[string]any `json:"data"`
}

func CreateManyReqHandler(schema *builder.Schema, raw []byte) Response {
	var req CreateManyRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if table, ok := schema.Tables[req.Table]; !ok {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	} else {
		created_rows := []map[string]any{}
		for _, row := range req.Data {
			res, err := query.Create(table, row)
			if err != nil {
				if query_error, ok := err.(*query.QueryError); ok {
					return NewErrorResponse(query_error.Status(), query_error.Error())
				}
				return NewErrorResponse(http.StatusBadRequest, err.Error())
			}
			created_rows = append(created_rows, res)
			schema.Data[table.Name].Rows[res[builder.SYS_PRIMARY_KEY].(int)] = res
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

func FindReqHandler(schema *builder.Schema, raw []byte) Response {
	var req FindRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if table, ok := schema.Tables[req.Table]; !ok {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	} else {
		res, err := query.FindUnique(table, req.Where)
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
			fmt.Sprintf("Found row with id %d in table %s", pkg.NumToInt(res[builder.SYS_PRIMARY_KEY]), table.Name),
			res,
		)
	}
}

type FindManyRequest struct {
	Table   string            `json:"table"`
	Where   map[string]any    `json:"where"`
	Take    map[string]int    `json:"take"`
	OrderBy map[string]string `json:"order_by"`
	Cursor  map[string]int    `json:"cursor"`
}

func FindManyReqHandler(schema *builder.Schema, raw []byte) Response {
	var req FindManyRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if table, ok := schema.Tables[req.Table]; !ok {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	} else {
		res, err := query.FindWithArgs(table, query.FindArgs{
			Where:   req.Where,
			Take:    req.Take,
			OrderBy: req.OrderBy,
			Cursor:  req.Cursor,
		}, true)
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

func DeleteReqHandler(schema *builder.Schema, raw []byte) Response {
	var req DeleteRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if table, ok := schema.Tables[req.Table]; !ok {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	} else {
		row, err := query.FindUnique(table, req.Where)
		if err != nil {
			return NewErrorResponse(http.StatusBadRequest, err.Error())
		} else if row == nil {
			return NewErrorResponse(
				http.StatusNotFound,
				fmt.Sprintf("No row found with constraint %v in table %s", req.Where, table.Name),
			)
		}

		query.Delete(table, row)
		return NewResponse(
			http.StatusOK,
			fmt.Sprintf("Deleted row with id %d in table %s", pkg.NumToInt(row[builder.SYS_PRIMARY_KEY]), table.Name),
			row,
		)
	}
}

func DeleteManyReqHandler(schema *builder.Schema, raw []byte) Response {
	var req DeleteRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if table, ok := schema.Tables[req.Table]; !ok {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	} else {
		rows, err := query.Find(table, req.Where, false)
		if err != nil {
			return NewErrorResponse(http.StatusBadRequest, err.Error())
		}

		for _, row := range rows {
			query.Delete(table, row)
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

func UpdateReqHandler(schema *builder.Schema, raw []byte) Response {
	var req UpdateRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	table, ok := schema.Tables[req.Table]
	if !ok {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	}

	row, err := query.FindUnique(table, req.Where)
	if err != nil {
		if query_error, ok := err.(*query.QueryError); ok {
			return NewErrorResponse(query_error.Status(), query_error.Error())
		}
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	} else if row == nil {
		return NewErrorResponse(
			http.StatusNotFound,
			fmt.Sprintf("No row found with constraint %v in table %s", req.Where, table.Name),
		)
	}

	res, err := query.Update(table, row, req.Data)
	if err != nil {
		if query_error, ok := err.(*query.QueryError); ok {
			return NewErrorResponse(query_error.Status(), query_error.Error())
		}
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	schema.Data[table.Name].Rows[pkg.NumToInt(row[builder.SYS_PRIMARY_KEY])] = nil

	res = pkg.MergeMaps(row, res)

	schema.Data[table.Name].Rows[pkg.NumToInt(res[builder.SYS_PRIMARY_KEY])] = res

	return NewResponse(
		http.StatusOK,
		fmt.Sprintf("Updated row with id %d in table %s", pkg.NumToInt(row[builder.SYS_PRIMARY_KEY]), table.Name),
		res,
	)
}

func UpdateManyReqHandler(schema *builder.Schema, raw []byte) Response {
	var req UpdateRequest
	err := json.Unmarshal(raw, &req)
	if err != nil {
		return NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	if table, ok := schema.Tables[req.Table]; !ok {
		return NewErrorResponse(http.StatusNotFound, "Table not found")
	} else {
		rows, err := query.Find(table, req.Where, false)
		if err != nil {
			return NewErrorResponse(http.StatusBadRequest, err.Error())
		}

		for i := 0; i < len(rows); i++ {
			row := rows[i]
			res, err := query.Update(table, row, req.Data)
			if err != nil {
				return NewErrorResponse(http.StatusBadRequest, err.Error())
			}

			schema.Data[table.Name].Rows[pkg.NumToInt(row[builder.SYS_PRIMARY_KEY])] = nil

			res = pkg.MergeMaps(row, res)

			schema.Data[table.Name].Rows[pkg.NumToInt(res[builder.SYS_PRIMARY_KEY])] = res
			rows[i] = res
		}

		return NewResponse(
			http.StatusOK,
			fmt.Sprintf("Updated %d rows in table %s", len(rows), table.Name),
			rows,
		)
	}
}
