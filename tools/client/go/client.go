// Golang client for Tobsdb.
//
// Usage:
//
// generate model types from schema
//
//	```sh
//	tdb-generate -lang golang -path ./schema.tdb -out ./tdb/schema/types.go
//	```
//
// create a new Tobsdb client
//
//		```go
//	 func main() {
//		  tdb, err := client.NewTdbClient("ws://localhost:7085", "example", client.TdbClientOptions{})
//	 }
//		```
//
// make requests with the generated types
//
//		```go
//	 import schema "package/tdb/schema"
//
//	 func main() {
//	  ...
//	  res, err := tdb.Create("example", schema.Example{ Field: "value" })
//	 }
//		```
//
// handle the response
//
//	```go
//	func main() {
//	 ...
//	 res.Data.(schema.Example)
//	}
//	```
package main

import (
	"fmt"
	"net/url"
	"os"
	"path"

	ws "github.com/gorilla/websocket"
)

type (
	TdbClientOptions struct {
		Username   string
		Password   string
		SchemaPath string
	}

	// Tobsdb client
	//
	// Unless you know what you're doing, you probably want to use
	// the `NewTdbClient` function instead.
	TdbClient struct {
		// The websocket connection used by the client
		conn *ws.Conn
		// The formatted connection url of the Tobsdb server
		Url     *url.URL
		options TdbClientOptions
	}
)

func (options *TdbClientOptions) readSchema() ([]byte, error) {
	if options.SchemaPath == "" {
		options.SchemaPath = "./schema.tdb"
	}

	if !path.IsAbs(options.SchemaPath) {
		cwd, _ := os.Getwd()
		options.SchemaPath = path.Join(cwd, options.SchemaPath)
	}

	schema, err := os.ReadFile(options.SchemaPath)
	return schema, err
}

func NewTdbClient(urlStr string, dbName string, options TdbClientOptions) (*TdbClient, error) {
	Url, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	q := Url.Query()
	q.Add("db", dbName)
	q.Add("username", options.Username)
	q.Add("password", options.Password)

	schema, err := options.readSchema()
	if err != nil {
		return nil, err
	}
	q.Add("schema", string(schema))

	Url.RawQuery = q.Encode()

	return &TdbClient{Url: Url, options: options}, nil
}

func (c *TdbClient) Connect() error {
	if c.conn != nil {
		return nil
	}
	conn, res, err := ws.DefaultDialer.Dial(c.Url.String(), nil)
	if err != nil {
		return err
	}
	if err := res.Header.Get("tdb-error"); err != "" {
		return fmt.Errorf("TDB Error: %s", err)
	}

	Log(LogLevelInfo, "Connected to TDB Server")
	c.conn = conn
	return nil
}

func (c *TdbClient) Disconnect() error {
	err := c.conn.WriteMessage(ws.CloseMessage,
		ws.FormatCloseMessage(ws.CloseNormalClosure, "Disconnect"))
	if err != nil {
		Log(LogLevelError, err.Error())
		return err
	}
	err = c.conn.Close()
	if err != nil {
		Log(LogLevelError, err.Error())
		return err
	}

	Log(LogLevelInfo, "Disconnected from TDB Server")
	return nil
}

type queryAction string

const (
	queryActionCreate       = "create"
	queryActionCreateMany   = "createMany"
	queryActionFindUnique   = "findUnique"
	queryActionFindMany     = "findMany"
	queryActionUpdateUnique = "updateUnique"
	queryActionUpdateMany   = "updateMany"
	queryActionDeleteUnique = "deleteUnique"
	queryActionDeleteMany   = "deleteMany"
)

type TdbResponse struct {
	Status    int32  `json:"status"`
	Message   string `json:"message"`
	Data      any    `json:"data"`
	RequestId int64  `json:"__tdb_client_req_id__"`
}

func (c *TdbClient) query(
	action queryAction, table string,
	data any, where map[string]any,
) (TdbResponse, error) {
	c.Connect()
	if c.conn == nil {
		return TdbResponse{}, fmt.Errorf("Not connected")
	}
	err := c.conn.WriteJSON(map[string]any{
		"action": action,
		"table":  table,
		"data":   data,
		"where":  where,
	})
	if err != nil {
		return TdbResponse{}, err
	}

	var res TdbResponse
	err = c.conn.ReadJSON(&res)
	if err != nil {
		return res, err
	}

	return res, nil
}

type (
	TdbRequestData  any
	TdbRequestWhere map[string]any
)

func (c *TdbClient) Create(table string, data TdbRequestData) (TdbResponse, error) {
	return c.query(queryActionCreate, table, data, nil)
}

func (c *TdbClient) CreateMany(table string, data []TdbRequestData) (TdbResponse, error) {
	return c.query(queryActionCreateMany, table, data, nil)
}

func (c *TdbClient) FindUnique(table string, where TdbRequestWhere) (TdbResponse, error) {
	return c.query(queryActionFindUnique, table, nil, where)
}

func (c *TdbClient) FindMany(table string, where TdbRequestWhere) (TdbResponse, error) {
	return c.query(queryActionFindMany, table, nil, where)
}

func (c *TdbClient) UpdateUnique(table string, where TdbRequestWhere, data TdbRequestData) (TdbResponse, error) {
	return c.query(queryActionUpdateUnique, table, data, where)
}

func (c *TdbClient) UpdateMany(table string, where TdbRequestWhere, data TdbRequestData) (TdbResponse, error) {
	return c.query(queryActionUpdateMany, table, data, where)
}

func (c *TdbClient) DeleteUnique(table string, where TdbRequestWhere) (TdbResponse, error) {
	return c.query(queryActionDeleteUnique, table, nil, where)
}

func (c *TdbClient) DeleteMany(table string, where TdbRequestWhere) (TdbResponse, error) {
	return c.query(queryActionDeleteMany, table, nil, where)
}
