package builder

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"os"
	"path"
	"sync/atomic"

	"github.com/tobsdb/tobsdb/pkg"
)

type Table struct {
	Name    string
	Fields  *pkg.InsertSortMap[string, *Field]
	Indexes []string

	IdTracker atomic.Int64 `json:"-"`

	Schema *Schema `json:"-"`

	first_page_id string
}

func (t *Table) MarshalJSON() ([]byte, error) {
	type T Table
	return json.Marshal(struct {
		*T
		IdTracker int64
	}{(*T)(t), t.IdTracker.Load()})
}

func (t *Table) UnmarshalJSON(data []byte) error {
	type T Table
	buf := struct {
		*T
		IdTracker int64
	}{T: (*T)(t)}
	if err := json.Unmarshal(data, &buf); err != nil {
		return err
	}
	t.IdTracker.Store(buf.IdTracker)
	return nil
}

func (t *Table) PrimaryKey() *Field {
	for _, field := range t.Fields.Idx {
		if field.IndexLevel() == IndexLevelPrimary {
			return field
		}
	}
	return nil
}

func (t *Table) Data() *TDBTableData {
	return t.Schema.Data.Get(t.Name)
}

func (t *Table) Rows() *TDBTableRows {
	return t.Data().Rows
}

func (t *Table) Row(id int) TDBTableRow {
	v, ok := t.Data().Rows.Get(id)
	if !ok {
		return nil
	}
	return v
}

func (t *Table) IndexMap(index string) *TDBTableIndexMap {
	return t.Data().Indexes.Get(index)
}

func (t *Table) DataBytes() (*bytes.Buffer, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(t.Data()); err != nil {
		return nil, err
	}
	return &buf, nil
}

func (t *Table) Base() string {
	return path.Join(t.Schema.Base(), t.Name)
}

// {base} is the directory where the schema is stored
func (t *Table) WriteToFile() error {
	buf, err := t.DataBytes()
	if err != nil {
		return err
	}

	base := t.Base()
	if _, err := os.Stat(base); os.IsNotExist(err) {
		if err := os.Mkdir(base, 0755); err != nil {
			return err
		}
	}

	// TODO: write rows and indexes in separate files
	// This will allow to read *all* indexes while making
	// partial reads of rows
	if err := os.WriteFile(path.Join(base, "data.tdb"), buf.Bytes(), 0644); err != nil {
		return err
	}

	return nil
}

func BuildTableDataFromPath(base, name string) (*TDBTableData, error) {
	file := path.Join(base, name, "data.tdb")
	buf, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	data := TDBTableData{}
	err = gob.NewDecoder(bytes.NewReader(buf)).Decode(&data)
	if err != nil {
		return nil, err
	}

	data.Rows.Map.SetComparisonFunc(func(a, b TDBTableRow) bool {
		return GetPrimaryKey(a) < GetPrimaryKey(b)
	})

	return &data, nil
}
