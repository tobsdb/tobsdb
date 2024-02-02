package builder

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"sync/atomic"

	"github.com/tobsdb/tobsdb/pkg"
)

type Table struct {
	Name    string
	Fields  *pkg.InsertSortMap[string, *Field]
	Indexes []string

	IdTracker atomic.Int64 `json:"-"`

	Schema *Schema `json:"-"`
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
