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

	parent *Table
}

func (t *Table) NewSnapshot() *Table {
    snapshot := &Table{
		Name:    t.Name,
		Fields:  pkg.NewInsertSortMap[string, *Field](),
		Indexes: make([]string, len(t.Indexes)),
        parent: t,
	}
	for _, f := range t.Fields.Idx {
		snapshot.Fields.Push(f.Name, f)
	}
	copy(snapshot.Indexes, t.Indexes)
	snapshot.IdTracker.Store(t.IdTracker.Load())
    return snapshot
}

func (t *Table) ApplySnapshot(snapshot *Table) {
    t.IdTracker.Store(snapshot.IdTracker.Load())
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

func (t *Table) Rows() *TDBTableRows {
	return t.Schema.Data.Get(t.Name)
}

func (t *Table) Row(id int) TDBTableRow {
	v, ok := t.Rows().Get(id)
	if !ok {
		return nil
	}
	return v
}

func (t *Table) IndexMap(index string) *TDBTableIndexMap {
	return t.Rows().Indexes.Get(index)
}

type TableIndexBytes struct {
	IndexBuf        *bytes.Buffer
	PrimaryIndexBuf *bytes.Buffer
}

func (t *Table) IndexBytes() (*TableIndexBytes, error) {
	var index_buf bytes.Buffer
	if err := gob.NewEncoder(&index_buf).Encode(t.Rows().Indexes); err != nil {
		return nil, err
	}
	var p_index_buf bytes.Buffer
	if err := gob.NewEncoder(&p_index_buf).Encode(t.Rows().PageRefs); err != nil {
		return nil, err
	}
	return &TableIndexBytes{&index_buf, &p_index_buf}, nil
}

func (t *Table) Base() string {
	if t.Schema == nil {
		return t.Name
	}
	base := path.Join(t.Schema.Base(), t.Name)
	if _, err := os.Stat(base); os.IsNotExist(err) {
		os.Mkdir(base, 0o755)
	}
	return base
}

const (
	INDEX_FILE         = "index.tdb"
	PRIMARY_INDEX_FILE = "primary_index.tdb"
)

func (t *Table) WriteToFile() error {
	indexes_bufs, err := t.IndexBytes()
	if err != nil {
		return err
	}

	base := t.Base()
	if _, err := os.Stat(base); os.IsNotExist(err) {
		if err := os.Mkdir(base, 0o755); err != nil {
			return err
		}
	}

	err = os.WriteFile(path.Join(base, INDEX_FILE), indexes_bufs.IndexBuf.Bytes(), 0o644)
	if err != nil {
		return err
	}

	err = os.WriteFile(path.Join(base, PRIMARY_INDEX_FILE), indexes_bufs.PrimaryIndexBuf.Bytes(), 0o644)
	if err != nil {
		return err
	}

	err = t.Rows().PM.p.WriteToFile(base, t.Schema.InMem())
	if err != nil {
		return err
	}

	return nil
}

type TdbIndexesBuilder struct {
	Indexes        TDBTableIndexes
	PrimaryIndexes TDBTablePageRefs
}

func BuildTableIndexesFromPath(base, name string) (*TdbIndexesBuilder, error) {
	index_file := path.Join(base, name, INDEX_FILE)
	index_buf, err := os.ReadFile(index_file)
	if err != nil {
		return nil, err
	}

	primary_index_file := path.Join(base, name, PRIMARY_INDEX_FILE)
	primary_index_buf, err := os.ReadFile(primary_index_file)
	if err != nil {
		return nil, err
	}

	indexes := TdbIndexesBuilder{}
	err = gob.NewDecoder(bytes.NewReader(index_buf)).Decode(&indexes.Indexes)
	if err != nil {
		return nil, err
	}
	err = gob.NewDecoder(bytes.NewReader(primary_index_buf)).Decode(&indexes.PrimaryIndexes)
	if err != nil {
		return nil, err
	}

	return &indexes, nil
}
