package builder

import (
	"bytes"
	"encoding/gob"

	"github.com/google/uuid"
	"github.com/tobsdb/tobsdb/internal/paging"
	"github.com/tobsdb/tobsdb/pkg"
	sorted "github.com/tobshub/go-sortedmap"
)

type PagingManager struct {
	base string

	p *paging.Page

	has_parsed      bool
	last_loaded_page string
}

func NewPagingManager(t *Table) *PagingManager {
	pm := &PagingManager{base: t.Base()}
	if t.first_page_id == "" {
		pm.p = paging.NewPage(uuid.Nil, uuid.Nil)
		page_id :=  pm.p.Id.String()
		pm.last_loaded_page = page_id
		t.first_page_id = page_id
	} else {
		p, err := paging.LoadPage(pm.base, t.first_page_id)
		if err != nil {
			pkg.FatalLog("NewPagingManager", err)
		}
		pm.p = p
	}
	return pm
}

func (pm *PagingManager) ParsePage() (*sorted.SortedMap[int, TDBTableRow], error) {
	r := pm.p.NewReader()

	m := sorted.New[int, TDBTableRow](0, tdbTableRowsComparisonFunc)
	d := make([]any, 2)
	for r.ReadNext() {
		err := gob.NewDecoder(bytes.NewReader(r.Buf)).Decode(&d)
		if err != nil {
			return nil, err
		}
		key := d[0].(int)
		value := d[1].(TDBTableRow)

		m.Insert(key, value)
	}
	pm.has_parsed = true
	return m, nil
}

func (pm *PagingManager) LoadPage(id string) error {
	if pm.last_loaded_page == id {
		return nil
	}
	p, err := paging.LoadPage(pm.base, id)
	if err != nil {
		return err
	}
	pm.has_parsed = false
	pm.p = p
	return nil
}

func (pm *PagingManager) Insert(key int, value TDBTableRow) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode([]any{key, value}); err != nil {
		return err
	}

	d := buf.Bytes()
	return pm.InsertBytes(d)
}

func (pm *PagingManager) InsertBytes(d []byte) error {
	pm.has_parsed = false
	err := pm.p.Push(d)
	if err == nil || err != paging.ERR_PAGE_OVERFLOW {
		return err
	}

	// on ERR_PAGE_OVERFLOW attempt to insert in next page
	p, err := paging.LoadPageUUID(pm.base, pm.p.Next)
	if err != nil {
		return err
	}
	pm.p = p
	return pm.InsertBytes(d)
}
