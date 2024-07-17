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

	has_parsed       bool
	first_page       string
	last_loaded_page string
}

// TODO(tobshub): we don't need to load the first page immediately
func NewPagingManager(t *Table) *PagingManager {
	pm := &PagingManager{base: t.Base()}
	if t.first_page_id == "" {
		pm.p = paging.NewPage(uuid.Nil, uuid.Nil)
		page_id := pm.p.Id.String()
		pm.last_loaded_page = page_id
		pm.first_page = page_id
		t.first_page_id = page_id
	} else {
		p, err := paging.LoadPage(pm.base, t.first_page_id)
		if err != nil {
			pkg.FatalLog("NewPagingManager", err)
		}
		pm.first_page = t.first_page_id
		pm.p = p
	}
	return pm
}

// TODO(tobshub):
// instead of returning a new map each time, could simply insert into existing map.
// this would allow keeping previous values
// & reduce the number of times a page has to be parsed.
// it would also enable manually evicting stale records in the map
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

		if !m.Insert(key, value) {
			m.Replace(key, value)
		}
	}
	pm.has_parsed = true
	return m, nil
}

func (pm *PagingManager) LoadPage(id string) error {
	if pm.last_loaded_page == id {
		return nil
	}

	err := pm.p.WriteToFile(pm.base)
	if err != nil {
		pkg.ErrorLog("failed to write page", err)
	}

	p, err := paging.LoadPage(pm.base, id)
	if err != nil {
		return err
	}
	pm.last_loaded_page = id
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
	err = pm.LoadPage(pm.p.Next.String())
	if err != nil {
		return err
	}
	return pm.InsertBytes(d)
}
