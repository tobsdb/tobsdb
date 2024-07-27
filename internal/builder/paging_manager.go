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
	t *Table

	p *paging.Page

	has_parsed       bool
	first_page       string
	last_loaded_page string
}

func NewPagingManager(t *Table) *PagingManager {
	pm := &PagingManager{t: t}
	if t.first_page_id == "" {
		pm.p = paging.NewPage(uuid.Nil, uuid.Nil)
		t.first_page_id = pm.p.Id.String()
	} else {
		p := paging.NewPageWithId(uuid.MustParse(t.first_page_id), uuid.Nil, uuid.Nil)
		pm.p = p
	}
	pm.last_loaded_page = t.first_page_id
	pm.first_page = t.first_page_id
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

	err := pm.p.WriteToFile(pm.t.Base(), pm.t.Schema.InMem())
	if err != nil {
		pkg.ErrorLog("failed to write page", err)
	}

	p, err := paging.LoadPage(pm.t.Base(), id)
	if err != nil {
		return err
	}
	pm.last_loaded_page = id
	pm.has_parsed = false
	pm.p = p
	return nil
}

func (pm *PagingManager) LoadNextPage() error {
	if pm.p.Next == uuid.Nil {
		pm.p.Next = uuid.New()
	}
	curr_page_id := pm.p.Id
	err := pm.LoadPage(pm.p.Next.String())
	if err != nil {
		return err
	}
	pm.p.Prev = curr_page_id
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
	err := pm.p.Push(d, pm.t.Schema.InMem())
	if err == nil || err != paging.ERR_PAGE_OVERFLOW {
		return err
	}

	// on ERR_PAGE_OVERFLOW attempt to insert in next page
	err = pm.LoadNextPage()
	if err != nil {
		return err
	}
	return pm.InsertBytes(d)
}
