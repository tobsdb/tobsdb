package builder

import (
	"sync"

	"github.com/google/uuid"
	"github.com/tobsdb/tobsdb/pkg"
	sorted "github.com/tobshub/go-sortedmap"
)

// Maps row field name to its saved data
type TDBTableRow = pkg.Map[string, any]

func GetPrimaryKey(r TDBTableRow) int {
	return pkg.NumToInt(r.Get(SYS_PRIMARY_KEY))
}

func SetPrimaryKey(r TDBTableRow, key int) {
	r.Set(SYS_PRIMARY_KEY, key)
}

type TDBTablePageRefs = pkg.Map[int, string]

// Maps row id to its saved data
type TDBTableRows struct {
	locker sync.RWMutex
	PM     *PagingManager

	Map     *sorted.SortedMap[int, TDBTableRow]
	Indexes TDBTableIndexes
	// primary key -> page id
	PageRefs         TDBTablePageRefs
	DeletedPageRefs TDBTablePageRefs
}

func tdbTableRowsComparisonFunc(a, b TDBTableRow) bool {
	return GetPrimaryKey(a) < GetPrimaryKey(b)
}

func NewTDBTableRows(t *Table, indexes TDBTableIndexes, primary_indexes TDBTablePageRefs) *TDBTableRows {
	pm := NewPagingManager(t)
	m, err := pm.ParsePage()
	if err != nil {
		pkg.FatalLog("failed to parse first page.", err)
	}
	return &TDBTableRows{sync.RWMutex{}, pm, m, indexes, primary_indexes, TDBTablePageRefs{}}
}

func (r *TDBTableRows) GetLocker() *sync.RWMutex { return &r.locker }

func (r *TDBTableRows) Get(id int) (TDBTableRow, bool) {
	r.locker.RLock()
	defer r.locker.RUnlock()

	if !r.PageRefs.Has(id) {
		return nil, false
	}

	page_id := r.PageRefs.Get(id)
	err := r.PM.LoadPage(page_id)
	if err != nil {
		pkg.FatalLog("failed to load page.", err)
	}

	if !r.PM.has_parsed {
		r.Map, err = r.PM.ParsePage()
		if err != nil {
			pkg.FatalLog("failed to parse page.", err)
		}
	}

	return r.Map.Get(id)
}

func (r *TDBTableRows) Insert(key int, value TDBTableRow) bool {
	r.locker.Lock()
	defer r.locker.Unlock()
	if r.PageRefs.Has(key) {
		return false
	}
	if err := r.PM.Insert(key, value); err != nil {
		pkg.ErrorLog(err)
		return false
	}
	r.PageRefs.Set(key, r.PM.p.Id.String())
	return true
}

func (r *TDBTableRows) Replace(key int, value TDBTableRow) bool {
	r.locker.Lock()
	defer r.locker.Unlock()
	err := r.PM.Insert(key, value)
	if err != nil {
		pkg.ErrorLog(err)
		return false
	}
	r.PageRefs.Set(key, r.PM.p.Id.String())
	return true
}

func (r *TDBTableRows) Delete(key int) bool {
	r.locker.Lock()
	defer r.locker.Unlock()
	if r.PageRefs.Has(key) {
		return false
	}
	r.PageRefs.Delete(key)
	return true
}

func (r *TDBTableRows) Has(key int) bool {
	r.locker.RLock()
	defer r.locker.RUnlock()
	return r.PageRefs.Has(key)
}

func (r *TDBTableRows) Len() int {
	r.locker.RLock()
	defer r.locker.RUnlock()
	return len(r.PageRefs)
}

func (r *TDBTableRows) Records() <-chan sorted.Record[int, TDBTableRow] {
	rchan := make(chan sorted.Record[int, TDBTableRow], 1)
	go func() {
		err := r.PM.LoadPage(r.PM.first_page)
		if err != nil {
			pkg.ErrorLog(err)
			return
		}
		for {
			if !r.PM.has_parsed {
				r.Map, err = r.PM.ParsePage()
				if err != nil {
					pkg.ErrorLog(err)
					return
				}
			}

			icc, err := r.Map.IterCh()
			if err != nil {
				close(rchan)
				return
			}
			for rec := range icc.Records() {
				rchan <- rec
			}

			if r.PM.p.Next == uuid.Nil {
				close(rchan)
				return
			}

			err = r.PM.LoadPage(r.PM.p.Next.String())
			if err != nil {
				pkg.ErrorLog(err)
				close(rchan)
				return
			}
		}
	}()
	return rchan
}

// TODO(Tobani): explore if paging manager needs to be updated in any way
func (r *TDBTableRows) ApplySnapshot(snapshot *TDBTableRows) {
	// TODO(Tobani): handle cases where replace needs to be called instead of insert
	r.Map.BatchInsertMap(snapshot.Map.Idx)
	r.Indexes = pkg.MergeMaps(r.Indexes, snapshot.Indexes)
	r.PageRefs = pkg.MergeMaps(r.PageRefs, snapshot.PageRefs)
}
