package builder

import (
	"sync"

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

type TDBTablePrimaryIndexes = pkg.Map[int, string]

// Maps row id to its saved data
type TDBTableRows struct {
	locker sync.RWMutex
	PM     *PagingManager

	Map     *sorted.SortedMap[int, TDBTableRow]
	Indexes TDBTableIndexes
	// primary key -> page id
	PrimaryIndexes TDBTablePrimaryIndexes
}

func tdbTableRowsComparisonFunc(a, b TDBTableRow) bool {
	return GetPrimaryKey(a) < GetPrimaryKey(b)
}

func NewTDBTableRows(t *Table, indexes TDBTableIndexes, primary_indexes TDBTablePrimaryIndexes) *TDBTableRows {
	pm := NewPagingManager(t)
	m, err := pm.ParsePage()
	if err != nil {
		pkg.FatalLog("failed to parse first page.", err)
	}
	return &TDBTableRows{sync.RWMutex{}, pm, m, indexes, primary_indexes}
}

func (r *TDBTableRows) GetLocker() *sync.RWMutex { return &r.locker }

func (r *TDBTableRows) Get(id int) (TDBTableRow, bool) {
	r.locker.RLock()
	defer r.locker.RUnlock()

	if !r.PrimaryIndexes.Has(id) {
		return nil, false
	}

	page_id := r.PrimaryIndexes.Get(id)
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
	if r.PrimaryIndexes.Has(key) {
		return false
	}
	if err := r.PM.Insert(key, value); err != nil {
		pkg.ErrorLog(err)
		return false
	}
	r.PrimaryIndexes.Set(key, r.PM.p.Id.String())
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
	r.PrimaryIndexes.Set(key, r.PM.p.Id.String())
	return true
}

func (r *TDBTableRows) Delete(key int) bool {
	r.locker.Lock()
	defer r.locker.Unlock()
	if !r.PrimaryIndexes.Has(key) {
		return false
	}
	r.PrimaryIndexes.Delete(key)
	return true
}

func (r *TDBTableRows) Has(key int) bool {
	r.locker.RLock()
	defer r.locker.RUnlock()
	return r.PrimaryIndexes.Has(key)
}

func (r *TDBTableRows) Len() int {
	r.locker.RLock()
	defer r.locker.RUnlock()
	return len(r.PrimaryIndexes)
}
