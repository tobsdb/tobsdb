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

// Maps row id to its saved data
type TDBTableRows struct {
	locker sync.RWMutex
	PM     *PagingManager

	Map            *sorted.SortedMap[int, TDBTableRow]
	Indexes        TDBTableIndexes
	PrimaryIndexes pkg.Map[int, bool]
}

func tdbTableRowsComparisonFunc(a, b TDBTableRow) bool {
	return GetPrimaryKey(a) < GetPrimaryKey(b)
}

// TODO(Tobshub): store and load `PrimaryIndexes` from disk
func NewTDBTableRows(t *Table, indexes TDBTableIndexes) *TDBTableRows {
	pm := NewPagingManager(t)
	m, err := pm.ParsePage()
	if err != nil {
		pkg.FatalLog("failed to parse first page.", err)
	}
	return &TDBTableRows{sync.RWMutex{}, pm, m, indexes, pkg.Map[int, bool]{}}
}

func (t *TDBTableRows) GetLocker() *sync.RWMutex { return &t.locker }

func (t *TDBTableRows) Get(id int) (TDBTableRow, bool) {
	t.locker.RLock()
	defer t.locker.RUnlock()

	for {
		if !t.PM.hasParsed {
			t.Map, _ = t.PM.ParsePage()
		}
		if t.Map.Has(id) {
			return t.Map.Get(id)
		}
		if t.PM.p.Next == uuid.Nil {
			return nil, false
		}
		err := t.PM.LoadPage(t.PM.p.Next.String())
		if err != nil {
			pkg.FatalLog("failed to load next page.", err)
		}
	}
}

func (t *TDBTableRows) Insert(key int, value TDBTableRow) bool {
	t.locker.Lock()
	defer t.locker.Unlock()
	if t.PrimaryIndexes.Get(key) {
		return false
	}
	if err := t.PM.Insert(key, value); err != nil {
		pkg.ErrorLog(err)
		return false
	}
	t.PrimaryIndexes.Set(key, true)
	return true
}

func (t *TDBTableRows) Replace(key int, value TDBTableRow) {
	t.locker.Lock()
	defer t.locker.Unlock()
	t.Map.Replace(key, value)
}

func (t *TDBTableRows) Delete(key int) bool {
	t.locker.Lock()
	defer t.locker.Unlock()
	return t.Map.Delete(key)
}

func (t *TDBTableRows) Has(key int) bool {
	t.locker.RLock()
	defer t.locker.RUnlock()
	for {
		if !t.PM.hasParsed {
			t.Map, _ = t.PM.ParsePage()
		}
		if t.Map.Has(key) {
			return t.Map.Has(key)
		}
		if t.PM.p.Next == uuid.Nil {
			return false
		}
		err := t.PM.LoadPage(t.PM.p.Next.String())
		if err != nil {
			pkg.FatalLog("failed to load next page.", err)
		}
	}
}

func (t *TDBTableRows) Len() int {
	t.locker.RLock()
	defer t.locker.RUnlock()
	return t.Map.Len()
}
