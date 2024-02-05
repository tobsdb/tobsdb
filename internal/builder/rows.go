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

// Maps row id to its saved data
type TDBTableRows struct {
	locker sync.RWMutex
	Map    *sorted.SortedMap[int, TDBTableRow]
}

func NewTDBTableRows() *TDBTableRows {
	return &TDBTableRows{
		locker: sync.RWMutex{},
		Map: sorted.New[int, TDBTableRow](0, func(a, b TDBTableRow) bool {
			return GetPrimaryKey(a) < GetPrimaryKey(b)
		}),
	}
}

func (t *TDBTableRows) GetLocker() *sync.RWMutex { return &t.locker }

func (t *TDBTableRows) Get(id int) (TDBTableRow, bool) {
	t.locker.RLock()
	defer t.locker.RUnlock()
	return t.Map.Get(id)
}

func (t *TDBTableRows) Insert(key int, value TDBTableRow) bool {
	t.locker.Lock()
	defer t.locker.Unlock()
	return t.Map.Insert(key, value)
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
	return t.Map.Has(key)
}

func (t *TDBTableRows) Len() int {
	t.locker.RLock()
	defer t.locker.RUnlock()
	return t.Map.Len()
}
