package builder

import (
	"fmt"
	"sync"

	"github.com/tobsdb/tobsdb/pkg"
)

type (
	TDBTableIndexMap struct {
		Locker sync.RWMutex
		Map    map[string]int
	}
	// index field name -> index value -> row id
	TDBTableIndexes = pkg.Map[string, *TDBTableIndexMap]
	TDBTableData    struct {
		Rows    *TDBTableRows
		Indexes TDBTableIndexes
	}
	// Maps table name to its saved data
	TDBData = pkg.Map[string, *TDBTableData]
)

func formatIndexValue(v any) string {
	return fmt.Sprintf("%v", v)
}

func (m *TDBTableIndexMap) Has(key any) bool {
	m.Locker.RLock()
	defer m.Locker.RUnlock()
	_, ok := m.Map[formatIndexValue(key)]
	return ok
}

func (m *TDBTableIndexMap) Get(key any) int {
	m.Locker.RLock()
	defer m.Locker.RUnlock()
	val, ok := m.Map[formatIndexValue(key)]
	if !ok {
		return 0
	}
	return val
}

func (m *TDBTableIndexMap) Set(key any, value int) {
	m.Locker.Lock()
	defer m.Locker.Unlock()
	m.Map[formatIndexValue(key)] = value
}

func (m *TDBTableIndexMap) Delete(key any) {
	m.Locker.Lock()
	defer m.Locker.Unlock()
	delete(m.Map, formatIndexValue(key))
}
