package builder

import (
	"fmt"
	"sync"

	"github.com/tobsdb/tobsdb/pkg"
)

type (
	TDBTableIndexMap struct {
		locker sync.RWMutex
		Map    map[string]int
	}
	// index field name -> index value -> row id
	TDBTableIndexes = pkg.Map[string, *TDBTableIndexMap]
)

func formatIndexValue(v any) string {
	return fmt.Sprintf("%v", v)
}

func (m *TDBTableIndexMap) Has(key any) bool {
	m.locker.RLock()
	defer m.locker.RUnlock()
	_, ok := m.Map[formatIndexValue(key)]
	return ok
}

func (m *TDBTableIndexMap) Get(key any) int {
	m.locker.RLock()
	defer m.locker.RUnlock()
	val, ok := m.Map[formatIndexValue(key)]
	if !ok {
		return 0
	}
	return val
}

func (m *TDBTableIndexMap) Set(key any, value int) {
	m.locker.Lock()
	defer m.locker.Unlock()
	m.Map[formatIndexValue(key)] = value
}

func (m *TDBTableIndexMap) Delete(key any) {
	m.locker.Lock()
	defer m.locker.Unlock()
	delete(m.Map, formatIndexValue(key))
}
