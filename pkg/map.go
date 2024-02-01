package pkg

type Map[K comparable, V any] map[K]V

func (m Map[K, V]) Get(key K) V {
	return m[key]
}

func (m Map[K, V]) Set(key K, value V) {
	m[key] = value
}

func (m Map[K, V]) Has(key K) bool {
	_, ok := m[key]
	return ok
}

func (m Map[K, V]) Delete(key K) {
	delete(m, key)
}

func (m Map[K, V]) Keys() []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

type InsertSortMap[K comparable, V any] struct {
	Idx    Map[K, V]
	Sorted []K
}

func NewInsertSortMap[K comparable, V any]() *InsertSortMap[K, V] {
	return &InsertSortMap[K, V]{Idx: Map[K, V]{}, Sorted: []K{}}
}

func (m *InsertSortMap[K, V]) Len() int { return len(m.Sorted) }

func (m *InsertSortMap[K, V]) Get(key K) V { return m.Idx.Get(key) }

func (m *InsertSortMap[K, V]) Has(key K) bool { return m.Idx.Has(key) }

func (m *InsertSortMap[K, V]) Push(key K, value V) {
	m.Idx.Set(key, value)
	m.Sorted = append(m.Sorted, key)
}

func (m *InsertSortMap[K, V]) Delete(key K) {
	m.Idx.Delete(key)
	for i, k := range m.Sorted {
		if k == key {
			m.Sorted = append(m.Sorted[:i], m.Sorted[i+1:]...)
			break
		}
	}
}
