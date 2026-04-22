package lsm

import (
	"sync"

	"github.com/huandu/skiplist"
)

type entry struct {
	value     []byte
	tombstone bool
}

type memtable struct {
	mu   sync.RWMutex
	list *skiplist.SkipList
	size int
}

func newMemtable() *memtable {
	return &memtable{list: skiplist.New(skiplist.BytesAsc)}
}

func (m *memtable) set(k []byte, e *entry) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.list.Set(k, e)
	m.size += len(k) + len(e.value)
	return m.size
}

func (m *memtable) get(k []byte) (*entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.list.GetValue(k)
	if !ok {
		return nil, false
	}
	return v.(*entry), true
}
