package lsm

import (
	"iter"

	"github.com/huandu/skiplist"
)

type immutableMemtable struct {
	list *skiplist.SkipList
	size int
}

func (m *memtable) freeze() *immutableMemtable {
	m.mu.Lock()
	defer m.mu.Unlock()
	im := &immutableMemtable{list: m.list, size: m.size}
	m.list = nil // poison: any further use panics cleanly
	return im
}

func (im *immutableMemtable) get(k []byte) (entry, bool) {
	v, ok := im.list.GetValue(k)
	if !ok {
		return entry{}, false
	}
	return v.(entry), true
}

func (im *immutableMemtable) iter() *skiplist.Element {
	return im.list.Front()
}

func (m *immutableMemtable) Len() int {
	return m.list.Len()
}

func (m *immutableMemtable) All() iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		for e := m.list.Front(); e != nil; e = e.Next() {
			if !yield(e.Key().([]byte), e.Value.([]byte)) {
				return
			}
		}
	}
}
