package lsm

import (
	"sync"

	"go.withmatt.com/size"
)

const MEMTABLE_LIMIT = size.Kilobyte * 12

type LSMTree struct {
	// this index is used to push a memtable from volatile to immutable
	mu        sync.RWMutex
	volatile  *memtable
	immutable []*immutableMemtable
}

func New() LSMTree {
	return LSMTree{
		volatile: newMemtable(),
	}
}

func (l *LSMTree) Insert(k []byte, v []byte) {
	l.mu.RLock()
	mt := l.volatile
	l.mu.RUnlock()
	mt.set(k, &entry{
		value: v,
	})

	if mt.size > int(MEMTABLE_LIMIT.Bytes()) {
		l.maybeRotate(mt)
	}
}

func (l *LSMTree) Get(k []byte) (*[]byte, bool) {
	l.mu.RLock()
	vol := l.volatile
	imms := l.immutable
	l.mu.RUnlock()
	val, found := vol.get(k)
	if !found {
		for i := len(imms) - 1; i >= 0; i-- {
			if e, ok := imms[i].get(k); ok {
				return &e.value, true
			}
		}
	}
	return &val.value, true
}

func (l *LSMTree) Delete(k []byte) {
	l.volatile.set(k, &entry{tombstone: true})
}

func (l *LSMTree) maybeRotate(expected *memtable) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.volatile == expected {
		// in this scenario a competing process has already rotated
		return
	}

	l.immutable = append(l.immutable, l.volatile.freeze())
	l.volatile = newMemtable()
}
