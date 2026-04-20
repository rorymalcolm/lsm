package lsm_test

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/rorymalcolm/lsm"
)

func makeKey(i uint64) []byte {
	k := make([]byte, 16)
	binary.BigEndian.PutUint64(k, i)
	return k
}

func makeValue(rng *rand.Rand, size int) []byte {
	v := make([]byte, size)
	rng.Read(v)
	return v
}

func BenchmarkInsertSequential(b *testing.B) {
	tree := lsm.New()
	value := make([]byte, 128)
	keyBuf := make([]byte, 16*b.N)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k := keyBuf[i*16 : (i+1)*16]
		binary.BigEndian.PutUint64(k, uint64(i))
		tree.Insert(k, value)
	}
}
func BenchmarkInsertParallel(b *testing.B) {
	tree := lsm.New()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		value := makeValue(rng, 128)

		var counter uint64
		goroutineID := rng.Uint64() << 32

		for pb.Next() {
			tree.Insert(makeKey(goroutineID|counter), value)
			counter++
		}
	})
}
