package lsm

import (
	"io"

	bloom "github.com/bits-and-blooms/bloom/v3"
)

type ssTable struct {
	bf *bloom.BloomFilter
}

type indexEntry struct {
	key    []byte
	offset int64
}

type ssTableWriter struct {
	w      io.Writer
	bf     *bloom.BloomFilter
	index  []indexEntry
	offset int64
}

func newSSTableWriter(w io.Writer, expectedKeys uint) *ssTableWriter {
	return &ssTableWriter{
		w:  w,
		bf: bloom.NewWithEstimates(expectedKeys, 0.01),
	}
}
