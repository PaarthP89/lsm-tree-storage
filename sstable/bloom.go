package sstable

import (
	"math"
)

// FNV-1a 64-bit constants (same algorithm hash/fnv.New64a implements).
// Inlined by hand rather than going through the hash.Hash64 interface:
// fnv.New64a() heap-allocates a hasher on every call, and profiling this
// package's benchmarks showed that allocation alone made the "with
// filter" miss path slower than the plain sparse-index scan it's meant to
// skip, for small page-cache-resident tables. A bare loop over the key
// bytes computes the identical hash with zero allocations.
const (
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
)

// defaultBloomFPR is the target false-positive rate used when a filter is
// built during flush/compaction. 1% is the conventional default for LSM
// bloom filters (LevelDB/RocksDB use similar defaults) -- tight enough to
// meaningfully cut disk reads on a miss-heavy workload without the bit
// array growing large enough to matter at this project's scale.
const defaultBloomFPR = 0.01

// bloomFilter is a standard Bloom filter: a bit array plus a hash-function
// count k, sized for a target entry count n and false-positive rate fpr.
// Zero false negatives is the entire point -- MayContain must never
// return false for a key that was Add-ed, only "maybe present" (true) or
// "definitely absent" (false).
type bloomFilter struct {
	bits []byte
	k    int
}

// newBloomFilter sizes a filter for n entries at the given target false
// positive rate, using the standard formulas:
//
//	m = ceil(-n * ln(fpr) / ln(2)^2)   (bit array size)
//	k = round((m / n) * ln(2))          (hash function count)
//
// n <= 0 returns nil -- callers must treat a nil *bloomFilter as "no
// filter, always fall through to a full lookup" (see the empty-SSTable
// case in FlushIterator and the pre-8a-file case in decodeBloom).
func newBloomFilter(n int, fpr float64) *bloomFilter {
	if n <= 0 {
		return nil
	}
	m := int(math.Ceil(-1 * float64(n) * math.Log(fpr) / (math.Ln2 * math.Ln2)))
	if m < 8 {
		m = 8
	}
	k := int(math.Round((float64(m) / float64(n)) * math.Ln2))
	if k < 1 {
		k = 1
	}
	return &bloomFilter{bits: make([]byte, (m+7)/8), k: k}
}

// bloomHashes computes a single FNV-1a 64-bit hash of key and splits it
// into two 32-bit halves h1/h2, which seed every probe position via the
// standard Kirsch-Mitzenmacher double-hashing trick: probing k independent
// positions with h1 + i*h2 approximates k independent hash functions
// without actually computing k of them.
func bloomHashes(key []byte) (h1, h2 uint32) {
	sum := fnvOffset64
	for _, b := range key {
		sum ^= uint64(b)
		sum *= fnvPrime64
	}
	return uint32(sum >> 32), uint32(sum)
}

// bitPos returns the i'th probe position for key, modulo the total bit
// count (not byte count) of the filter.
func (b *bloomFilter) bitPos(h1, h2 uint32, i int) uint32 {
	nBits := uint32(len(b.bits)) * 8
	return (h1 + uint32(i)*h2) % nBits
}

func (b *bloomFilter) Add(key []byte) {
	h1, h2 := bloomHashes(key)
	for i := 0; i < b.k; i++ {
		pos := b.bitPos(h1, h2, i)
		b.bits[pos/8] |= 1 << (pos % 8)
	}
}

// MayContain reports whether key might be present. false is a hard
// guarantee of absence; true means "maybe" -- the caller must still
// check the real data.
func (b *bloomFilter) MayContain(key []byte) bool {
	h1, h2 := bloomHashes(key)
	for i := 0; i < b.k; i++ {
		pos := b.bitPos(h1, h2, i)
		if b.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}
