package main

import (
	"math"
	"sort"
)

// Deterministic UTXO workload generation.
//
// The workload models what a storage engine actually sees during block
// connection: a set of newly created outputs inserted at effectively random
// key positions, plus a set of existing outputs looked up and deleted, where
// the choice of which outputs get spent follows Bitcoin's long-tailed
// spend-age distribution rather than being uniform.
//
// Spending uniformly at random would understate both engine cache behaviour
// and the value of hot/cold tiering, so the age distribution below is the most
// important part of this file.

// spendAgeQuantiles pins the observed spend-age curve. Ages are in blocks.
//
// The values come from rBTC's full mainnet replay to height 935,000, which
// observed 3,257,609,051 spends. They describe Bitcoin's behaviour rather than
// any particular implementation, which is why they transfer here, but the
// resulting tiering threshold still has to be re-derived against this
// codebase's own store before it is trusted.
var spendAgeQuantiles = []struct {
	p   float64
	age float64
}{
	{p: 0.00, age: 1},
	{p: 0.50, age: 42},
	{p: 0.90, age: 8299},
	{p: 0.95, age: 33082},
	{p: 0.99, age: 122194},
	{p: 0.999, age: 323668},
	{p: 1.00, age: 700000},
}

// sampleSpendAge maps a uniform draw in [0,1) onto a spend age in blocks by
// interpolating logarithmically between the pinned quantiles. Log
// interpolation keeps the shape of the tail; linear interpolation across four
// orders of magnitude would flatten it into something the storage engine sees
// as nearly uniform.
func sampleSpendAge(u float64) uint64 {
	if u < 0 {
		u = 0
	}
	if u >= 1 {
		u = 0.999999
	}

	for i := 1; i < len(spendAgeQuantiles); i++ {
		hi := spendAgeQuantiles[i]
		if u > hi.p {
			continue
		}

		lo := spendAgeQuantiles[i-1]
		span := hi.p - lo.p
		if span <= 0 {
			return uint64(lo.age)
		}

		frac := (u - lo.p) / span
		logLo := math.Log(lo.age)
		logHi := math.Log(hi.age)
		age := math.Exp(logLo + frac*(logHi-logLo))

		return uint64(math.Max(1, age))
	}

	return uint64(spendAgeQuantiles[len(spendAgeQuantiles)-1].age)
}

// rng is a small deterministic generator. The benchmark must be reproducible
// across runs and machines, so this is fixed rather than seeded from the
// clock, and is defined here rather than pulled from math/rand so a future Go
// release cannot silently change the sequence.
type rng struct {
	state uint64
}

func newRNG(seed uint64) *rng {
	if seed == 0 {
		seed = 0x2545f4914f6cdd1d
	}

	return &rng{state: seed}
}

// next returns the next 64-bit value using xorshift64*.
func (r *rng) next() uint64 {
	r.state ^= r.state >> 12
	r.state ^= r.state << 25
	r.state ^= r.state >> 27

	return r.state * 2685821657736338717
}

// float64 returns a value in [0,1).
func (r *rng) float64() float64 {
	return float64(r.next()>>11) / float64(1<<53)
}

// intn returns a value in [0,n).
func (r *rng) intn(n int) int {
	if n <= 0 {
		return 0
	}

	return int(r.next() % uint64(n))
}

// liveSet tracks which outputs currently exist.
//
// Entries are appended in creation order and never reordered, so heights stay
// sorted and a sampled coin age can be turned into a position by binary
// search. Spent entries are tombstoned rather than removed, because
// swap-removing would destroy exactly the ordering the age sampling depends
// on. The memory cost is one bool per output, which is cheaper than losing the
// distribution.
type liveSet struct {
	keys    [][36]byte
	heights []uint32
	spent   []bool
	live    int
}

func newLiveSet(capacity int) *liveSet {
	return &liveSet{
		keys:    make([][36]byte, 0, capacity),
		heights: make([]uint32, 0, capacity),
		spent:   make([]bool, 0, capacity),
	}
}

func (l *liveSet) add(key [36]byte, height uint32) {
	l.keys = append(l.keys, key)
	l.heights = append(l.heights, height)
	l.spent = append(l.spent, false)
	l.live++
}

func (l *liveSet) total() int {
	return len(l.keys)
}

// selectSpend picks an output to spend at the given tip height by sampling the
// spend-age distribution, locating the creation height that age implies, and
// taking the nearest unspent entry. It returns the key, the age actually
// achieved, and whether anything was available.
//
// The realised age can be shorter than the sampled one when the workload's
// history is not deep enough to contain a coin that old. That shortfall is
// visible in the reported quantiles, which is the point: it tells the operator
// the run was too shallow to say anything about the tail.
func (l *liveSet) selectSpend(r *rng, tipHeight uint32) ([36]byte, uint64, bool) {
	if l.live == 0 {
		return [36]byte{}, 0, false
	}

	age := sampleSpendAge(r.float64())
	target := uint32(0)
	if uint64(tipHeight) > age {
		target = tipHeight - uint32(age)
	}

	// heights is sorted because entries are only ever appended.
	idx := sort.Search(len(l.heights), func(i int) bool {
		return l.heights[i] >= target
	})
	if idx >= len(l.heights) {
		idx = len(l.heights) - 1
	}

	// Walk outwards to the nearest unspent entry. Spends cluster in recent
	// history, so this probe is short in practice.
	found := -1
	for offset := 0; offset < len(l.heights); offset++ {
		if idx-offset >= 0 && !l.spent[idx-offset] {
			found = idx - offset

			break
		}
		if idx+offset < len(l.heights) && !l.spent[idx+offset] {
			found = idx + offset

			break
		}
	}
	if found < 0 {
		return [36]byte{}, 0, false
	}

	l.spent[found] = true
	l.live--

	actualAge := uint64(0)
	if tipHeight > l.heights[found] {
		actualAge = uint64(tipHeight - l.heights[found])
	}

	return l.keys[found], actualAge, true
}

// randomLive returns a key that is still unspent, for the lookup phase.
func (l *liveSet) randomLive(r *rng) ([36]byte, bool) {
	if l.live == 0 {
		return [36]byte{}, false
	}

	start := r.intn(len(l.keys))
	for offset := 0; offset < len(l.keys); offset++ {
		idx := (start + offset) % len(l.keys)
		if !l.spent[idx] {
			return l.keys[idx], true
		}
	}

	return [36]byte{}, false
}
