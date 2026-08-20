// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ffldb

import (
	"bytes"
)

// dbIterator is the iterator contract the metadata layer is built on.
//
// It exists because the underlying key/value engine's iterator type used to be
// woven through this package directly. Defining the contract here means the
// cache iterator, the pending-keys treap iterator, and the engine's own
// iterator are interchangeable, and swapping the engine does not ripple into
// the bucket and cursor code.
//
// Positioning follows the rules the rest of this package already assumed:
// First, Last, Seek, Next and Prev each report whether the iterator ended up
// on a valid entry, and Key and Value are only meaningful while Valid reports
// true.
type dbIterator interface {
	// First positions the iterator at the first key and reports whether
	// one exists.
	First() bool

	// Last positions the iterator at the last key and reports whether one
	// exists.
	Last() bool

	// Seek positions the iterator at the first key greater than or equal
	// to the passed key and reports whether such a key exists.
	Seek(key []byte) bool

	// Next moves the iterator one key forward and reports whether it
	// remains valid.
	Next() bool

	// Prev moves the iterator one key backward and reports whether it
	// remains valid.
	Prev() bool

	// Valid reports whether the iterator is positioned on an entry.
	Valid() bool

	// Key returns the key of the current entry. The returned slice is only
	// guaranteed valid until the iterator is moved again.
	Key() []byte

	// Value returns the value of the current entry. The returned slice is
	// only guaranteed valid until the iterator is moved again.
	Value() []byte

	// Error returns any error the iterator has encountered.
	Error() error

	// Release frees the resources associated with the iterator. It is safe
	// to call more than once.
	Release()
}

// keyRange describes a half-open key range: keys greater than or equal to
// start and strictly less than limit. A nil start means unbounded below and a
// nil limit means unbounded above.
type keyRange struct {
	start []byte
	limit []byte
}

// bytesPrefix returns the key range that satisfies the given prefix.
//
// The limit is the prefix with its last non-0xff byte incremented, which is
// the smallest key sorting after every key carrying the prefix. A prefix of
// all 0xff bytes has no such upper bound, so the limit stays nil to mean
// unbounded.
func bytesPrefix(prefix []byte) *keyRange {
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i]++

			break
		}
	}

	return &keyRange{start: prefix, limit: limit}
}

// iterDirection tracks which way a merged iterator is currently walking.
//
// The distinction matters because the child iterators are only positioned
// consistently for one direction at a time: after a Next the children sit at
// or ahead of the merged position, and after a Prev they sit at or behind it.
// Reversing therefore has to reposition every child before picking a
// neighbour.
type iterDirection int

const (
	dirReleased iterDirection = iota
	dirSOI
	dirEOI
	dirForward
	dirBackward
)

// mergedIterator presents several iterators over disjoint or overlapping key
// ranges as one iterator in key order.
//
// The metadata layer needs this because a bucket's cursor walks both the
// bucket's keys and its nested bucket index, which live under two different
// prefixes, and it must see them as a single ordered sequence in both
// directions.
type mergedIterator struct {
	iters   []dbIterator
	dir     iterDirection
	current int
	err     error

	// keys holds each child's current key so the winner can be chosen
	// without repeatedly calling through to the children.
	//
	// The keys are copied rather than aliased. An engine iterator only has
	// to keep its key valid until it moves again, so holding the engine's
	// slice here would leave the cache pointing at whatever that child
	// moved on to. bufs backs the copies and is reused, so the safety does
	// not cost an allocation per step.
	keys [][]byte
	bufs [][]byte

	// curBuf holds a copy of the key being stepped off, which cannot alias
	// keys because that array is overwritten as the children advance.
	curBuf []byte
}

// Enforce that mergedIterator implements dbIterator.
var _ dbIterator = (*mergedIterator)(nil)

// newMergedIterator returns an iterator yielding the union of the passed
// iterators in key order.
func newMergedIterator(iters []dbIterator) *mergedIterator {
	return &mergedIterator{
		iters: iters,
		dir:   dirSOI,
		keys:  make([][]byte, len(iters)),
		bufs:  make([][]byte, len(iters)),
	}
}

// setKey caches the child's current key.
func (m *mergedIterator) setKey(i int, iter dbIterator) {
	key := iter.Key()
	if key == nil {
		m.clearKey(i, iter)

		return
	}

	m.bufs[i] = append(m.bufs[i][:0], key...)
	m.keys[i] = m.bufs[i]
}

// clearKey marks a child as exhausted and records any error it reported.
func (m *mergedIterator) clearKey(i int, iter dbIterator) {
	m.keys[i] = nil
	m.recordErr(iter)
}

// smallest selects the child holding the smallest current key.
func (m *mergedIterator) smallest() int {
	winner := -1
	for i, key := range m.keys {
		if key == nil {
			continue
		}
		if winner < 0 || bytes.Compare(key, m.keys[winner]) < 0 {
			winner = i
		}
	}

	return winner
}

// largest selects the child holding the largest current key.
func (m *mergedIterator) largest() int {
	winner := -1
	for i, key := range m.keys {
		if key == nil {
			continue
		}
		if winner < 0 || bytes.Compare(key, m.keys[winner]) > 0 {
			winner = i
		}
	}

	return winner
}

// First positions every child at its first key and selects the smallest.
func (m *mergedIterator) First() bool {
	if m.dir == dirReleased {
		return false
	}

	for i, iter := range m.iters {
		if iter.First() {
			m.setKey(i, iter)
		} else {
			m.clearKey(i, iter)
		}
	}

	m.current = m.smallest()
	m.dir = dirForward
	if m.current < 0 {
		m.dir = dirEOI

		return false
	}

	return true
}

// Last positions every child at its last key and selects the largest.
func (m *mergedIterator) Last() bool {
	if m.dir == dirReleased {
		return false
	}

	for i, iter := range m.iters {
		if iter.Last() {
			m.setKey(i, iter)
		} else {
			m.clearKey(i, iter)
		}
	}

	m.current = m.largest()
	m.dir = dirBackward
	if m.current < 0 {
		m.dir = dirSOI

		return false
	}

	return true
}

// Seek positions every child at the first key at or after the passed key and
// selects the smallest.
func (m *mergedIterator) Seek(key []byte) bool {
	if m.dir == dirReleased {
		return false
	}

	for i, iter := range m.iters {
		if iter.Seek(key) {
			m.setKey(i, iter)
		} else {
			m.clearKey(i, iter)
		}
	}

	m.current = m.smallest()
	m.dir = dirForward
	if m.current < 0 {
		m.dir = dirEOI

		return false
	}

	return true
}

// Next advances past the current key and selects the next smallest.
//
// When the previous move was backwards every child sits at or behind the
// merged position, so they are re-seeked forward to the current key before a
// winner can be chosen.
func (m *mergedIterator) Next() bool {
	switch m.dir {
	case dirSOI:
		return m.First()

	case dirReleased, dirEOI:
		return false

	case dirBackward:
		m.curBuf = append(m.curBuf[:0], m.keys[m.current]...)
		for i, iter := range m.iters {
			if iter.Seek(m.curBuf) {
				m.setKey(i, iter)
			} else {
				m.clearKey(i, iter)
			}
		}
		m.dir = dirForward
	}

	// Advance every child sitting on the current key. More than one can
	// hold it when the child ranges overlap. The key is copied first
	// because the loop overwrites the cache it lives in.
	m.curBuf = append(m.curBuf[:0], m.keys[m.current]...)
	for i, iter := range m.iters {
		if m.keys[i] == nil || !bytes.Equal(m.keys[i], m.curBuf) {
			continue
		}
		if iter.Next() {
			m.setKey(i, iter)
		} else {
			m.clearKey(i, iter)
		}
	}

	m.current = m.smallest()
	if m.current < 0 {
		m.dir = dirEOI

		return false
	}

	return true
}

// Prev steps back from the current key and selects the next largest.
func (m *mergedIterator) Prev() bool {
	switch m.dir {
	case dirEOI:
		return m.Last()

	case dirReleased, dirSOI:
		return false

	case dirForward:
		// Reposition every child behind the current key. Seek only
		// moves forward, so land on the key and step back from there;
		// a child with no such key is walked to its last entry
		// instead, which is still behind the merged position.
		m.curBuf = append(m.curBuf[:0], m.keys[m.current]...)
		for i, iter := range m.iters {
			var ok bool
			if iter.Seek(m.curBuf) {
				ok = iter.Prev()
			} else {
				ok = iter.Last()
			}
			if ok {
				m.setKey(i, iter)
			} else {
				m.clearKey(i, iter)
			}
		}
		m.dir = dirBackward

		m.current = m.largest()
		if m.current < 0 {
			m.dir = dirSOI

			return false
		}

		return true
	}

	m.curBuf = append(m.curBuf[:0], m.keys[m.current]...)
	for i, iter := range m.iters {
		if m.keys[i] == nil || !bytes.Equal(m.keys[i], m.curBuf) {
			continue
		}
		if iter.Prev() {
			m.setKey(i, iter)
		} else {
			m.clearKey(i, iter)
		}
	}

	m.current = m.largest()
	if m.current < 0 {
		m.dir = dirSOI

		return false
	}

	return true
}

// Valid returns whether the merged iterator is positioned on an entry.
func (m *mergedIterator) Valid() bool {
	switch m.dir {
	case dirReleased, dirSOI, dirEOI:
		return false
	}

	return m.current >= 0
}

// Key returns the current key.
func (m *mergedIterator) Key() []byte {
	if !m.Valid() {
		return nil
	}

	return m.iters[m.current].Key()
}

// Value returns the current value.
func (m *mergedIterator) Value() []byte {
	if !m.Valid() {
		return nil
	}

	return m.iters[m.current].Value()
}

// Error returns the first error any child reported.
func (m *mergedIterator) Error() error {
	return m.err
}

// Release frees every child iterator.
func (m *mergedIterator) Release() {
	if m.dir == dirReleased {
		return
	}

	for _, iter := range m.iters {
		iter.Release()
	}
	m.iters = nil
	m.keys = nil
	m.bufs = nil
	m.dir = dirReleased
}

// recordErr keeps the first error a child reported.
func (m *mergedIterator) recordErr(iter dbIterator) {
	if m.err == nil {
		m.err = iter.Error()
	}
}

// compareKeys orders two keys the way the metadata store does.
//
// The store's keys are ordered lexicographically by unsigned byte value, which
// is what both the engine and the pending-keys treap use, so the comparison is
// kept in one place rather than being open-coded per iterator.
func compareKeys(a, b []byte) int {
	return bytes.Compare(a, b)
}
