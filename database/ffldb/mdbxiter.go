// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ffldb

import (
	"github.com/erigontech/mdbx-go/mdbx"
)

// mdbxIterator adapts an engine cursor to the dbIterator contract used
// throughout this package.
//
// The engine's cursor differs from the contract in three ways, all absorbed
// here. It reports the end of the range by returning a not-found error rather
// than a false result, it has no notion of being bounded to a key range, and
// its key and value are only valid until the cursor moves or its transaction
// ends. The bounds are therefore enforced here, and the caller's contract that
// Key and Value stay valid until the next move is met by the engine's own
// guarantee.
type mdbxIterator struct {
	cursor *mdbx.Cursor
	bounds *keyRange

	key   []byte
	value []byte
	valid bool

	err      error
	released bool
}

// Enforce that mdbxIterator implements dbIterator.
var _ dbIterator = (*mdbxIterator)(nil)

// newMDBXIterator wraps a cursor, restricting it to the passed range.
//
// A construction failure produces a permanently invalid iterator carrying the
// error, since the callers build iterators inside functions with no error
// return and already check Error.
func newMDBXIterator(cursor *mdbx.Cursor, bounds *keyRange,
	err error) *mdbxIterator {

	if err != nil {
		return &mdbxIterator{err: err, released: true}
	}

	return &mdbxIterator{cursor: cursor, bounds: bounds}
}

// inBounds reports whether the key falls inside the iterator's range.
func (m *mdbxIterator) inBounds(key []byte) bool {
	if m.bounds == nil {
		return true
	}
	if m.bounds.start != nil && compareKeys(key, m.bounds.start) < 0 {
		return false
	}
	if m.bounds.limit != nil && compareKeys(key, m.bounds.limit) >= 0 {
		return false
	}

	return true
}

// apply records the outcome of a cursor operation.
func (m *mdbxIterator) apply(key, value []byte, err error) bool {
	if err != nil {
		if !mdbx.IsNotFound(err) && m.err == nil {
			m.err = err
		}
		m.key, m.value, m.valid = nil, nil, false

		return false
	}
	if !m.inBounds(key) {
		m.key, m.value, m.valid = nil, nil, false

		return false
	}

	m.key, m.value, m.valid = key, value, true

	return true
}

// First positions the iterator at the first key in its range.
func (m *mdbxIterator) First() bool {
	if m.released {
		return false
	}

	// With a lower bound, the first in-range key is the first at or after
	// it; without one it is the first key in the table.
	if m.bounds != nil && m.bounds.start != nil {
		return m.Seek(m.bounds.start)
	}

	return m.apply(m.cursor.Get(nil, nil, mdbx.First))
}

// Last positions the iterator at the last key in its range.
func (m *mdbxIterator) Last() bool {
	if m.released {
		return false
	}

	if m.bounds == nil || m.bounds.limit == nil {
		return m.apply(m.cursor.Get(nil, nil, mdbx.Last))
	}

	// Land on the first key at or after the limit, then step back to the
	// last key below it. When nothing sorts at or after the limit, the
	// last key in the table is the candidate.
	key, _, err := m.cursor.Get(m.bounds.limit, nil, mdbx.SetRange)
	if err != nil {
		if !mdbx.IsNotFound(err) {
			return m.apply(nil, nil, err)
		}

		return m.apply(m.cursor.Get(nil, nil, mdbx.Last))
	}
	_ = key

	return m.apply(m.cursor.Get(nil, nil, mdbx.Prev))
}

// Seek positions the iterator at the first key at or after the passed key.
func (m *mdbxIterator) Seek(key []byte) bool {
	if m.released {
		return false
	}

	return m.apply(m.cursor.Get(key, nil, mdbx.SetRange))
}

// Next moves the iterator forward one key.
func (m *mdbxIterator) Next() bool {
	if m.released {
		return false
	}

	// Advancing from an invalid position is how the merged iterator asks
	// for the first key, which the engine cursor cannot answer.
	if !m.valid {
		return m.First()
	}

	return m.apply(m.cursor.Get(nil, nil, mdbx.Next))
}

// Prev moves the iterator back one key.
func (m *mdbxIterator) Prev() bool {
	if m.released {
		return false
	}

	if !m.valid {
		return m.Last()
	}

	return m.apply(m.cursor.Get(nil, nil, mdbx.Prev))
}

// Valid returns whether the iterator is positioned on an entry.
func (m *mdbxIterator) Valid() bool {
	return !m.released && m.valid
}

// Key returns the current key.
func (m *mdbxIterator) Key() []byte {
	if !m.Valid() {
		return nil
	}

	return m.key
}

// Value returns the current value.
func (m *mdbxIterator) Value() []byte {
	if !m.Valid() {
		return nil
	}

	return m.value
}

// Error returns any error the iterator has encountered.
func (m *mdbxIterator) Error() error {
	return m.err
}

// Release closes the underlying cursor. Calling it more than once is safe,
// which the cursor and cache paths rely on.
func (m *mdbxIterator) Release() {
	if m.released {
		return
	}

	if m.cursor != nil {
		m.cursor.Close()
	}
	m.key, m.value, m.valid = nil, nil, false
	m.released = true
}
