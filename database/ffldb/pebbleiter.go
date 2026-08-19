// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ffldb

import (
	"errors"
	"io"

	"github.com/cockroachdb/pebble"
)

// pebbleIterator adapts the storage engine's iterator to the dbIterator
// contract used throughout this package.
//
// Two differences from the contract are absorbed here. The engine positions
// with SeekGE rather than Seek, and reading a key or value from an invalid
// iterator is undefined rather than returning nil, so both accessors are
// guarded.
type pebbleIterator struct {
	iter     *pebble.Iterator
	err      error
	released bool
}

// Enforce that pebbleIterator implements dbIterator.
var _ dbIterator = (*pebbleIterator)(nil)

// newPebbleIterator wraps the passed engine iterator.
//
// The engine reports iterator construction failures as an error, while the
// callers here build iterators inside functions that have no error return.
// Passing the failure in produces an iterator that is permanently invalid and
// surfaces the cause through Error, which is what those callers already check.
func newPebbleIterator(iter *pebble.Iterator, err error) *pebbleIterator {
	if err != nil {
		return &pebbleIterator{err: err, released: true}
	}

	return &pebbleIterator{iter: iter}
}

// iterOptions converts a key range into the engine's iterator bounds. A nil
// range means the iterator is unbounded in both directions.
func iterOptions(r *keyRange) *pebble.IterOptions {
	if r == nil {
		return nil
	}

	return &pebble.IterOptions{
		LowerBound: r.start,
		UpperBound: r.limit,
	}
}

// First positions the iterator at the first key in its range.
func (p *pebbleIterator) First() bool {
	if p.released {
		return false
	}

	return p.iter.First()
}

// Last positions the iterator at the last key in its range.
func (p *pebbleIterator) Last() bool {
	if p.released {
		return false
	}

	return p.iter.Last()
}

// Seek positions the iterator at the first key at or after the passed key.
func (p *pebbleIterator) Seek(key []byte) bool {
	if p.released {
		return false
	}

	return p.iter.SeekGE(key)
}

// Next moves the iterator forward one key.
func (p *pebbleIterator) Next() bool {
	if p.released {
		return false
	}

	return p.iter.Next()
}

// Prev moves the iterator back one key.
func (p *pebbleIterator) Prev() bool {
	if p.released {
		return false
	}

	return p.iter.Prev()
}

// Valid returns whether the iterator is positioned on an entry.
func (p *pebbleIterator) Valid() bool {
	if p.released {
		return false
	}

	return p.iter.Valid()
}

// Key returns the current key, or nil when the iterator is not positioned on
// an entry.
func (p *pebbleIterator) Key() []byte {
	if !p.Valid() {
		return nil
	}

	return p.iter.Key()
}

// Value returns the current value, or nil when the iterator is not positioned
// on an entry.
func (p *pebbleIterator) Value() []byte {
	if !p.Valid() {
		return nil
	}

	return p.iter.Value()
}

// Error returns any error the iterator has encountered.
func (p *pebbleIterator) Error() error {
	if p.err != nil {
		return p.err
	}
	if p.released {
		return nil
	}

	return p.iter.Error()
}

// Release closes the underlying engine iterator. Calling it more than once is
// safe, which the cursor and cache paths rely on.
func (p *pebbleIterator) Release() {
	if p.released {
		return
	}

	p.iter.Close()
	p.released = true
}

// pebbleGet reads one key through the passed getter, copying the value out
// before releasing the engine's reference to it.
//
// The engine hands back a slice that stays valid only until the returned
// closer runs, so every read has to copy. Absent keys come back as a nil
// value and a nil error, matching what the cache layer expects.
func pebbleGet(get pebbleGetter, key []byte) ([]byte, error) {
	value, closer, err := get.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}

		return nil, err
	}
	defer closer.Close()

	result := make([]byte, len(value))
	copy(result, value)

	return result, nil
}

// pebbleGetter is satisfied by both the database and a snapshot of it.
type pebbleGetter interface {
	Get(key []byte) ([]byte, io.Closer, error)
}

// recoverClosed converts the engine's panic on a closed store into an error.
//
// The engine panics with ErrClosed when a store is used after it has been
// closed, where the previous one returned that condition as an ordinary
// error.  Without this the database interface would turn an ordinary
// use-after-close into a crash, and callers that already handle
// ErrDbNotOpen would never see it.  Any other panic is re-raised untouched,
// since only this one specific condition is being translated.
func recoverClosed(err *error) {
	r := recover()
	if r == nil {
		return
	}

	panicErr, ok := r.(error)
	if !ok || !errors.Is(panicErr, pebble.ErrClosed) {
		panic(r)
	}

	*err = convertErr("metadata store is closed", pebble.ErrClosed)
}
