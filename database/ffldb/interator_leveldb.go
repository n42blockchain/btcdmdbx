// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Note:
// All contents of this file are copied from go-leveldb project
// intend to reduce the dependency to that
//

package ffldb

// import (
// 	"bytes"
// 	"errors"
// )

// type Iterator interface {
// 	CommonIterator

// 	// Key returns the key of the current key/value pair, or nil if done.
// 	// The caller should not modify the contents of the returned slice, and
// 	// its contents may change on the next call to any 'seeks method'.
// 	Key() []byte

// 	// Value returns the value of the current key/value pair, or nil if done.
// 	// The caller should not modify the contents of the returned slice, and
// 	// its contents may change on the next call to any 'seeks method'.
// 	Value() []byte
// }

// // CommonIterator is the interface that wraps common iterator methods.
// type CommonIterator interface {
// 	IteratorSeeker

// 	// util.Releaser is the interface that wraps basic Release method.
// 	// When called Release will releases any resources associated with the
// 	// iterator.
// 	Releaser

// 	// util.ReleaseSetter is the interface that wraps the basic SetReleaser
// 	// method.
// 	// ReleaseSetter

// 	// TODO: Remove this when ready.
// 	Valid() bool

// 	// Error returns any accumulated error. Exhausting all the key/value pairs
// 	// is not considered to be an error.
// 	Error() error
// }

// // Releaser is the interface that wraps the basic Release method.
// type Releaser interface {
// 	// Release releases associated resources. Release should always success
// 	// and can be called multiple times without causing error.
// 	Release()
// }

// // ReleaseSetter is the interface that wraps the basic SetReleaser method.
// // type ReleaseSetter interface {
// // 	// SetReleaser associates the given releaser to the resources. The
// // 	// releaser will be called once coresponding resources released.
// // 	// Calling SetReleaser with nil will clear the releaser.
// // 	//
// // 	// This will panic if a releaser already present or coresponding
// // 	// resource is already released. Releaser should be cleared first
// // 	// before assigned a new one.
// // 	SetReleaser(releaser Releaser)
// // }

// // IteratorSeeker is the interface that wraps the 'seeks method'.
// type IteratorSeeker interface {
// 	// First moves the iterator to the first key/value pair. If the iterator
// 	// only contains one key/value pair then First and Last would moves
// 	// to the same key/value pair.
// 	// It returns whether such pair exist.
// 	First() bool

// 	// Last moves the iterator to the last key/value pair. If the iterator
// 	// only contains one key/value pair then First and Last would moves
// 	// to the same key/value pair.
// 	// It returns whether such pair exist.
// 	Last() bool

// 	// Seek moves the iterator to the first key/value pair whose key is greater
// 	// than or equal to the given key.
// 	// It returns whether such pair exist.
// 	//
// 	// It is safe to modify the contents of the argument after Seek returns.
// 	Seek(key []byte) bool

// 	// Next moves the iterator to the next key/value pair.
// 	// It returns false if the iterator is exhausted.
// 	Next() bool

// 	// Prev moves the iterator to the previous key/value pair.
// 	// It returns false if the iterator is exhausted.
// 	Prev() bool
// }

// // Range is a key range.
// type Range struct {
// 	// Start of the key range, include in the range.
// 	Start []byte

// 	// Limit of the key range, not include in the range.
// 	Limit []byte
// }

// // BytesPrefix returns key range that satisfy the given prefix.
// // This only applicable for the standard 'bytes comparer'.
// func BytesPrefix(prefix []byte) *Range {
// 	var limit []byte
// 	for i := len(prefix) - 1; i >= 0; i-- {
// 		c := prefix[i]
// 		if c < 0xff {
// 			limit = make([]byte, i+1)
// 			copy(limit, prefix)
// 			limit[i] = c + 1
// 			break
// 		}
// 	}
// 	return &Range{prefix, limit}
// }

// // BasicComparer is the interface that wraps the basic Compare method.
// type BasicComparer interface {
// 	// Compare returns -1, 0, or +1 depending on whether a is 'less than',
// 	// 'equal to' or 'greater than' b. The two arguments can only be 'equal'
// 	// if their contents are exactly equal. Furthermore, the empty slice
// 	// must be 'less than' any non-empty slice.
// 	Compare(a, b []byte) int
// }

// // Comparer defines a total ordering over the space of []byte keys: a 'less
// // than' relationship.
// type Comparer interface {
// 	BasicComparer

// 	// Name returns name of the comparer.
// 	//
// 	// The Level-DB on-disk format stores the comparer name, and opening a
// 	// database with a different comparer from the one it was created with
// 	// will result in an error.
// 	//
// 	// An implementation to a new name whenever the comparer implementation
// 	// changes in a way that will cause the relative ordering of any two keys
// 	// to change.
// 	//
// 	// Names starting with "leveldb." are reserved and should not be used
// 	// by any users of this package.
// 	Name() string

// 	// Bellow are advanced functions used to reduce the space requirements
// 	// for internal data structures such as index blocks.

// 	// Separator appends a sequence of bytes x to dst such that a <= x && x < b,
// 	// where 'less than' is consistent with Compare. An implementation should
// 	// return nil if x equal to a.
// 	//
// 	// Either contents of a or b should not by any means modified. Doing so
// 	// may cause corruption on the internal state.
// 	Separator(dst, a, b []byte) []byte

// 	// Successor appends a sequence of bytes x to dst such that x >= b, where
// 	// 'less than' is consistent with Compare. An implementation should return
// 	// nil if x equal to b.
// 	//
// 	// Contents of b should not by any means modified. Doing so may cause
// 	// corruption on the internal state.
// 	Successor(dst, b []byte) []byte
// }

// func NewMergedIterator(iters []Iterator, cmp Comparer, strict bool) Iterator {
// 	return &mergedIterator{
// 		iters:  iters,
// 		cmp:    cmp,
// 		strict: strict,
// 		keys:   make([][]byte, len(iters)),
// 	}
// }

// const (
// 	dirReleased dir = iota - 1
// 	dirSOI
// 	dirEOI
// 	dirBackward
// 	dirForward
// )

// type dir int

// type mergedIterator struct {
// 	cmp    Comparer
// 	iters  []Iterator
// 	strict bool

// 	keys     [][]byte
// 	index    int
// 	dir      dir
// 	err      error
// 	errf     func(err error)
// 	releaser Releaser
// }

// func (i *mergedIterator) iterErr(iter Iterator) bool {
// 	if err := iter.Error(); err != nil {
// 		if i.errf != nil {
// 			i.errf(err)
// 		}
// 		if i.strict || !IsCorrupted(err) {
// 			i.err = err
// 			return true
// 		}
// 	}
// 	return false
// }

// func (i *mergedIterator) Valid() bool {
// 	return i.err == nil && i.dir > dirEOI
// }

// func (i *mergedIterator) First() bool {
// 	if i.err != nil {
// 		return false
// 	} else if i.dir == dirReleased {
// 		i.err = ErrIterReleased
// 		return false
// 	}

// 	for x, iter := range i.iters {
// 		switch {
// 		case iter.First():
// 			i.keys[x] = assertKey(iter.Key())
// 		case i.iterErr(iter):
// 			return false
// 		default:
// 			i.keys[x] = nil
// 		}
// 	}
// 	i.dir = dirSOI
// 	return i.next()
// }

// func (i *mergedIterator) Last() bool {
// 	if i.err != nil {
// 		return false
// 	} else if i.dir == dirReleased {
// 		i.err = ErrIterReleased
// 		return false
// 	}

// 	for x, iter := range i.iters {
// 		switch {
// 		case iter.Last():
// 			i.keys[x] = assertKey(iter.Key())
// 		case i.iterErr(iter):
// 			return false
// 		default:
// 			i.keys[x] = nil
// 		}
// 	}
// 	i.dir = dirEOI
// 	return i.prev()
// }

// func (i *mergedIterator) Seek(key []byte) bool {
// 	if i.err != nil {
// 		return false
// 	} else if i.dir == dirReleased {
// 		i.err = ErrIterReleased
// 		return false
// 	}

// 	for x, iter := range i.iters {
// 		switch {
// 		case iter.Seek(key):
// 			i.keys[x] = assertKey(iter.Key())
// 		case i.iterErr(iter):
// 			return false
// 		default:
// 			i.keys[x] = nil
// 		}
// 	}
// 	i.dir = dirSOI
// 	return i.next()
// }

// func (i *mergedIterator) next() bool {
// 	var key []byte
// 	if i.dir == dirForward {
// 		key = i.keys[i.index]
// 	}
// 	for x, tkey := range i.keys {
// 		if tkey != nil && (key == nil || i.cmp.Compare(tkey, key) < 0) {
// 			key = tkey
// 			i.index = x
// 		}
// 	}
// 	if key == nil {
// 		i.dir = dirEOI
// 		return false
// 	}
// 	i.dir = dirForward
// 	return true
// }

// func (i *mergedIterator) Next() bool {
// 	if i.dir == dirEOI || i.err != nil {
// 		return false
// 	} else if i.dir == dirReleased {
// 		i.err = ErrIterReleased
// 		return false
// 	}

// 	switch i.dir {
// 	case dirSOI:
// 		return i.First()
// 	case dirBackward:
// 		key := append([]byte{}, i.keys[i.index]...)
// 		if !i.Seek(key) {
// 			return false
// 		}
// 		return i.Next()
// 	}

// 	x := i.index
// 	iter := i.iters[x]
// 	switch {
// 	case iter.Next():
// 		i.keys[x] = assertKey(iter.Key())
// 	case i.iterErr(iter):
// 		return false
// 	default:
// 		i.keys[x] = nil
// 	}
// 	return i.next()
// }

// func (i *mergedIterator) prev() bool {
// 	var key []byte
// 	if i.dir == dirBackward {
// 		key = i.keys[i.index]
// 	}
// 	for x, tkey := range i.keys {
// 		if tkey != nil && (key == nil || i.cmp.Compare(tkey, key) > 0) {
// 			key = tkey
// 			i.index = x
// 		}
// 	}
// 	if key == nil {
// 		i.dir = dirSOI
// 		return false
// 	}
// 	i.dir = dirBackward
// 	return true
// }

// func (i *mergedIterator) Prev() bool {
// 	if i.dir == dirSOI || i.err != nil {
// 		return false
// 	} else if i.dir == dirReleased {
// 		i.err = ErrIterReleased
// 		return false
// 	}

// 	switch i.dir {
// 	case dirEOI:
// 		return i.Last()
// 	case dirForward:
// 		key := append([]byte{}, i.keys[i.index]...)
// 		for x, iter := range i.iters {
// 			if x == i.index {
// 				continue
// 			}
// 			seek := iter.Seek(key)
// 			switch {
// 			case seek && iter.Prev(), !seek && iter.Last():
// 				i.keys[x] = assertKey(iter.Key())
// 			case i.iterErr(iter):
// 				return false
// 			default:
// 				i.keys[x] = nil
// 			}
// 		}
// 	}

// 	x := i.index
// 	iter := i.iters[x]
// 	switch {
// 	case iter.Prev():
// 		i.keys[x] = assertKey(iter.Key())
// 	case i.iterErr(iter):
// 		return false
// 	default:
// 		i.keys[x] = nil
// 	}
// 	return i.prev()
// }

// func (i *mergedIterator) Key() []byte {
// 	if i.err != nil || i.dir <= dirEOI {
// 		return nil
// 	}
// 	return i.keys[i.index]
// }

// func (i *mergedIterator) Value() []byte {
// 	if i.err != nil || i.dir <= dirEOI {
// 		return nil
// 	}
// 	return i.iters[i.index].Value()
// }

// func (i *mergedIterator) Release() {
// 	if i.dir != dirReleased {
// 		i.dir = dirReleased
// 		for _, iter := range i.iters {
// 			iter.Release()
// 		}
// 		i.iters = nil
// 		i.keys = nil
// 		if i.releaser != nil {
// 			i.releaser.Release()
// 			i.releaser = nil
// 		}
// 	}
// }

// func (i *mergedIterator) SetReleaser(releaser Releaser) {
// 	if i.dir == dirReleased {
// 		// panic(util.ErrReleased)
// 	}
// 	if i.releaser != nil && releaser != nil {
// 		// panic(ErrHasReleaser)
// 	}
// 	i.releaser = releaser
// }

// func (i *mergedIterator) Error() error {
// 	return i.err
// }

// func (i *mergedIterator) SetErrorCallback(f func(err error)) {
// 	i.errf = f
// }

// var DefaultComparer = bytesComparer{}

// type bytesComparer struct{}

// func (bytesComparer) Compare(a, b []byte) int {
// 	return bytes.Compare(a, b)
// }

// func (bytesComparer) Name() string {
// 	return "leveldb.BytewiseComparator"
// }

// func (bytesComparer) Separator(dst, a, b []byte) []byte {
// 	i, n := 0, len(a)
// 	if n > len(b) {
// 		n = len(b)
// 	}
// 	for ; i < n && a[i] == b[i]; i++ {
// 	}
// 	if i >= n {
// 		// Do not shorten if one string is a prefix of the other
// 	} else if c := a[i]; c < 0xff && c+1 < b[i] {
// 		dst = append(dst, a[:i+1]...)
// 		dst[len(dst)-1]++
// 		return dst
// 	}
// 	return nil
// }

// func (bytesComparer) Successor(dst, b []byte) []byte {
// 	for i, c := range b {
// 		if c != 0xff {
// 			dst = append(dst, b[:i+1]...)
// 			dst[len(dst)-1]++
// 			return dst
// 		}
// 	}
// 	return nil
// }

// var (
// 	ErrIterReleased = errors.New("leveldb/iterator: iterator released")
// )

// // IsCorrupted returns a boolean indicating whether the error is indicating
// // a corruption.
// func IsCorrupted(err error) bool {
// 	// switch err.(type) {
// 	// case *ErrCorrupted:
// 	// 	return true
// 	// case *storage.ErrCorrupted:
// 	// 	return true
// 	// }
// 	return false
// }

// func assertKey(key []byte) []byte {
// 	if key == nil {
// 		panic("leveldb/iterator: nil key")
// 	}
// 	return key
// }

// // IsCorrupted returns a boolean indicating whether the error is indicating
// // a corruption.
// // func IsCorrupted(err error) bool {
// // 	switch err.(type) {
// // 	case *ErrCorrupted:
// // 		return true
// // 	case *storage.ErrCorrupted:
// // 		return true
// // 	}
// // 	return false
// // }
