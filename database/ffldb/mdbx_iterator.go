package ffldb

// import (
// 	"bytes"

// 	"github.com/ledgerwatch/erigon-lib/kv"
// )

// // ldbCacheIter wraps a treap iterator to provide the additional functionality
// // needed to satisfy the leveldb iterator.Iterator interface.
// type mdbxIterator struct {
// 	cursor     kv.Cursor
// 	prefix     []byte
// 	key, value []byte // current key & value
// 	lastKey    []byte
// }

// // Enforce ldbCacheIterator implements the leveldb iterator.Iterator interface.
// var _ Iterator = (*mdbxIterator)(nil)

// func newMdbxIterator(tx kv.Tx, prefix []byte) *mdbxIterator {
// 	if (tx == nil) || (len(prefix) < 1) {
// 		return nil
// 	}

// 	csr, err := tx.Cursor(mdbxBucketRoot)
// 	if err != nil {
// 		return nil
// 	}

// 	_, _, err = csr.Seek(prefix)
// 	if err != nil {
// 		return nil
// 	}

// 	return &mdbxIterator{cursor: csr, prefix: prefix}
// }

// // Error is only provided to satisfy the iterator interface as there are no
// // errors for this memory-only structure.
// //
// // This is part of the leveldb iterator.Iterator interface implementation.
// func (iter *mdbxIterator) Error() error {
// 	return nil
// }

// // SetReleaser is only provided to satisfy the iterator interface as there is no
// // need to override it.
// //
// // This is part of the leveldb iterator.Iterator interface implementation.
// func (iter *mdbxIterator) SetReleaser(releaser Releaser) {
// }

// // Release is only provided to satisfy the iterator interface.
// //
// // This is part of the leveldb iterator.Iterator interface implementation.
// func (iter *mdbxIterator) Release() {
// 	iter.cursor.Close()

// 	iter.cursor = nil
// 	iter.key = nil
// 	iter.value = nil
// 	iter.prefix = nil
// 	iter.lastKey = nil
// }

// func (iter *mdbxIterator) Valid() bool {
// 	return iter.key != nil
// }

// func (iter *mdbxIterator) First() bool {
// 	return iter.Seek(iter.prefix)
// }

// func (iter *mdbxIterator) Last() bool {
// 	if len(iter.lastKey) > 0 {
// 		k, v, err := iter.cursor.Seek(iter.lastKey)
// 		if err != nil {
// 			return false
// 		}
// 		iter.key = copySlice(k)
// 		iter.value = copySlice(v)
// 		return true
// 	}

// 	for k, _, err := iter.cursor.Next(); k != nil; k, _, err = iter.cursor.Next() {
// 		if err != nil {
// 			return false
// 		}
// 		if bytes.HasPrefix(k, iter.prefix) {
// 			continue
// 		}

// 		k, v, err := iter.cursor.Prev()
// 		if err == nil {
// 			iter.lastKey = copySlice(k)
// 			iter.key = iter.lastKey
// 			iter.value = copySlice(v)
// 		}
// 		return err == nil
// 	}

// 	return false
// }

// func (iter *mdbxIterator) Next() bool {
// 	k, v, err := iter.cursor.Next()
// 	if err == nil {
// 		if bytes.HasPrefix(k, iter.prefix) {
// 			iter.key = copySlice(k)
// 			iter.value = copySlice(v)
// 		} else {
// 			iter.key = nil // indicate previous is the last
// 			iter.value = nil

// 			k, _, err = iter.cursor.Prev()
// 			if (err == nil) && (len(k) > 0) {
// 				iter.lastKey = copySlice(k) // indicate this is the "last"
// 			}
// 			return false
// 		}
// 	}
// 	return err == nil
// }

// func (iter *mdbxIterator) Prev() bool {
// 	k, v, err := iter.cursor.Prev()
// 	if err == nil {
// 		if bytes.HasPrefix(k, iter.prefix) {
// 			iter.key = copySlice(k)
// 			iter.value = copySlice(v)
// 		} else {
// 			iter.cursor.Next()

// 			iter.key = nil // indicate previous is the first
// 			iter.value = nil
// 			return false
// 		}
// 	}
// 	return err == nil
// }

// func (iter *mdbxIterator) Seek(key []byte) bool {
// 	k, v, err := iter.cursor.Seek(iter.prefix)
// 	if err == nil {
// 		if bytes.HasPrefix(k, iter.prefix) {
// 			iter.key = copySlice(k)
// 			iter.value = copySlice(v)
// 			return true
// 		}
// 	}
// 	return false
// }

// func (iter *mdbxIterator) Key() []byte {
// 	return iter.key
// }

// func (iter *mdbxIterator) Value() []byte {
// 	return iter.value
// }
