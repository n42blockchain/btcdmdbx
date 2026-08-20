// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ffldb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/btcsuite/btcd/database/internal/treap"
	"github.com/erigontech/mdbx-go/mdbx"
)

const (
	// defaultCacheSize is the default size for the database cache.
	defaultCacheSize = 100 * 1024 * 1024 // 100 MB

	// defaultFlushSecs is the default number of seconds to use as a
	// threshold in between database cache flushes when the cache size has
	// not been exceeded.
	defaultFlushSecs = 300 // 5 minutes

	// batchHeaderSize is the size of a write batch header, which includes
	// the sequence header and record counter.
	//
	// batchRecordKeySize is the per-record key overhead the engine adds
	// when appending a record to a batch.
	//
	// These are used to preallocate the space a batch needs in one
	// allocation instead of letting it grow repeatedly. That results in
	// far less pressure on the GC and keeps it from reserving a lot of
	// extra unneeded space.
	batchHeaderSize    = 12
	batchRecordKeySize = 8
)

// cacheTreapIter wraps a treap iterator with the two methods the dbIterator
// contract needs beyond what the treap already provides.
type cacheTreapIter struct {
	*treap.Iterator
}

// Enforce cacheTreapIter implements dbIterator.
var _ dbIterator = (*cacheTreapIter)(nil)

// Error is only provided to satisfy the iterator interface as there are no
// errors for this memory-only structure.
//
// This is part of the dbIterator interface implementation.
func (iter *cacheTreapIter) Error() error {
	return nil
}

// Release is only provided to satisfy the iterator interface.
//
// This is part of the dbIterator interface implementation.
func (iter *cacheTreapIter) Release() {
}

// newCacheTreapIter creates a new treap iterator for the given range against
// the pending keys for the passed cache snapshot.
func newCacheTreapIter(snap *dbCacheSnapshot, r *keyRange) *cacheTreapIter {
	iter := snap.pendingKeys.Iterator(r.start, r.limit)
	return &cacheTreapIter{Iterator: iter}
}

// dbCacheIterator defines an iterator over the key/value pairs in the database
// cache and underlying database.
type dbCacheIterator struct {
	cacheSnapshot *dbCacheSnapshot
	dbIter        dbIterator
	cacheIter     dbIterator
	currentIter   dbIterator
	released      bool
}

// Enforce dbCacheIterator implements dbIterator.
var _ dbIterator = (*dbCacheIterator)(nil)

// skipPendingUpdates skips any keys at the current database iterator position
// that are being updated by the cache.  The forwards flag indicates the
// direction the iterator is moving.
func (iter *dbCacheIterator) skipPendingUpdates(forwards bool) {
	for iter.dbIter.Valid() {
		var skip bool
		key := iter.dbIter.Key()
		if iter.cacheSnapshot.pendingRemove.Has(key) {
			skip = true
		} else if iter.cacheSnapshot.pendingKeys.Has(key) {
			skip = true
		}
		if !skip {
			break
		}

		if forwards {
			iter.dbIter.Next()
		} else {
			iter.dbIter.Prev()
		}
	}
}

// chooseIterator first skips any entries in the database iterator that are
// being updated by the cache and sets the current iterator to the appropriate
// iterator depending on their validity and the order they compare in while taking
// into account the direction flag.  When the iterator is being moved forwards
// and both iterators are valid, the iterator with the smaller key is chosen and
// vice versa when the iterator is being moved backwards.
func (iter *dbCacheIterator) chooseIterator(forwards bool) bool {
	// Skip any keys at the current database iterator position that are
	// being updated by the cache.
	iter.skipPendingUpdates(forwards)

	// When both iterators are exhausted, the iterator is exhausted too.
	if !iter.dbIter.Valid() && !iter.cacheIter.Valid() {
		iter.currentIter = nil
		return false
	}

	// Choose the database iterator when the cache iterator is exhausted.
	if !iter.cacheIter.Valid() {
		iter.currentIter = iter.dbIter
		return true
	}

	// Choose the cache iterator when the database iterator is exhausted.
	if !iter.dbIter.Valid() {
		iter.currentIter = iter.cacheIter
		return true
	}

	// Both iterators are valid, so choose the iterator with either the
	// smaller or larger key depending on the forwards flag.
	compare := bytes.Compare(iter.dbIter.Key(), iter.cacheIter.Key())
	if (forwards && compare > 0) || (!forwards && compare < 0) {
		iter.currentIter = iter.cacheIter
	} else {
		iter.currentIter = iter.dbIter
	}
	return true
}

// First positions the iterator at the first key/value pair and returns whether
// or not the pair exists.
//
// This is part of the dbIterator interface implementation.
func (iter *dbCacheIterator) First() bool {
	// Seek to the first key in both the database and cache iterators and
	// choose the iterator that is both valid and has the smaller key.
	iter.dbIter.First()
	iter.cacheIter.First()
	return iter.chooseIterator(true)
}

// Last positions the iterator at the last key/value pair and returns whether or
// not the pair exists.
//
// This is part of the dbIterator interface implementation.
func (iter *dbCacheIterator) Last() bool {
	// Seek to the last key in both the database and cache iterators and
	// choose the iterator that is both valid and has the larger key.
	iter.dbIter.Last()
	iter.cacheIter.Last()
	return iter.chooseIterator(false)
}

// Next moves the iterator one key/value pair forward and returns whether or not
// the pair exists.
//
// This is part of the dbIterator interface implementation.
func (iter *dbCacheIterator) Next() bool {
	// Nothing to return if cursor is exhausted.
	if iter.currentIter == nil {
		return false
	}

	// Move the current iterator to the next entry and choose the iterator
	// that is both valid and has the smaller key.
	iter.currentIter.Next()
	return iter.chooseIterator(true)
}

// Prev moves the iterator one key/value pair backward and returns whether or
// not the pair exists.
//
// This is part of the dbIterator interface implementation.
func (iter *dbCacheIterator) Prev() bool {
	// Nothing to return if cursor is exhausted.
	if iter.currentIter == nil {
		return false
	}

	// Move the current iterator to the previous entry and choose the
	// iterator that is both valid and has the larger key.
	iter.currentIter.Prev()
	return iter.chooseIterator(false)
}

// Seek positions the iterator at the first key/value pair that is greater than
// or equal to the passed seek key.  Returns false if no suitable key was found.
//
// This is part of the dbIterator interface implementation.
func (iter *dbCacheIterator) Seek(key []byte) bool {
	// Seek to the provided key in both the database and cache iterators
	// then choose the iterator that is both valid and has the larger key.
	iter.dbIter.Seek(key)
	iter.cacheIter.Seek(key)
	return iter.chooseIterator(true)
}

// Valid indicates whether the iterator is positioned at a valid key/value pair.
// It will be considered invalid when the iterator is newly created or exhausted.
//
// This is part of the dbIterator interface implementation.
func (iter *dbCacheIterator) Valid() bool {
	return iter.currentIter != nil
}

// Key returns the current key the iterator is pointing to.
//
// This is part of the dbIterator interface implementation.
func (iter *dbCacheIterator) Key() []byte {
	// Nothing to return if iterator is exhausted.
	if iter.currentIter == nil {
		return nil
	}

	return iter.currentIter.Key()
}

// Value returns the current value the iterator is pointing to.
//
// This is part of the dbIterator interface implementation.
func (iter *dbCacheIterator) Value() []byte {
	// Nothing to return if iterator is exhausted.
	if iter.currentIter == nil {
		return nil
	}

	return iter.currentIter.Value()
}

// Release releases the iterator by removing the underlying treap iterator from
// the list of active iterators against the pending keys treap.
//
// This is part of the dbIterator interface implementation.
func (iter *dbCacheIterator) Release() {
	if !iter.released {
		iter.dbIter.Release()
		iter.cacheIter.Release()
		iter.currentIter = nil
		iter.released = true
	}
}

// Error is only provided to satisfy the iterator interface as there are no
// errors for this memory-only structure.
//
// This is part of the dbIterator interface implementation.
func (iter *dbCacheIterator) Error() error {
	return nil
}

// dbCacheSnapshot defines a snapshot of the database cache and underlying
// database at a particular point in time.
// dbCacheSnapshot holds a read transaction against the store plus the
// cached keys as of the moment it was taken.
//
// The read transaction is what gives the snapshot its consistent view.  It
// must be released promptly: the engine cannot reclaim pages that any open
// read transaction might still reach, so a forgotten snapshot shows up as
// unbounded file growth rather than as an error.
type dbCacheSnapshot struct {
	dbTx          *mdbx.Txn
	dbi           mdbx.DBI
	pendingKeys   *treap.Immutable
	pendingRemove *treap.Immutable
}

// Has returns whether or not the passed key exists.
func (snap *dbCacheSnapshot) Has(key []byte) bool {
	// Check the cached entries first.
	if snap.pendingRemove.Has(key) {
		return false
	}
	if snap.pendingKeys.Has(key) {
		return true
	}

	// Consult the database.
	_, err := snap.dbTx.Get(snap.dbi, key)
	return err == nil
}

// Get returns the value for the passed key.  The function will return nil when
// the key does not exist.
func (snap *dbCacheSnapshot) Get(key []byte) []byte {
	// Check the cached entries first.
	if snap.pendingRemove.Has(key) {
		return nil
	}
	if value := snap.pendingKeys.Get(key); value != nil {
		return value
	}

	// Consult the database.  The returned bytes belong to the read
	// transaction and stay valid for as long as it does, which outlives
	// every caller here, so no copy is needed.
	value, err := snap.dbTx.Get(snap.dbi, key)
	if err != nil {
		return nil
	}
	return value
}

// Release releases the snapshot.
func (snap *dbCacheSnapshot) Release() {
	snap.dbTx.Abort()
	snap.pendingKeys = nil
	snap.pendingRemove = nil
}

// NewIterator returns a new iterator for the snapshot.  The newly returned
// iterator is not pointing to a valid item until a call to one of the methods
// to position it is made.
//
// The slice parameter allows the iterator to be limited to a range of keys.
// The start key is inclusive and the limit key is exclusive.  Either or both
// can be nil if the functionality is not desired.
func (snap *dbCacheSnapshot) NewIterator(r *keyRange) *dbCacheIterator {
	cursor, err := snap.dbTx.OpenCursor(snap.dbi)

	return &dbCacheIterator{
		dbIter:        newMDBXIterator(cursor, r, err),
		cacheIter:     newCacheTreapIter(snap, r),
		cacheSnapshot: snap,
	}
}

// dbCache provides a database cache layer backed by an underlying database.  It
// allows a maximum cache size and flush interval to be specified such that the
// cache is flushed to the database when the cache size exceeds the maximum
// configured value or it has been longer than the configured interval since the
// last flush.  This effectively provides transaction batching so that callers
// can commit transactions at will without incurring large performance hits due
// to frequent disk syncs.
type dbCache struct {
	// env is the underlying key/value store holding the metadata, and dbi
	// the single table within it that everything is namespaced into.
	env *mdbx.Env
	dbi mdbx.DBI

	// store is used to sync blocks to flat files.
	store *blockStore

	// The following fields are related to flushing the cache to persistent
	// storage.  Note that all flushing is performed in an opportunistic
	// fashion.  This means that it is only flushed during a transaction or
	// when the database cache is closed.
	//
	// maxSize is the maximum size threshold the cache can grow to before
	// it is flushed.
	//
	// flushInterval is the threshold interval of time that is allowed to
	// pass before the cache is flushed.
	//
	// lastFlush is the time the cache was last flushed.  It is used in
	// conjunction with the current time and the flush interval.
	//
	// NOTE: These flush related fields are protected by the database write
	// lock.
	maxSize       uint64
	flushInterval time.Duration
	lastFlush     time.Time

	// The following fields hold the keys that need to be stored or deleted
	// from the underlying database once the cache is full, enough time has
	// passed, or when the database is shutting down.  Note that these are
	// stored using immutable treaps to support O(1) MVCC snapshots against
	// the cached data.  The cacheLock is used to protect concurrent access
	// for cache updates and snapshots.
	cacheLock    sync.RWMutex
	cachedKeys   *treap.Immutable
	cachedRemove *treap.Immutable

	// closed records that the store has been closed.  Calls into the
	// engine after that point are undefined rather than erroring, so they
	// are refused here instead.
	//
	// NOTE: protected by the database write lock, like the flush fields.
	closed bool
}

// Snapshot returns a snapshot of the database cache and underlying database at
// a particular point in time.
//
// The snapshot must be released after use by calling Release.
func (c *dbCache) Snapshot() (*dbCacheSnapshot, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}

	dbTx, txErr := c.env.BeginTxn(nil, mdbx.Readonly)
	if txErr != nil {
		return nil, convertErr("failed to open metadata snapshot", txErr)
	}

	// Since the cached keys to be added and removed use an immutable treap,
	// a snapshot is simply obtaining the root of the tree under the lock
	// which is used to atomically swap the root.
	c.cacheLock.RLock()
	cacheSnapshot := &dbCacheSnapshot{
		dbTx:          dbTx,
		dbi:           c.dbi,
		pendingKeys:   c.cachedKeys,
		pendingRemove: c.cachedRemove,
	}
	c.cacheLock.RUnlock()
	return cacheSnapshot, nil
}

// updateDB invokes the passed function against a write batch.  Any error the
// user-supplied function returns discards the batch and is returned from this
// function.  Otherwise the batch is committed durably, so a nil return means
// every mutation it carried reached disk.
func (c *dbCache) updateDB(fn func(tx *mdbx.Txn) error) error {
	if err := c.checkClosed(); err != nil {
		return err
	}

	tx, txErr := c.env.BeginTxn(nil, 0)
	if txErr != nil {
		return convertErr("failed to open metadata transaction", txErr)
	}

	if err := fn(tx); err != nil {
		tx.Abort()

		return err
	}

	if _, commitErr := tx.Commit(); commitErr != nil {
		return convertErr("failed to commit metadata transaction",
			commitErr)
	}
	return nil
}

// TreapForEacher is an interface which allows iteration of a treap in ascending
// order using a user-supplied callback for each key/value pair.  It mainly
// exists so both mutable and immutable treaps can be atomically committed to
// the database with the same function.
type TreapForEacher interface {
	ForEach(func(k, v []byte) bool)
}

// commitTreaps atomically commits all of the passed pending add/update/remove
// updates to the underlying database.
func (c *dbCache) commitTreaps(pendingKeys, pendingRemove TreapForEacher) error {
	// Perform all metadata updates in one atomic batch.
	return c.updateDB(func(tx *mdbx.Txn) error {
		var innerErr error
		pendingKeys.ForEach(func(k, v []byte) bool {
			if dbErr := tx.Put(c.dbi, k, v, 0); dbErr != nil {
				str := fmt.Sprintf("failed to put key %q to "+
					"metadata batch", k)
				innerErr = convertErr(str, dbErr)
				return false
			}
			return true
		})
		if innerErr != nil {
			return innerErr
		}

		pendingRemove.ForEach(func(k, v []byte) bool {
			dbErr := tx.Del(c.dbi, k, nil)
			if dbErr != nil && !mdbx.IsNotFound(dbErr) {
				str := fmt.Sprintf("failed to delete "+
					"key %q from metadata batch",
					k)
				innerErr = convertErr(str, dbErr)
				return false
			}
			return true
		})
		return innerErr
	})
}

// flush flushes the database cache to persistent storage.  This involes syncing
// the block store and replaying all transactions that have been applied to the
// cache to the underlying database.
//
// This function MUST be called with the database write lock held.
func (c *dbCache) flush() error {
	c.lastFlush = time.Now()

	// Sync the current write file associated with the block store.  This is
	// necessary before writing the metadata to prevent the case where the
	// metadata contains information about a block which actually hasn't
	// been written yet in unexpected shutdown scenarios.
	if err := c.store.syncBlocks(); err != nil {
		return err
	}

	// Since the cached keys to be added and removed use an immutable treap,
	// a snapshot is simply obtaining the root of the tree under the lock
	// which is used to atomically swap the root.
	c.cacheLock.RLock()
	cachedKeys := c.cachedKeys
	cachedRemove := c.cachedRemove
	c.cacheLock.RUnlock()

	// Nothing to do if there is no data to flush.
	if cachedKeys.Len() == 0 && cachedRemove.Len() == 0 {
		return nil
	}

	// Perform all metadata updates in one atomic batch.
	if err := c.commitTreaps(cachedKeys, cachedRemove); err != nil {
		if errors.Is(err, syscall.ENOSPC) {
			log.Errorf("%v. Cannot save any more blocks "+
				"due to the disk being full "+
				"-- exiting", err)
			os.Exit(1)
		}
		return err
	}

	// Clear the cache since it has been flushed.
	c.cacheLock.Lock()
	c.cachedKeys = treap.NewImmutable()
	c.cachedRemove = treap.NewImmutable()
	c.cacheLock.Unlock()

	return nil
}

// needsFlush returns whether or not the database cache needs to be flushed to
// persistent storage based on its current size, whether or not adding all of
// the entries in the passed database transaction would cause it to exceed the
// configured limit, and how much time has elapsed since the last time the cache
// was flushed.
//
// This function MUST be called with the database write lock held.
func (c *dbCache) needsFlush(tx *transaction) bool {
	// A flush is needed when more time has elapsed than the configured
	// flush interval.
	if time.Since(c.lastFlush) > c.flushInterval {
		return true
	}

	// A flush is needed when the size of the database cache exceeds the
	// specified max cache size.  The total calculated size is multiplied by
	// 1.5 here to account for additional memory consumption that will be
	// needed during the flush as well as old nodes in the cache that are
	// referenced by the snapshot used by the transaction.
	snap := tx.snapshot
	totalSize := snap.pendingKeys.Size() + snap.pendingRemove.Size()
	totalSize = uint64(float64(totalSize) * 1.5)
	return totalSize > c.maxSize
}

// commitTx atomically adds all of the pending keys to add and remove into the
// database cache.  When adding the pending keys would cause the size of the
// cache to exceed the max cache size, or the time since the last flush exceeds
// the configured flush interval, the cache will be flushed to the underlying
// persistent database.
//
// This is an atomic operation with respect to the cache in that either all of
// the pending keys to add and remove in the transaction will be applied or none
// of them will.
//
// The database cache itself might be flushed to the underlying persistent
// database even if the transaction fails to apply, but it will only be the
// state of the cache without the transaction applied.
//
// This function MUST be called during a database write transaction which in
// turn implies the database write lock will be held.
func (c *dbCache) commitTx(tx *transaction) error {
	// Flush the cache and write the current transaction directly to the
	// database if a flush is needed.
	if c.needsFlush(tx) {
		if err := c.flush(); err != nil {
			if errors.Is(err, syscall.ENOSPC) {
				log.Errorf("%v. Cannot save any more blocks "+
					"due to the disk being full "+
					"-- exiting", err)
				os.Exit(1)
			}
			return err
		}

		// Perform all metadata updates in one atomic batch.
		err := c.commitTreaps(tx.pendingKeys, tx.pendingRemove)
		if err != nil {
			return err
		}

		// Clear the transaction entries since they have been committed.
		tx.pendingKeys = nil
		tx.pendingRemove = nil
		return nil
	}

	// At this point a database flush is not needed, so atomically commit
	// the transaction to the cache.

	// Since the cached keys to be added and removed use an immutable treap,
	// a snapshot is simply obtaining the root of the tree under the lock
	// which is used to atomically swap the root.
	c.cacheLock.RLock()
	newCachedKeys := c.cachedKeys
	newCachedRemove := c.cachedRemove
	c.cacheLock.RUnlock()

	// Apply every key to add in the database transaction to the cache.
	pendingKVs := make([]treap.KVPair, 0, tx.pendingKeys.Len())
	tx.pendingKeys.ForEach(func(k, v []byte) bool {
		pendingKVs = append(pendingKVs, treap.KVPair{Key: k, Value: v})

		newCachedRemove = newCachedRemove.Delete(k)
		return true
	})
	newCachedKeys = newCachedKeys.Put(pendingKVs...)
	tx.pendingKeys = nil

	// Apply every key to remove in the database transaction to the cache.
	pendingRemoveKVs := make([]treap.KVPair, 0, tx.pendingRemove.Len())
	tx.pendingRemove.ForEach(func(k, v []byte) bool {
		pendingRemoveKVs = append(pendingRemoveKVs, treap.KVPair{Key: k, Value: v})

		newCachedKeys = newCachedKeys.Delete(k)
		return true
	})
	newCachedRemove = newCachedRemove.Put(pendingRemoveKVs...)
	tx.pendingRemove = nil

	// Atomically replace the immutable treaps which hold the cached keys to
	// add and delete.
	c.cacheLock.Lock()
	c.cachedKeys = newCachedKeys
	c.cachedRemove = newCachedRemove
	c.cacheLock.Unlock()
	return nil
}

// Close cleanly shuts down the database cache by syncing all data and closing
// the underlying metadata store.
//
// This function MUST be called with the database write lock held.
func (c *dbCache) Close() error {
	if err := c.checkClosed(); err != nil {
		return err
	}

	// Flush any outstanding cached entries to disk.
	if err := c.flush(); err != nil {
		// Even if there is an error while flushing, attempt to close
		// the underlying database.  The error is ignored since it would
		// mask the flush error.
		c.env.Close()
		c.closed = true
		return err
	}

	// Close the underlying metadata store.
	c.env.Close()
	c.closed = true

	return nil
}

// newDbCache returns a new database cache instance backed by the provided
// metadata store.  The cache is flushed to it when the max size exceeds the
// provided value or it has been longer than the provided interval since the
// last flush.
func newDbCache(env *mdbx.Env, dbi mdbx.DBI, store *blockStore,
	maxSize uint64, flushIntervalSecs uint32) *dbCache {

	return &dbCache{
		env:           env,
		dbi:           dbi,
		store:         store,
		maxSize:       maxSize,
		flushInterval: time.Second * time.Duration(flushIntervalSecs),
		lastFlush:     time.Now(),
		cachedKeys:    treap.NewImmutable(),
		cachedRemove:  treap.NewImmutable(),
	}
}
