// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ffldb

import (
	"errors"
	"runtime"

	"github.com/btcsuite/btcd/database"
	"github.com/ledgerwatch/erigon-lib/kv"
)

const (
	//
	// All bucket names definition
	// Warning:
	// DON't modify these name, they are defined in other files, you can get them by search
	//
	blockIndexBucketName   = "blockheaderidx"
	cfIndexParentBucketKey = "cfindexparentbucket"
	cfIndexKeys            = "cf0byhashidx"
	cfHashKeys             = "cf0hashbyhashidx"
	cfHeaderKeys           = "cf0headerbyhashidx"
	hashIndexBucketName    = "hashidx"
	heightIndexBucketName  = "heightidx"
	indexTipsBucketName    = "idxtips"
	spendJournalBucketName = "spendjournal"
	utxoSetBucketName      = "utxosetv2"

	// below 2 name definition inside ffldb package
	metadataBucketID = "metadata"
	blockIdxBucketID = "blockidx"
)

// bucket is an internal type used to represent a collection of key/value pairs
// and implements the database.Bucket interface.
type bucket struct {
	name string // bucket name for mdbx
	tx   *transaction
}

// Enforce bucket implements the database.Bucket interface.
var _ database.Bucket = (*bucket)(nil)

// Bucket retrieves a nested bucket with the given key.  Returns nil if
// the bucket does not exist.
//
// This function is part of the database.Bucket interface implementation.
func (b *bucket) Bucket(key []byte) database.Bucket {
	return &bucket{tx: b.tx, name: string(key)}

}

// CreateBucket creates and returns a new nested bucket with the given key.
// This function is part of the database.Bucket interface implementation.
func (b *bucket) CreateBucket(key []byte) (database.Bucket, error) {
	return &bucket{tx: b.tx, name: string(key)}, nil
}

// CreateBucketIfNotExists creates and returns a new nested bucket with the
// given key if it does not already exist.
// This function is part of the database.Bucket interface implementation.
func (b *bucket) CreateBucketIfNotExists(key []byte) (database.Bucket, error) {
	return b.CreateBucket(key)
}

// DeleteBucket removes a nested bucket with the given key.
// This function is part of the database.Bucket interface implementation.
func (b *bucket) DeleteBucket(key []byte) error {
	// Ensure transaction state is valid.
	err := b.tx.checkClosed()
	if (err != nil) || (b.tx.mdbRwTx == nil) {
		return errors.New("read-only state")
	}

	return nil
}

// Cursor returns a new cursor, allowing for iteration over the bucket's
// key/value pairs and nested buckets in forward or backward order.
//
// You must seek to a position using the First, Last, or Seek functions before
// calling the Next, Prev, Key, or Value functions.  Failure to do so will
// result in the same return values as an exhausted cursor, which is false for
// the Prev and Next functions and nil for Key and Value functions.
//
// This function is part of the database.Bucket interface implementation.
func (b *bucket) Cursor() database.Cursor {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return &cursor{bucket: b}
	}

	// Create the cursor and setup a runtime finalizer to ensure the
	// iterators are released when the cursor is garbage collected.
	c := newCursor(b)
	runtime.SetFinalizer(c, cursorFinalizer)
	return c
}

// ForEach invokes the passed function with every key/value pair in the bucket.
// This does not include nested buckets or the key/value pairs within those
// nested buckets.
//
// WARNING: It is not safe to mutate data while iterating with this method.
// Doing so may cause the underlying cursor to be invalidated and return
// unexpected keys and/or values.
//
// Returns the following errors as required by the interface contract:
//   - ErrTxClosed if the transaction has already been closed
//
// NOTE: The values returned by this function are only valid during a
// transaction.  Attempting to access them after a transaction has ended will
// likely result in an access violation.
//
// This function is part of the database.Bucket interface implementation.
func (b *bucket) ForEach(fn func(k, v []byte) error) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	var cursor kv.Cursor
	var err error
	if b.tx.mdbRoTx != nil {
		cursor, err = b.tx.mdbRoTx.Cursor(b.name)
	}
	if err != nil {
		if b.tx.mdbRwTx != nil {
			cursor, err = b.tx.mdbRwTx.Cursor(b.name)
		}
	}
	if err != nil {
		return err
	}

	for k, v, err := cursor.First(); k != nil; k, v, err = cursor.Next() {
		if err != nil {
			return err
		}
		if err2 := fn(k, v); err2 != nil {
			return err2
		}
	}

	return nil
}

// ForEachBucket invokes the passed function with the key of every nested bucket
// in the current bucket.  This does not include any nested buckets within those
// nested buckets.
//
// WARNING: It is not safe to mutate data while iterating with this method.
// Doing so may cause the underlying cursor to be invalidated and return
// unexpected keys.
//
// Returns the following errors as required by the interface contract:
//   - ErrTxClosed if the transaction has already been closed
//
// NOTE: The values returned by this function are only valid during a
// transaction.  Attempting to access them after a transaction has ended will
// likely result in an access violation.
//
// This function is part of the database.Bucket interface implementation.
func (b *bucket) ForEachBucket(fn func(k []byte) error) error {
	return nil
}

// Writable returns whether or not the bucket is writable.
//
// This function is part of the database.Bucket interface implementation.
func (b *bucket) Writable() bool {
	return b.tx.writable
}

// Put saves the specified key/value pair to the bucket.  Keys that do not
// already exist are added and keys that already exist are overwritten.
//
// Returns the following errors as required by the interface contract:
//   - ErrKeyRequired if the key is empty
//   - ErrIncompatibleValue if the key is the same as an existing bucket
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the database.Bucket interface implementation.
func (b *bucket) Put(key, value []byte) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable || (b.tx.mdbRwTx == nil) {
		str := "setting a key requires a writable database transaction"
		return makeDbErr(database.ErrTxNotWritable, str, nil)
	}

	// Ensure a key was provided.
	if len(key) == 0 {
		str := "put requires a key"
		return makeDbErr(database.ErrKeyRequired, str, nil)
	}

	return b.tx.mdbRwTx.Put(b.name, key, value)
}

// Get returns the value for the given key.  Returns nil if the key does not
// exist in this bucket.  An empty slice is returned for keys that exist but
// have no value assigned.
//
// NOTE: The value returned by this function is only valid during a transaction.
// Attempting to access it after a transaction has ended results in undefined
// behavior.  Additionally, the value must NOT be modified by the caller.
//
// This function is part of the database.Bucket interface implementation.
func (b *bucket) Get(key []byte) []byte {
	// Ensure transaction state is valid.
	err := b.tx.checkClosed()
	if err != nil {
		return nil
	}

	// Nothing to return if there is no key.
	if len(key) == 0 {
		return nil
	}

	var mdbtx kv.Tx = b.tx.mdbRwTx
	if b.tx.mdbRoTx != nil {
		mdbtx = b.tx.mdbRoTx
	}
	if mdbtx != nil {
		var val []byte
		val, err = mdbtx.GetOne(b.name, key)
		if err == nil {
			return copySlice(val)
		}
	}

	return nil
}

// Delete removes the specified key from the bucket.  Deleting a key that does
// not exist does not return an error.
//
// Returns the following errors as required by the interface contract:
//   - ErrKeyRequired if the key is empty
//   - ErrIncompatibleValue if the key is the same as an existing bucket
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the database.Bucket interface implementation.
func (b *bucket) Delete(key []byte) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable || (b.tx.mdbRwTx == nil) {
		str := "deleting a value requires a writable database transaction"
		return makeDbErr(database.ErrTxNotWritable, str, nil)
	}

	// Nothing to do if there is no key.
	if len(key) == 0 {
		return nil
	}

	return b.tx.mdbRwTx.Delete(b.name, key)
}
