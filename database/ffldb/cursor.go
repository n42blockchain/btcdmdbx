// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ffldb

import (
	"encoding/binary"

	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/wire"
	"github.com/ledgerwatch/erigon-lib/kv"
)

const (
	// blockHdrSize is the size of a block header.  This is simply the
	// constant from wire and is only provided here for convenience since
	// wire.MaxBlockHeaderPayload is quite long.
	blockHdrSize = wire.MaxBlockHeaderPayload

	// blockHdrOffset defines the offsets into a block index row for the
	// block header.
	//
	// The serialized block index row format is:
	//   <blocklocation><blockheader>
	// blockHdrOffset = blockLocSize
)

var (
	// byteOrder is the preferred byte order used through the database and
	// block files.  Sometimes big endian will be used to allow ordered byte
	// sortable integer values.
	byteOrder = binary.LittleEndian

	// bucketIndexPrefix is the prefix used for all entries in the bucket
	// index.
	// bucketIndexPrefix = []byte("bidx")

	// curBucketIDKeyName is the name of the key used to keep track of the
	// current bucket ID counter.
	curBucketIDKeyName = []byte("bidx-cbid")

	// metadataBucketID is the ID of the top-level metadata bucket.
	// It is the value 0 encoded as an unsigned big-endian uint32.
	// metadataBucketID = []byte{0x00, 0x00, 0x00, 0x00}

	// blockIdxBucketID is the ID of the internal block metadata bucket.
	// It is the value 1 encoded as an unsigned big-endian uint32.
	// blockIdxBucketID = []byte{0x00, 0x00, 0x00, 0x01}

	// blockIdxBucketName is the bucket used internally to track block
	// metadata.
	blockIdxBucketName = []byte("ffldb-blockidx")

	// writeLocKeyName is the key used to store the current write file
	// location.
	writeLocKeyName = []byte("ffldb-writeloc")
)

// Common error strings.
const (
	// errDbNotOpenStr is the text to use for the database.ErrDbNotOpen
	// error code.
	errDbNotOpenStr = "database is not open"

	// errTxClosedStr is the text to use for the database.ErrTxClosed error
	// code.
	errTxClosedStr = "database tx is closed"
)

// makeDbErr creates a database.Error given a set of arguments.
func makeDbErr(c database.ErrorCode, desc string, err error) database.Error {
	return database.Error{ErrorCode: c, Description: desc, Err: err}
}

// convertErr converts the passed leveldb error into a database error with an
// equivalent error code  and the passed description.  It also sets the passed
// error as the underlying error.
func convertErr(desc string, ldbErr error) database.Error {
	// Use the driver-specific error code by default.  The code below will
	// update this with the converted error if it's recognized.
	var code = database.ErrDriverSpecific

	return database.Error{ErrorCode: code, Description: desc, Err: ldbErr}
}

// copySlice returns a copy of the passed slice.  This is mostly used to copy
// leveldb iterator keys and values since they are only valid until the iterator
// is moved instead of during the entirety of the transaction.
func copySlice(slice []byte) []byte {
	if len(slice) < 1 {
		return nil
	}

	ret := make([]byte, len(slice))
	copy(ret, slice)
	return ret
}

// cursor is an internal type used to represent a cursor over key/value pairs
// and nested buckets of a bucket and implements the database.Cursor interface.
type cursor struct {
	bucket        *bucket
	rwCursor      kv.RwCursor // read-write cursor
	roCursor      kv.Cursor   // read-only cursor
	currentCursor kv.Cursor   // current cursor
	key, value    []byte      // current key & value
}

// Enforce cursor implements the database.Cursor interface.
var _ database.Cursor = (*cursor)(nil)

// Bucket returns the bucket the cursor was created for.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Bucket() database.Bucket {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}

	return c.bucket
}

// Delete removes the current key/value pair the cursor is at without
// invalidating the cursor.
//
// Returns the following errors as required by the interface contract:
//   - ErrIncompatibleValue if attempted when the cursor points to a nested
//     bucket
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Delete() error {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return err
	}
	if !c.bucket.tx.writable || (c.rwCursor == nil) {
		str := "can't delete anything on read only cursor"
		return makeDbErr(database.ErrIncompatibleValue, str, nil)
	}

	return c.rwCursor.DeleteCurrent()
}

// First positions the cursor at the first key/value pair and returns whether or
// not the pair exists.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) First() bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	if c.currentCursor == nil {
		return false
	}

	key, val, err := c.currentCursor.First()
	if (key != nil) && (err == nil) {
		c.key = copySlice(key)
		c.value = copySlice(val)
	} else {
		c.key = nil
		c.value = nil
	}
	return (key != nil) && (err == nil)
}

// Last positions the cursor at the last key/value pair and returns whether or
// not the pair exists.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Last() bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	if c.currentCursor == nil {
		return false
	}

	key, val, err := c.currentCursor.Last()
	if (key != nil) && (err == nil) {
		c.key = copySlice(key)
		c.value = copySlice(val)
	} else {
		c.key = nil
		c.value = nil
	}
	return (key != nil) && (err == nil)
}

// Next moves the cursor one key/value pair forward and returns whether or not
// the pair exists.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Next() bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	if c.currentCursor == nil {
		return false
	}

	key, val, err := c.currentCursor.Next()
	if (key != nil) && (err == nil) {
		c.key = copySlice(key)
		c.value = copySlice(val)
	} else {
		c.key = nil
		c.value = nil
	}
	return (key != nil) && (err == nil)
}

// Prev moves the cursor one key/value pair backward and returns whether or not
// the pair exists.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Prev() bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	// Nothing to return if cursor is exhausted.
	if c.currentCursor == nil {
		return false
	}

	key, val, err := c.currentCursor.Prev()
	if (key != nil) && (err == nil) {
		c.key = copySlice(key)
		c.value = copySlice(val)
	} else {
		c.key = nil
		c.value = nil
	}
	return (key != nil) && (err == nil)
}

// Seek positions the cursor at the first key/value pair that is greater than or
// equal to the passed seek key.  Returns false if no suitable key was found.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Seek(seek []byte) bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	if (c.currentCursor == nil) || (len(seek) < 1) {
		return false
	}

	key, val, err := c.currentCursor.Seek(seek)
	if (key != nil) && (err == nil) {
		c.key = copySlice(key)
		c.value = copySlice(val)
	} else {
		c.key = nil
		c.value = nil
	}
	return (key != nil) && (err == nil)
}

// Key returns the current key the cursor is pointing to.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Key() []byte {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}

	return c.key
}

// Value returns the current value the cursor is pointing to.  This will be nil
// for nested buckets.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Value() []byte {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}

	return c.value
}

// cursorFinalizer is either invoked when a cursor is being garbage collected or
// called manually to ensure the underlying cursor iterators are released.
func cursorFinalizer(c *cursor) {
	if c == nil {
		return
	}
	if c.currentCursor != nil {
		c.currentCursor.Close()
	}
	c.currentCursor = nil
	c.rwCursor = nil
	c.roCursor = nil
}

// newCursor returns a new cursor for the given bucket, bucket ID, and cursor
// type.
//
// NOTE: The caller is responsible for calling the cursorFinalizer function on
// the returned cursor.
func newCursor(b *bucket) *cursor {
	if b.tx.mdbRwTx != nil {
		csr, err := b.tx.mdbRwTx.RwCursor(b.name)
		if err != nil {
			return nil
		}

		return &cursor{
			bucket:        b,
			currentCursor: csr,
			rwCursor:      csr,
			roCursor:      nil,
		}
	}

	if b.tx.mdbRoTx != nil {
		csr, err := b.tx.mdbRoTx.Cursor(b.name)
		if err != nil {
			return nil
		}

		return &cursor{
			bucket:        b,
			currentCursor: csr,
			rwCursor:      nil,
			roCursor:      csr,
		}
	}

	return nil
}
