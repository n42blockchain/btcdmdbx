// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ffldb

import (
	"github.com/btcsuite/btcd/database/internal/treap"
)

// pendingTreapIter wraps a treap iterator over a transaction's pending keys
// with the two methods the dbIterator contract needs beyond what the treap
// already provides.
type pendingTreapIter struct {
	*treap.Iterator
	tx       *transaction
	released bool
}

// Enforce pendingTreapIter implements dbIterator.
var _ dbIterator = (*pendingTreapIter)(nil)

// Error is only provided to satisfy the iterator interface as there are no
// errors for this memory-only structure.
//
// This is part of the dbIterator interface implementation.
func (iter *pendingTreapIter) Error() error {
	return nil
}

// Release releases the iterator by removing the underlying treap iterator from
// the list of active iterators against the pending keys treap.
//
// This is part of the dbIterator interface implementation.
func (iter *pendingTreapIter) Release() {
	if !iter.released {
		iter.tx.removeActiveIter(iter.Iterator)
		iter.released = true
	}
}

// newPendingTreapIter creates a new treap iterator for the given range against
// the pending keys for the passed transaction, and adds it to the
// transaction's list of active iterators so it can be invalidated if the
// pending keys change underneath it.
func newPendingTreapIter(tx *transaction, r *keyRange) *pendingTreapIter {
	iter := tx.pendingKeys.Iterator(r.start, r.limit)
	tx.addActiveIter(iter)
	return &pendingTreapIter{Iterator: iter, tx: tx}
}
