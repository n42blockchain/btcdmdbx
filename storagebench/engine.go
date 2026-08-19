package main

import (
	"os"
	"path/filepath"
)

// getter performs point lookups inside an already-open read view.
type getter interface {
	// get returns the value for key, or nil if it is absent. A miss is an
	// ordinary outcome for the tiered read path this benchmark informs, so
	// it is not an error.
	get(key []byte) ([]byte, error)
}

// engine is the storage backend under test.
//
// Every implementation must give the same durability guarantee: a returned
// commit is on disk. Comparing a durable engine against a buffered one
// measures nothing useful, so the sync semantics are part of the contract
// rather than a tuning knob.
type engine interface {
	// name identifies the engine in the results.
	name() string

	// open creates or opens the store in dir.
	open(dir string) error

	// close releases the store.
	close() error

	// commit applies every put and delete atomically and durably. This
	// models one checkpoint of block connection.
	commit(puts [][2][]byte, dels [][]byte) error

	// view runs fn inside a single read transaction.
	//
	// Lookups are batched through one view rather than issued
	// individually because that is what block connection does: a node
	// resolves every prevout in a block against one consistent view of the
	// chainstate. Charging an engine a transaction setup cost per lookup
	// would measure an access pattern btcd never produces.
	view(fn func(g getter) error) error

	// settle forces any deferred background work to complete.
	//
	// An LSM engine defers compaction, so a benchmark that stops the clock
	// at the last commit charges it for none of the merge work its writes
	// created. Calling this before measuring space and read performance
	// puts both engines in a comparable state: all writes durable and all
	// deferred reorganisation done.
	settle() error

	// iterate walks every record in key order and returns how many were
	// seen. This models snapshot export.
	iterate() (int, error)

	// usedBytes reports the bytes the engine actually occupies, which is
	// not the same as the file size for engines that preallocate.
	usedBytes() (int64, error)
}

// dirSize sums the on-disk footprint of a directory tree. This is the
// allocated size, which for a preallocating engine can be far larger than the
// data it holds.
func dirSize(dir string) (int64, error) {
	var total int64
	err := filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}

		return nil
	})

	return total, err
}
