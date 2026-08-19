package main

import (
	badger "github.com/dgraph-io/badger/v4"
)

// badgerEngine benchmarks BadgerDB, a pure-Go LSM that stores values in a
// separate log and keeps only keys and pointers in the tree.
//
// That design targets large values, and UTXO records are small, so this is
// partly a test of whether value separation hurts when the value is barely
// larger than the pointer to it.
type badgerEngine struct {
	db  *badger.DB
	dir string
}

func newBadgerEngine() engine {
	return &badgerEngine{}
}

func (e *badgerEngine) name() string {
	return "badger"
}

func (e *badgerEngine) open(dir string) error {
	options := badger.DefaultOptions(dir).
		WithSyncWrites(true).
		WithLogger(nil)

	db, err := badger.Open(options)
	if err != nil {
		return err
	}
	e.db = db
	e.dir = dir

	return nil
}

func (e *badgerEngine) close() error {
	if e.db == nil {
		return nil
	}

	err := e.db.Close()
	e.db = nil

	return err
}

func (e *badgerEngine) commit(puts [][2][]byte, dels [][]byte) error {
	batch := e.db.NewWriteBatch()
	defer batch.Cancel()

	for _, kv := range puts {
		if err := batch.Set(kv[0], kv[1]); err != nil {
			return err
		}
	}
	for _, key := range dels {
		if err := batch.Delete(key); err != nil {
			return err
		}
	}

	return batch.Flush()
}

// badgerView reads through one transaction.
type badgerView struct {
	txn *badger.Txn
}

func (v *badgerView) get(key []byte) ([]byte, error) {
	item, err := v.txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return item.ValueCopy(nil)
}

func (e *badgerEngine) view(fn func(g getter) error) error {
	return e.db.View(func(txn *badger.Txn) error {
		return fn(&badgerView{txn: txn})
	})
}

// settle flattens the LSM so deferred compaction is charged in the measured
// window, then runs value-log garbage collection so reclaimable space is
// actually reclaimed before the size is recorded.
func (e *badgerEngine) settle() error {
	if err := e.db.Flatten(1); err != nil {
		return err
	}

	for {
		// RunValueLogGC returns ErrNoRewrite once there is nothing left
		// worth rewriting.
		if err := e.db.RunValueLogGC(0.5); err != nil {
			return nil
		}
	}
}

func (e *badgerEngine) iterate() (int, error) {
	count := 0
	err := e.db.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.PrefetchValues = false
		iter := txn.NewIterator(options)
		defer iter.Close()

		for iter.Rewind(); iter.Valid(); iter.Next() {
			if len(iter.Item().Key()) > 0 {
				count++
			}
		}

		return nil
	})

	return count, err
}

// usedBytes reports badger's own accounting of its LSM tree plus value log.
//
// The directory size is useless here: badger preallocates a memory-mapped
// value log far larger than the data it holds and truncates it on close, so
// measuring the directory while the store is open reports gigabytes for a
// handful of megabytes of records.
func (e *badgerEngine) usedBytes() (int64, error) {
	lsm, vlog := e.db.Size()

	return lsm + vlog, nil
}
