package main

import (
	"github.com/cockroachdb/pebble"
)

// pebbleEngine benchmarks Pebble, the LSM CockroachDB maintains and the engine
// go-ethereum now defaults to.
//
// It is the most direct comparison available for leveldb: the same storage
// family, a decade newer, and pure Go, so adopting it would not put cgo on the
// build the way MDBX does.
type pebbleEngine struct {
	db  *pebble.DB
	dir string
}

func newPebbleEngine() engine {
	return &pebbleEngine{}
}

func (e *pebbleEngine) name() string {
	return "pebble"
}

func (e *pebbleEngine) open(dir string) error {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return err
	}
	e.db = db
	e.dir = dir

	return nil
}

func (e *pebbleEngine) close() error {
	if e.db == nil {
		return nil
	}

	err := e.db.Close()
	e.db = nil

	return err
}

func (e *pebbleEngine) commit(puts [][2][]byte, dels [][]byte) error {
	batch := e.db.NewBatch()
	defer batch.Close()

	for _, kv := range puts {
		if err := batch.Set(kv[0], kv[1], nil); err != nil {
			return err
		}
	}
	for _, key := range dels {
		if err := batch.Delete(key, nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

// pebbleView reads through a snapshot.
type pebbleView struct {
	snapshot *pebble.Snapshot
}

func (v *pebbleView) get(key []byte) ([]byte, error) {
	value, closer, err := v.snapshot.Get(key)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	// The returned slice is only valid until closer runs.
	result := make([]byte, len(value))
	copy(result, value)

	return result, nil
}

func (e *pebbleEngine) view(fn func(g getter) error) error {
	snapshot := e.db.NewSnapshot()
	defer snapshot.Close()

	return fn(&pebbleView{snapshot: snapshot})
}

// settle flushes the memtable and waits for compactions to drain.
func (e *pebbleEngine) settle() error {
	if err := e.db.Flush(); err != nil {
		return err
	}

	return e.db.Compact([]byte{}, []byte{0xff}, true)
}

func (e *pebbleEngine) iterate() (int, error) {
	iter, err := e.db.NewIter(nil)
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		if len(iter.Key()) > 0 && len(iter.Value()) >= 0 {
			count++
		}
	}

	return count, iter.Error()
}

func (e *pebbleEngine) usedBytes() (int64, error) {
	return dirSize(e.dir)
}
