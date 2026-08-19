package main

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// levelDBEngine benchmarks the store btcd uses today for chain metadata and
// the UTXO set, at the same version the main module pins.
type levelDBEngine struct {
	db  *leveldb.DB
	dir string
}

func newLevelDBEngine() engine {
	return &levelDBEngine{}
}

func (e *levelDBEngine) name() string {
	return "leveldb"
}

func (e *levelDBEngine) open(dir string) error {
	// These options mirror database/ffldb's metadata store rather than
	// goleveldb's defaults, so the comparison is against what btcd
	// actually runs.
	options := &opt.Options{
		Compression:  opt.NoCompression,
		ErrorIfExist: false,
		Strict:       opt.DefaultStrict,
	}

	db, err := leveldb.OpenFile(dir, options)
	if err != nil {
		return err
	}
	e.db = db
	e.dir = dir

	return nil
}

func (e *levelDBEngine) close() error {
	if e.db == nil {
		return nil
	}

	err := e.db.Close()
	e.db = nil

	return err
}

func (e *levelDBEngine) commit(puts [][2][]byte, dels [][]byte) error {
	batch := new(leveldb.Batch)
	for _, kv := range puts {
		batch.Put(kv[0], kv[1])
	}
	for _, key := range dels {
		batch.Delete(key)
	}

	// Sync is required: the engine contract is that a returned commit is
	// durable.
	return e.db.Write(batch, &opt.WriteOptions{Sync: true})
}

// levelDBView reads through a snapshot, which is goleveldb's equivalent of a
// read transaction.
type levelDBView struct {
	snapshot *leveldb.Snapshot
}

func (v *levelDBView) get(key []byte) ([]byte, error) {
	value, err := v.snapshot.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}

	return value, err
}

func (e *levelDBEngine) view(fn func(g getter) error) error {
	snapshot, err := e.db.GetSnapshot()
	if err != nil {
		return err
	}
	defer snapshot.Release()

	return fn(&levelDBView{snapshot: snapshot})
}

// settle runs a full compaction so the deferred merge cost of this run's
// writes is paid inside the measured window rather than after it.
func (e *levelDBEngine) settle() error {
	return e.db.CompactRange(util.Range{})
}

func (e *levelDBEngine) iterate() (int, error) {
	iter := e.db.NewIterator(&util.Range{}, nil)
	defer iter.Release()

	count := 0
	for iter.Next() {
		// Touch both key and value so the read is not optimized away.
		if len(iter.Key()) > 0 && len(iter.Value()) >= 0 {
			count++
		}
	}

	return count, iter.Error()
}

// usedBytes returns the directory size. goleveldb does not preallocate, so its
// files are already the space it consumes.
func (e *levelDBEngine) usedBytes() (int64, error) {
	return dirSize(e.dir)
}
