package main

import (
	bolt "go.etcd.io/bbolt"
)

// boltBucket is the single bucket the benchmark uses.
var boltBucket = []byte("utxo")

// boltEngine benchmarks bbolt, etcd's fork of Bolt.
//
// It is the closest pure-Go analogue to MDBX: a memory-mapped copy-on-write
// B+tree with one writer, many readers, and an fsync at commit. Including it
// separates "is a B-tree the right shape for this workload" from "is the cgo
// dependency worth it", which a leveldb-versus-MDBX comparison alone conflates.
type boltEngine struct {
	db  *bolt.DB
	dir string
}

func newBoltEngine() engine {
	return &boltEngine{}
}

func (e *boltEngine) name() string {
	return "bbolt"
}

func (e *boltEngine) open(dir string) error {
	db, err := bolt.Open(dir+"/utxo.db", 0644, nil)
	if err != nil {
		return err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(boltBucket)

		return err
	})
	if err != nil {
		db.Close()

		return err
	}
	e.db = db
	e.dir = dir

	return nil
}

func (e *boltEngine) close() error {
	if e.db == nil {
		return nil
	}

	err := e.db.Close()
	e.db = nil

	return err
}

func (e *boltEngine) commit(puts [][2][]byte, dels [][]byte) error {
	return e.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(boltBucket)
		for _, kv := range puts {
			if err := bucket.Put(kv[0], kv[1]); err != nil {
				return err
			}
		}
		for _, key := range dels {
			if err := bucket.Delete(key); err != nil {
				return err
			}
		}

		return nil
	})
}

// boltView reads through one read transaction.
type boltView struct {
	bucket *bolt.Bucket
}

func (v *boltView) get(key []byte) ([]byte, error) {
	value := v.bucket.Get(key)
	if value == nil {
		return nil, nil
	}

	// The slice is only valid for the life of the transaction.
	result := make([]byte, len(value))
	copy(result, value)

	return result, nil
}

func (e *boltEngine) view(fn func(g getter) error) error {
	return e.db.View(func(tx *bolt.Tx) error {
		return fn(&boltView{bucket: tx.Bucket(boltBucket)})
	})
}

// settle is a no-op. bbolt writes its tree in place at commit time.
func (e *boltEngine) settle() error {
	return nil
}

func (e *boltEngine) iterate() (int, error) {
	count := 0
	err := e.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(boltBucket).Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if len(k) > 0 && len(v) >= 0 {
				count++
			}
		}

		return nil
	})

	return count, err
}

// usedBytes reports the live tree size from bbolt's own accounting rather than
// the file size, since bbolt also keeps freed pages in the file.
func (e *boltEngine) usedBytes() (int64, error) {
	var used int64
	err := e.db.View(func(tx *bolt.Tx) error {
		stats := tx.Bucket(boltBucket).Stats()
		used = int64(stats.LeafInuse + stats.BranchInuse)

		return nil
	})

	return used, err
}
