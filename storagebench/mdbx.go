package main

import (
	"github.com/erigontech/mdbx-go/mdbx"
)

const (
	// mdbxTable is the single named table the benchmark uses. The real
	// design splits the set into utxo_hot and utxo_cold, but tiering is a
	// policy question measured separately; the engine comparison must not
	// be confounded by it.
	mdbxTable = "utxo"

	// mdbxMapUpper caps the environment size. MDBX requires this to be
	// declared up front and cannot grow past it without a reopen, which is
	// one of the operational risks recorded in docs/storage_design.md.
	mdbxMapUpper = 32 * 1024 * 1024 * 1024

	// mdbxGrowthStep is how much the map grows at a time once the current
	// size is exhausted. Keep it small enough that a short benchmark run
	// is not dominated by one preallocation.
	mdbxGrowthStep = 16 * 1024 * 1024

	// mdbxDefaultPageSize matches the page size the design assumes.
	mdbxDefaultPageSize = 4096
)

// mdbxPageSize is the page size the next opened environment will use. It is a
// variable so the benchmark can sweep it: page size trades per-page header
// overhead against internal fragmentation, and which way that lands depends on
// the record size, so it has to be measured rather than assumed.
var mdbxPageSize = mdbxDefaultPageSize

// mdbxEngine benchmarks MDBX through the binding Erigon maintains.
type mdbxEngine struct {
	env *mdbx.Env
	dbi mdbx.DBI
}

func newMDBXEngine() engine {
	return &mdbxEngine{}
}

func (e *mdbxEngine) name() string {
	return "mdbx"
}

func (e *mdbxEngine) open(dir string) error {
	env, err := mdbx.NewEnv(mdbx.Label("storagebench"))
	if err != nil {
		return err
	}

	if err := env.SetOption(mdbx.OptMaxDB, 4); err != nil {
		env.Close()

		return err
	}

	err = env.SetGeometry(
		-1, -1, mdbxMapUpper, mdbxGrowthStep, -1, mdbxPageSize,
	)
	if err != nil {
		env.Close()

		return err
	}

	// Durable is MDBX's fsync-on-commit mode, which is the guarantee the
	// engine contract requires. NoTLS lets any goroutine drive a read
	// transaction without being pinned to an OS thread.
	err = env.Open(dir, mdbx.Create|mdbx.NoTLS|mdbx.Durable, 0644)
	if err != nil {
		env.Close()

		return err
	}

	err = env.Update(func(txn *mdbx.Txn) error {
		dbi, err := txn.OpenDBISimple(mdbxTable, mdbx.Create)
		if err != nil {
			return err
		}
		e.dbi = dbi

		return nil
	})
	if err != nil {
		env.Close()

		return err
	}
	e.env = env

	return nil
}

func (e *mdbxEngine) close() error {
	if e.env == nil {
		return nil
	}

	e.env.Close()
	e.env = nil

	return nil
}

func (e *mdbxEngine) commit(puts [][2][]byte, dels [][]byte) error {
	return e.env.Update(func(txn *mdbx.Txn) error {
		for _, kv := range puts {
			err := txn.Put(e.dbi, kv[0], kv[1], 0)
			if err != nil {
				return err
			}
		}
		for _, key := range dels {
			err := txn.Del(e.dbi, key, nil)
			if err != nil && !mdbx.IsNotFound(err) {
				return err
			}
		}

		return nil
	})
}

// mdbxView reads through one MDBX read transaction.
type mdbxView struct {
	txn *mdbx.Txn
	dbi mdbx.DBI
}

func (v *mdbxView) get(key []byte) ([]byte, error) {
	value, err := v.txn.Get(v.dbi, key)
	if err != nil {
		if mdbx.IsNotFound(err) {
			return nil, nil
		}

		return nil, err
	}

	return value, nil
}

func (e *mdbxEngine) view(fn func(g getter) error) error {
	return e.env.View(func(txn *mdbx.Txn) error {
		return fn(&mdbxView{txn: txn, dbi: e.dbi})
	})
}

// settle is a no-op. MDBX writes its B-tree in place at commit time and has no
// deferred compaction, which is the other side of why its commits cost more.
func (e *mdbxEngine) settle() error {
	return nil
}

func (e *mdbxEngine) iterate() (int, error) {
	count := 0
	err := e.env.View(func(txn *mdbx.Txn) error {
		cursor, err := txn.OpenCursor(e.dbi)
		if err != nil {
			return err
		}
		defer cursor.Close()

		op := uint(mdbx.First)
		for {
			k, v, err := cursor.Get(nil, nil, op)
			if err != nil {
				if mdbx.IsNotFound(err) {
					return nil
				}

				return err
			}
			if len(k) > 0 && len(v) >= 0 {
				count++
			}
			op = mdbx.Next
		}
	})

	return count, err
}

// usedBytes reports the bytes the B-tree actually occupies: leaf, branch and
// overflow pages.
//
// This is deliberately not the file high-water mark. MDBX never returns pages
// to the filesystem, so last_pgno only ever grows and reports freed pages as
// still in use; the difference between this figure and the directory size is
// exactly the space a compaction would reclaim, which is worth seeing
// separately.
func (e *mdbxEngine) usedBytes() (int64, error) {
	var used int64
	err := e.env.View(func(txn *mdbx.Txn) error {
		stat, err := txn.StatDBI(e.dbi)
		if err != nil {
			return err
		}
		pages := stat.LeafPages + stat.BranchPages + stat.OverflowPages
		used = int64(pages) * int64(stat.PSize)

		return nil
	})

	return used, err
}
