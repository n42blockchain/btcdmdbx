// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ffldb

import (
	"errors"
	"os"

	"github.com/btcsuite/btcd/database"
	"github.com/erigontech/mdbx-go/mdbx"
)

// errStoreClosed is returned when the metadata store is used after it has been
// closed.
//
// The store is a C library reached through cgo, so calling into it after close
// is not a Go-level error but undefined behaviour. Every entry point checks a
// flag first and returns this instead, which keeps a use-after-close bug an
// ordinary error rather than a crash.
var errStoreClosed = errors.New("metadata store is closed")

// maxMetadataReaders bounds concurrent read transactions.
//
// Every cursor and snapshot in flight holds one. The default is small enough
// that a node serving several RPC clients while connecting a block can exhaust
// it, and running out is reported as an opaque failure, so it is raised here.
const maxMetadataReaders = 4096

// openMetadataStore opens, or creates, the metadata store at the given path
// and returns the environment along with the single table everything lives in.
func openMetadataStore(path string) (*mdbx.Env, mdbx.DBI, error) {
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, 0, convertErr("failed to create metadata "+
			"directory", err)
	}

	env, err := mdbx.NewEnv(mdbx.Label("ffldb"))
	if err != nil {
		return nil, 0, convertErr("failed to create metadata "+
			"environment", err)
	}

	if err := env.SetOption(mdbx.OptMaxDB, 4); err != nil {
		env.Close()

		return nil, 0, convertErr("failed to set metadata table "+
			"limit", err)
	}

	err = env.SetOption(mdbx.OptMaxReaders, maxMetadataReaders)
	if err != nil {
		env.Close()

		return nil, 0, convertErr("failed to set metadata reader "+
			"limit", err)
	}

	err = env.SetGeometry(-1, -1, metadataMapUpper, metadataGrowthStep, -1,
		metadataPageSize)
	if err != nil {
		env.Close()

		return nil, 0, convertErr("failed to set metadata geometry",
			err)
	}

	// Durable is the fsync-on-commit mode, which is what the database
	// interface promises. NoTLS lets a read transaction be used from any
	// goroutine rather than being pinned to the thread that opened it,
	// which the cursor and snapshot paths rely on.
	err = env.Open(path, mdbx.Create|mdbx.NoTLS|mdbx.Durable, 0700)
	if err != nil {
		env.Close()

		return nil, 0, convertErr("failed to open metadata store", err)
	}

	var dbi mdbx.DBI
	err = env.Update(func(tx *mdbx.Txn) error {
		var openErr error
		dbi, openErr = tx.OpenDBISimple(metadataTable, mdbx.Create)

		return openErr
	})
	if err != nil {
		env.Close()

		return nil, 0, convertErr("failed to open metadata table", err)
	}

	return env, dbi, nil
}

// checkClosed returns an error when the store has been closed.
func (c *dbCache) checkClosed() error {
	if c.closed {
		return makeDbErr(database.ErrDbNotOpen, errStoreClosed.Error(),
			errStoreClosed)
	}

	return nil
}
