// Copyright (c) 2023 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"bytes"
	"math/rand"
	"testing"
)

// TestSerializeSpendJournalParallel ensures the parallel journal encoder
// produces byte-identical output to the serial loop for journals large
// enough to take the parallel path, across varied script shapes and the
// boundary sizes around the cutover.
func TestSerializeSpendJournalParallel(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(1))

	makeStxos := func(n int) []SpentTxOut {
		stxos := make([]SpentTxOut, n)
		for i := range stxos {
			script := make([]byte, 1+rng.Intn(80))
			rng.Read(script)

			stxos[i] = SpentTxOut{
				Amount:     rng.Int63n(21e14),
				PkScript:   script,
				Height:     rng.Int31n(1000000),
				IsCoinBase: rng.Intn(50) == 0,
			}
		}

		return stxos
	}

	// serialOnly mirrors the serial branch of serializeSpendJournalEntry
	// so the comparison cannot accidentally test the parallel path
	// against itself.
	serialOnly := func(stxos []SpentTxOut) []byte {
		var size int
		for i := range stxos {
			size += spentTxOutSerializeSize(&stxos[i])
		}
		serialized := make([]byte, size)
		var offset int
		for i := len(stxos) - 1; i > -1; i-- {
			offset += putSpentTxOut(serialized[offset:], &stxos[i])
		}

		return serialized
	}

	for _, n := range []int{511, 512, 513, 1000, 4999} {
		stxos := makeStxos(n)

		want := serialOnly(stxos)
		got := serializeSpendJournalEntry(stxos)

		if !bytes.Equal(got, want) {
			t.Fatalf("n=%d: parallel journal encoding differs "+
				"from serial", n)
		}
	}
}
