// Copyright (c) 2015 The Decred developers
// Copyright (c) 2016-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package chainhash

import (
	"hash"
	"sync"
	"crypto/sha256"
	"io"
)

// HashB calculates hash(b) and returns the resulting bytes.
func HashB(b []byte) []byte {
	hash := sha256.Sum256(b)
	return hash[:]
}

// HashH calculates hash(b) and returns the resulting bytes as a Hash.
func HashH(b []byte) Hash {
	return Hash(sha256.Sum256(b))
}

// DoubleHashB calculates hash(hash(b)) and returns the resulting bytes.
func DoubleHashB(b []byte) []byte {
	first := sha256.Sum256(b)
	second := sha256.Sum256(first[:])
	return second[:]
}

// DoubleHashH calculates hash(hash(b)) and returns the resulting bytes as a
// Hash.
func DoubleHashH(b []byte) Hash {
	first := sha256.Sum256(b)
	return Hash(sha256.Sum256(first[:]))
}

// hasherPool recycles SHA-256 states.  sha256.New returns a heap-allocated
// digest, and the sighash paths call DoubleHashRaw once per signature from
// every validation goroutine at once; allocating a fresh digest each time
// put the allocator's locks on the signature-verification critical path.
var hasherPool = sync.Pool{
	New: func() interface{} {
		return &pooledHasher{h: sha256.New()}
	},
}

// pooledHasher pairs a digest with scratch space that already lives on the
// heap.  Summing into a stack array would force that array to the heap on
// every call, because the slice crosses an interface boundary; summing into
// the pooled object's own scratch allocates nothing in steady state.
type pooledHasher struct {
	h       hash.Hash
	scratch [HashSize]byte
}

// DoubleHashRaw calculates hash(hash(b)) where b is the output of the passed
// serialize function, and returns the resulting bytes as a Hash.  The
// serialized data is fed straight into the digest, so it is never
// materialised as a whole.
func DoubleHashRaw(serialize func(w io.Writer) error) Hash {
	p := hasherPool.Get().(*pooledHasher)
	h := p.h
	h.Reset()
	_ = serialize(h)

	h.Sum(p.scratch[:0])
	h.Reset()
	h.Write(p.scratch[:])
	h.Sum(p.scratch[:0])

	res := Hash(p.scratch)
	hasherPool.Put(p)

	return res
}
