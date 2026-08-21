// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package txscript

import (
	"bytes"
	"sync"
	"sync/atomic"

	"github.com/btcsuite/btcd/chainhash/v2"
)

// sigCacheEntry represents an entry in the SigCache. Entries within the
// SigCache are keyed according to the sigHash of the signature. In the
// scenario of a cache-hit (according to the sigHash), an additional comparison
// of the signature, and public key will be executed in order to ensure a complete
// match. In the occasion that two sigHashes collide, the newer sigHash will
// simply overwrite the existing entry.
type sigCacheEntry struct {
	sig    []byte
	pubKey []byte
}

// SigCache implements an Schnorr+ECDSA signature verification cache with a
// randomized entry eviction policy. Only valid signatures will be added to the
// cache. The benefits of SigCache are two fold. Firstly, usage of SigCache
// mitigates a DoS attack wherein an attack causes a victim's client to hang
// due to worst-case behavior triggered while processing attacker crafted
// invalid transactions. A detailed description of the mitigated DoS attack can
// be found here:
// https://bitslog.wordpress.com/2013/01/23/fixed-bitcoin-vulnerability-explanation-why-the-signature-cache-is-a-dos-protection/.
// Secondly, usage of the SigCache introduces a signature verification
// optimization which speeds up the validation of transactions within a block,
// if they've already been seen and verified within the mempool.
//
// TODO(roasbeef): use type params here after Go 1.18
// sigCacheShards is the number of independently locked partitions the cache
// is split across.  Every signature a syncing node verifies is new, so every
// verification ends in an Add that takes the exclusive lock; with one lock
// over the whole cache that serialises all validation goroutines on a map
// insert.  Routing by the leading byte of the sighash spreads them out.  Must
// be a power of two.
const sigCacheShards = 64

// sigCacheShard is one partition of the cache with its own lock.  The padding
// keeps neighbouring locks off a shared cache line.
type sigCacheShard struct {
	sync.RWMutex
	validSigs map[chainhash.Hash]sigCacheEntry
	_         [32]byte
}

// SigCache implements an ECDSA signature verification cache with a randomized
// entry eviction policy. Only valid signatures will be added to the cache. The
// benefits of SigCache are two fold. Firstly, usage of SigCache mitigates a DoS
// attack wherein an attack causes a victim's client to hang due to worst-case
// behavior triggered while processing attacker crafted invalid transactions. A
// detailed description of the mitigated DoS attack can be found here:
// https://bitslog.wordpress.com/2013/01/23/fixed-bitcoin-vulnerability-explanation-why-the-signature-cache-is-a-dos-protection/.
// Secondly, usage of the SigCache introduces a signature verification
// optimization which speeds up the validation of transactions within a block,
// if they've already been seen and verified within the mempool.
type SigCache struct {
	shards [sigCacheShards]sigCacheShard

	// maxEntries bounds the whole cache and total tracks how full it is,
	// so capacity is exact regardless of how signatures spread across
	// the shards.  Concurrent adds may overshoot by at most one entry per
	// goroutine in flight.
	maxEntries uint
	total      atomic.Int64
}

// shard returns the partition a sighash routes to.
func (s *SigCache) shard(sigHash *chainhash.Hash) *sigCacheShard {
	return &s.shards[sigHash[0]&(sigCacheShards-1)]
}

// numEntries returns the number of cached signatures across every shard.
func (s *SigCache) numEntries() int {
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.RLock()
		n += len(sh.validSigs)
		sh.RUnlock()
	}

	return n
}

func NewSigCache(maxEntries uint) *SigCache {
	// The capacity is split evenly; a zero total keeps every shard at
	// zero so Add stays a no-op, as before.
	perShard := maxEntries / sigCacheShards
	if maxEntries > 0 && perShard == 0 {
		perShard = 1
	}

	cache := &SigCache{maxEntries: maxEntries}
	for i := range cache.shards {
		cache.shards[i].validSigs = make(
			map[chainhash.Hash]sigCacheEntry, perShard,
		)
	}

	return cache
}

// Exists returns true if an existing entry of 'sig' over 'sigHash' for public
// key 'pubKey' is found within the SigCache. Otherwise, false is returned.
//
// NOTE: This function is safe for concurrent access. Readers won't be blocked
// unless there exists a writer, adding an entry to the SigCache.
func (s *SigCache) Exists(sigHash chainhash.Hash, sig []byte, pubKey []byte) bool {
	sh := s.shard(&sigHash)
	sh.RLock()
	entry, ok := sh.validSigs[sigHash]
	sh.RUnlock()

	return ok && bytes.Equal(entry.pubKey, pubKey) && bytes.Equal(entry.sig, sig)
}

// Add adds an entry for a signature over 'sigHash' under public key 'pubKey'
// to the signature cache. In the event that the SigCache is 'full', an
// existing entry is randomly chosen to be evicted in order to make space for
// the new entry.
//
// NOTE: This function is safe for concurrent access. Writers will block
// simultaneous readers until function execution has concluded.
func (s *SigCache) Add(sigHash chainhash.Hash, sig []byte, pubKey []byte) {
	if s.maxEntries <= 0 {
		return
	}

	// Make room first, without holding the destination shard's lock, so
	// that evicting from a neighbouring shard can never deadlock against
	// another goroutine doing the same in the opposite direction.  The
	// victim is random within the shard, as it always was; the shard is
	// the first non-empty one starting from the destination, which is
	// random too since sighashes are uniformly distributed.
	if uint(s.total.Load()+1) > s.maxEntries {
		first := int(sigHash[0] & (sigCacheShards - 1))
		for i := 0; i < sigCacheShards; i++ {
			sh := &s.shards[(first+i)&(sigCacheShards-1)]
			sh.Lock()
			if len(sh.validSigs) > 0 {
				for victim := range sh.validSigs {
					delete(sh.validSigs, victim)

					break
				}
				s.total.Add(-1)
				sh.Unlock()

				break
			}
			sh.Unlock()
		}
	}

	sh := s.shard(&sigHash)
	sh.Lock()
	if _, exists := sh.validSigs[sigHash]; !exists {
		s.total.Add(1)
	}
	sh.validSigs[sigHash] = sigCacheEntry{sig, pubKey}
	sh.Unlock()
}
