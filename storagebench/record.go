package main

// UTXO record encoding matching btcd's on-disk format closely enough for the
// record sizes to be representative.
//
// btcd serializes a UTXO entry (blockchain/chainio.go) as a VLQ header code
// packing the creation height and the coinbase flag, followed by a compressed
// txout: a VLQ-transformed amount and a script compressed to a one-byte class
// tag plus its 20- or 32-byte payload for the standard templates.
//
// This file reimplements enough of that to produce records with the same size
// distribution. It is deliberately not a copy of the consensus code: the
// benchmark measures storage engines, and only the byte sizes and key layout
// need to be faithful.

// putVLQ serializes the provided number to a variable-length quantity
// following the Bitcoin Core convention: MSB-first, seven bits per byte, with
// the high bit set on every byte except the last, and each successive byte
// biased by one so the encoding is canonical.
func putVLQ(target []byte, n uint64) int {
	var result []byte
	for {
		high := byte(0x00)
		if len(result) > 0 {
			high = 0x80
		}
		result = append(result, byte(n&0x7f)|high)
		if n <= 0x7f {
			break
		}
		n = (n >> 7) - 1
	}

	// Reverse the bytes into the target, since they were generated in
	// reverse order.
	for i, j := 0, len(result)-1; j >= 0; i, j = i+1, j-1 {
		target[i] = result[j]
	}

	return len(result)
}

// serializeSizeVLQ returns the number of bytes putVLQ would write for n.
func serializeSizeVLQ(n uint64) int {
	size := 1
	for ; n > 0x7f; n = (n >> 7) - 1 {
		size++
	}

	return size
}

// compressTxOutAmount applies the same order-of-magnitude compression Bitcoin
// Core uses for output amounts, which makes round satoshi values markedly
// shorter under VLQ.
func compressTxOutAmount(amount uint64) uint64 {
	if amount == 0 {
		return 0
	}

	exponent := uint64(0)
	for amount%10 == 0 && exponent < 9 {
		amount /= 10
		exponent++
	}

	if exponent < 9 {
		lastDigit := amount % 10
		amount /= 10

		return 1 + 10*(9*amount+lastDigit-1) + exponent
	}

	return 10 + 10*(amount-1)
}

// scriptClass enumerates the output templates the workload generates, with the
// payload length each one contributes to a compressed record.
type scriptClass struct {
	tag         byte
	payloadLen  int
	shareInPct  int
	description string
}

// scriptClasses approximates the mix of output types on mainnet. The exact
// shares matter less than the resulting record-size distribution, which
// straddles the 20- and 32-byte payload boundary.
var scriptClasses = []scriptClass{
	{tag: 0x00, payloadLen: 20, shareInPct: 30, description: "p2pkh"},
	{tag: 0x01, payloadLen: 20, shareInPct: 15, description: "p2sh"},
	{tag: 0x02, payloadLen: 20, shareInPct: 35, description: "p2wpkh"},
	{tag: 0x03, payloadLen: 32, shareInPct: 5, description: "p2wsh"},
	{tag: 0x04, payloadLen: 32, shareInPct: 15, description: "p2tr"},
}

// pickScriptClass maps a value in [0,100) onto the class mix above.
func pickScriptClass(roll int) scriptClass {
	acc := 0
	for _, class := range scriptClasses {
		acc += class.shareInPct
		if roll < acc {
			return class
		}
	}

	return scriptClasses[len(scriptClasses)-1]
}

// encodeUtxo builds one serialized UTXO record. The payload bytes are filled
// from the supplied seed so records are deterministic but incompressible
// enough not to flatter any engine's internal compression.
func encodeUtxo(height uint32, isCoinbase bool, amount uint64,
	class scriptClass, seed uint64) []byte {

	headerCode := uint64(height) << 1
	if isCoinbase {
		headerCode |= 0x01
	}

	compressedAmount := compressTxOutAmount(amount)
	size := serializeSizeVLQ(headerCode) +
		serializeSizeVLQ(compressedAmount) + 1 + class.payloadLen

	record := make([]byte, size)
	offset := putVLQ(record, headerCode)
	offset += putVLQ(record[offset:], compressedAmount)
	record[offset] = class.tag
	offset++

	// Fill the payload with a cheap deterministic hash of the seed.
	state := seed
	for i := 0; i < class.payloadLen; i++ {
		state = state*6364136223846793005 + 1442695040888963407
		record[offset+i] = byte(state >> 33)
	}

	return record
}

// outpointKey builds the fixed 36-byte key proposed in docs/storage_design.md:
// the 32-byte txid in wire order followed by the output index as big-endian.
//
// Big-endian rather than little-endian so the key is lexicographically ordered
// by outpoint, which keeps every output of one transaction contiguous.
func outpointKey(txid [32]byte, vout uint32) [36]byte {
	var key [36]byte
	copy(key[:32], txid[:])
	key[32] = byte(vout >> 24)
	key[33] = byte(vout >> 16)
	key[34] = byte(vout >> 8)
	key[35] = byte(vout)

	return key
}

// deriveTxid produces a deterministic pseudo-txid for the given ordinal. The
// values are spread across the key space so insertion order bears no relation
// to key order, which is what a real chain looks like to the storage engine.
func deriveTxid(ordinal uint64) [32]byte {
	var txid [32]byte
	state := ordinal*0x9e3779b97f4a7c15 + 0x165667b19e3779f9
	for i := 0; i < 32; i += 8 {
		state ^= state >> 30
		state *= 0xbf58476d1ce4e5b9
		state ^= state >> 27
		state *= 0x94d049bb133111eb
		state ^= state >> 31
		for j := 0; j < 8; j++ {
			txid[i+j] = byte(state >> (8 * j))
		}
	}

	return txid
}
