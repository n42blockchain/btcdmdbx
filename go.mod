module github.com/btcsuite/btcd

require (
	github.com/btcsuite/btcd/btcec/v2 v2.3.6
	github.com/btcsuite/btcd/btcutil v1.1.6
	github.com/btcsuite/btcd/chaincfg/chainhash v1.1.0
	github.com/btcsuite/btcd/v2transport v1.0.1
	github.com/btcsuite/btclog v1.0.0
	github.com/btcsuite/go-socks v0.0.0-20170105172521-4720035b7bfd
	github.com/btcsuite/websocket v0.0.0-20150119174127-31079b680792
	github.com/btcsuite/winsvc v1.0.0
	github.com/davecgh/go-spew v1.1.1
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.4.0
	github.com/decred/dcrd/lru v1.1.3
	github.com/gorilla/websocket v1.5.0
	github.com/jessevdk/go-flags v1.6.1
	github.com/jrick/logrotate v1.1.2
	github.com/ledgerwatch/erigon-lib v1.0.0
	github.com/ledgerwatch/log/v3 v3.9.0
	github.com/stretchr/testify v1.11.1
	github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7
	golang.org/x/crypto v0.44.0
	golang.org/x/sys v0.38.0
	pgregory.net/rapid v1.2.0
)

require (
	github.com/VictoriaMetrics/metrics v1.40.2 // indirect
	github.com/aead/siphash v1.0.1 // indirect
	github.com/c2h5oh/datasize v0.0.0-20231215233829-aa82cc1e6500 // indirect
	github.com/decred/dcrd/crypto/blake256 v1.1.0 // indirect
	github.com/erigontech/mdbx-go v0.27.14 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/kkdai/bstream v1.0.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	golang.org/x/exp v0.0.0-20251113190631-e25ba8c21ef6 // indirect
	golang.org/x/sync v0.18.0 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// The retract statements below fixes an accidental push of the tags of a btcd
// fork.
retract (
	v0.18.1
	v0.18.0
	v0.17.1
	v0.17.0
	v0.16.5
	v0.16.4
	v0.16.3
	v0.16.2
	v0.16.1
	v0.16.0

	v0.15.2
	v0.15.1
	v0.15.0

	v0.14.7
	v0.14.6
	v0.14.6
	v0.14.5
	v0.14.4
	v0.14.3
	v0.14.2
	v0.14.1

	v0.14.0
	v0.13.0-beta2
	v0.13.0-beta
)

go 1.24.0
