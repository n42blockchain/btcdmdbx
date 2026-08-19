// Copyright (c) 2013-2017 The btcsuite developers
// Copyright (c) 2017 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/btcsuite/btcd/addrmgr"
	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/blockchain/indexers"
	"github.com/btcsuite/btcd/connmgr"
	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/internal/inbound"
	"github.com/btcsuite/btcd/mempool"
	"github.com/btcsuite/btcd/mining"
	"github.com/btcsuite/btcd/mining/cpuminer"
	"github.com/btcsuite/btcd/netsync"
	"github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/v2transport"

	"github.com/btcsuite/btclog"
	"github.com/jrick/logrotate/rotator"
)

const (
	colorReset        = "\033[0m"
	colorGray         = "\033[90m"
	colorRed          = "\033[31m"
	colorGreen        = "\033[32m"
	colorYellow       = "\033[33m"
	colorBlue         = "\033[34m"
	colorCyan         = "\033[36m"
	colorWhite        = "\033[37m"
	colorLightBlue    = "\033[38;5;117m"
	colorLightOrange  = "\033[38;5;215m"
	colorLightMagenta = "\033[38;5;213m"
)

// Regex patterns compiled once at package level.
var (
	logPattern          = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?)\s+\[(\w+)\]\s+(\w+):\s+(.*)$`)
	processedPattern    = regexp.MustCompile(`(?i)(Processed\s+)(\d+)(\s+blocks)`)
	heightPattern       = regexp.MustCompile(`(?i)(height\s+)(\d+)`)
	transactionsPattern = regexp.MustCompile(`(?i)(\d+)(\s+transactions,?)`)
)

type colorizedWriter struct {
	buffer []byte
}

func newColorizedWriter() *colorizedWriter {
	enableWindowsANSIColors()
	return &colorizedWriter{
		buffer: make([]byte, 0, 4096),
	}
}

// btclog emits 3-char level tags: TRC, DBG, INF, WRN, ERR, CRT.
func getLevelColor(level string) string {
	switch level {
	case "TRC":
		return colorCyan
	case "DBG":
		return colorBlue
	case "INF":
		return colorGreen
	case "WRN":
		return colorYellow
	case "ERR", "CRT":
		return colorRed
	default:
		return colorWhite
	}
}

func colorizeMessage(message string) string {
	// Pre-check with cheap string searches before running regex.
	if strings.Contains(message, "rocess") {
		message = processedPattern.ReplaceAllString(message, "${1}"+colorLightOrange+"${2}"+colorReset+"${3}")
	}
	if strings.Contains(message, "eight") {
		message = heightPattern.ReplaceAllString(message, "${1}"+colorLightBlue+"${2}"+colorReset)
	}
	if strings.Contains(message, "ransaction") {
		message = transactionsPattern.ReplaceAllString(message, colorLightMagenta+"${1}"+colorReset+"${2}")
	}
	return message
}

func colorizeLine(line string) string {
	hasLF := strings.HasSuffix(line, "\n")
	line = strings.TrimRight(line, "\r\n")

	matches := logPattern.FindStringSubmatch(line)
	if len(matches) == 5 {
		var b strings.Builder
		b.WriteString(colorGray)
		b.WriteString(matches[1])
		b.WriteString(colorReset)
		b.WriteString(" [")
		b.WriteString(getLevelColor(matches[2]))
		b.WriteString(matches[2])
		b.WriteString(colorReset)
		b.WriteString("] ")
		b.WriteString(colorCyan)
		b.WriteString(matches[3])
		b.WriteString(colorReset)
		b.WriteString(": ")
		b.WriteString(colorizeMessage(matches[4]))
		if hasLF {
			b.WriteByte('\n')
		}
		return b.String()
	}

	if hasLF {
		return line + "\n"
	}
	return line
}

func (cw *colorizedWriter) Write(p []byte) (n int, err error) {
	if logRotator != nil {
		logRotator.Write(p)
	}

	cw.buffer = append(cw.buffer, p...)

	start := 0
	for {
		rel := bytes.IndexByte(cw.buffer[start:], '\n')
		if rel == -1 {
			break
		}
		end := start + rel + 1
		if _, err = os.Stdout.WriteString(colorizeLine(string(cw.buffer[start:end]))); err != nil {
			break
		}
		start = end
	}

	// Reset when drained so the backing array is reused instead of growing
	// unbounded; otherwise compact the tail back to the start.
	switch {
	case start == len(cw.buffer):
		cw.buffer = cw.buffer[:0]
	case start > 0:
		cw.buffer = cw.buffer[:copy(cw.buffer, cw.buffer[start:])]
	}

	return len(p), err
}

// Loggers per subsystem.  A single backend logger is created and all subsystem
// loggers created from it will write to the backend.  When adding new
// subsystems, add the subsystem logger variable here and to the
// subsystemLoggers map.
//
// Loggers can not be used before the log rotator has been initialized with a
// log file.  This must be performed early during application startup by calling
// initLogRotator.
var (
	// backendLog is the logging backend used to create all subsystem loggers.
	// The backend must not be used before the log rotator has been initialized,
	// or data races and/or nil pointer dereferences will occur.
	backendLog = btclog.NewBackend(newColorizedWriter())

	// logRotator is one of the logging outputs.  It should be closed on
	// application shutdown.
	logRotator *rotator.Rotator

	adxrLog = backendLog.Logger("ADXR")
	amgrLog = backendLog.Logger("AMGR")
	cmgrLog = backendLog.Logger("CMGR")
	bcdbLog = backendLog.Logger("BCDB")
	btcdLog = backendLog.Logger("BTCD")
	chanLog = backendLog.Logger("CHAN")
	discLog = backendLog.Logger("DISC")
	indxLog = backendLog.Logger("INDX")
	minrLog = backendLog.Logger("MINR")
	peerLog = backendLog.Logger("PEER")
	rpcsLog = backendLog.Logger("RPCS")
	scrpLog = backendLog.Logger("SCRP")
	srvrLog = backendLog.Logger("SRVR")
	syncLog = backendLog.Logger("SYNC")
	txmpLog = backendLog.Logger("TXMP")
	v2trLog = backendLog.Logger(v2transport.Subsystem)
)

// Initialize package-global logger variables.
func init() {
	addrmgr.UseLogger(amgrLog)
	connmgr.UseLogger(cmgrLog)
	database.UseLogger(bcdbLog)
	inbound.UseLogger(srvrLog)
	blockchain.UseLogger(chanLog)
	indexers.UseLogger(indxLog)
	mining.UseLogger(minrLog)
	cpuminer.UseLogger(minrLog)
	peer.UseLogger(peerLog)
	txscript.UseLogger(scrpLog)
	netsync.UseLogger(syncLog)
	mempool.UseLogger(txmpLog)
	v2transport.UseLogger(v2trLog)
}

// subsystemLoggers maps each subsystem identifier to its associated logger.
var subsystemLoggers = map[string]btclog.Logger{
	"ADXR":                adxrLog,
	"AMGR":                amgrLog,
	"CMGR":                cmgrLog,
	"BCDB":                bcdbLog,
	"BTCD":                btcdLog,
	"CHAN":                chanLog,
	"DISC":                discLog,
	"INDX":                indxLog,
	"MINR":                minrLog,
	"PEER":                peerLog,
	"RPCS":                rpcsLog,
	"SCRP":                scrpLog,
	"SRVR":                srvrLog,
	"SYNC":                syncLog,
	"TXMP":                txmpLog,
	v2transport.Subsystem: v2trLog,
}

// initLogRotator initializes the logging rotater to write logs to logFile and
// create roll files in the same directory.  It must be called before the
// package-global log rotater variables are used.
func initLogRotator(logFile string) {
	logDir, _ := filepath.Split(logFile)
	err := os.MkdirAll(logDir, 0700)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create log directory: %v\n", err)
		os.Exit(1)
	}
	r, err := rotator.New(logFile, 10*1024, false, 3)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create file rotator: %v\n", err)
		os.Exit(1)
	}

	logRotator = r
}

// setLogLevel sets the logging level for provided subsystem.  Invalid
// subsystems are ignored.  Uninitialized subsystems are dynamically created as
// needed.
func setLogLevel(subsystemID string, logLevel string) {
	// Ignore invalid subsystems.
	logger, ok := subsystemLoggers[subsystemID]
	if !ok {
		return
	}

	// Defaults to info if the log level is invalid.
	level, _ := btclog.LevelFromString(logLevel)
	logger.SetLevel(level)
}

// setLogLevels sets the log level for all subsystem loggers to the passed
// level.  It also dynamically creates the subsystem loggers as needed, so it
// can be used to initialize the logging system.
func setLogLevels(logLevel string) {
	// Configure all sub-systems with the new logging level.  Dynamically
	// create loggers as needed.
	for subsystemID := range subsystemLoggers {
		setLogLevel(subsystemID, logLevel)
	}
}

// directionString is a helper function that returns a string that represents
// the direction of a connection (inbound or outbound).
func directionString(inbound bool) string {
	if inbound {
		return "inbound"
	}
	return "outbound"
}

// pickNoun returns the singular or plural form of a noun depending
// on the count n.
func pickNoun(n uint64, singular, plural string) string {
	if n == 1 {
		return singular
	}
	return plural
}
