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
	"runtime"
	"strings"
	"syscall"

	"github.com/btcsuite/btcd/addrmgr"
	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/blockchain/indexers"
	"github.com/btcsuite/btcd/connmgr"
	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/mempool"
	"github.com/btcsuite/btcd/mining"
	"github.com/btcsuite/btcd/mining/cpuminer"
	"github.com/btcsuite/btcd/netsync"
	"github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/v2transport"

	"github.com/btcsuite/btclog"
	"github.com/jrick/logrotate/rotator"
)

var (
	kernel32                        = syscall.NewLazyDLL("kernel32.dll")
	procGetStdHandle                = kernel32.NewProc("GetStdHandle")
	procSetConsoleMode              = kernel32.NewProc("SetConsoleMode")
	enableVirtualTerminalProcessing = uint32(0x0004)
	stdOutputHandle                 = uintptr(0xFFFFFFF5) // STD_OUTPUT_HANDLE
)

// enableWindowsANSIColors enables ANSI color support on Windows 10+
func enableWindowsANSIColors() {
	if runtime.GOOS != "windows" {
		return
	}

	handle, _, _ := procGetStdHandle.Call(stdOutputHandle)
	if handle == 0 || handle == ^uintptr(0) {
		return
	}

	var mode uint32
	err := syscall.GetConsoleMode(syscall.Handle(handle), &mode)
	if err != nil {
		return
	}

	mode |= enableVirtualTerminalProcessing
	procSetConsoleMode.Call(handle, uintptr(mode))
}

// ANSI color codes
const (
	colorReset         = "\033[0m"
	colorGray          = "\033[90m"
	colorRed           = "\033[31m"
	colorGreen         = "\033[32m"
	colorYellow        = "\033[33m"
	colorBlue          = "\033[34m"
	colorMagenta       = "\033[35m" // Purple
	colorCyan          = "\033[36m"
	colorWhite         = "\033[37m"
	colorOrange        = "\033[38;5;208m" // Orange (256-color mode)
	colorBrightMagenta = "\033[38;5;201m" // Bright Purple (256-color mode)
	colorBrightOrange  = "\033[38;5;214m" // Bright Orange (256-color mode)
	colorLightBlue     = "\033[38;5;117m" // Light Blue / Cyan Blue (256-color mode)
	colorLightOrange   = "\033[38;5;215m" // Light Orange / Tangerine (256-color mode)
)

// colorizedWriter implements an io.Writer that adds colors to log output
// and writes to both standard output and the log rotator.
type colorizedWriter struct {
	buffer           []byte
	plainBuf         bytes.Buffer
	logPattern       *regexp.Regexp
	processedPattern *regexp.Regexp
	heightPattern    *regexp.Regexp
}

// newColorizedWriter creates a new colorized writer
func newColorizedWriter() *colorizedWriter {
	// Pattern to match: timestamp [LEVEL] subsystem: message
	// Example: "2024-01-01 12:00:00.000 [INFO] BTCD: message"
	pattern := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?)\s+\[(\w+)\]\s+(\w+):\s+(.*)$`)
	// Pattern to match "Processed X blocks" - capture the number (case-insensitive)
	processedPattern := regexp.MustCompile(`(?i)(Processed\s+)(\d+)(\s+blocks)`)
	// Pattern to match "height X" - capture the number (case-insensitive)
	heightPattern := regexp.MustCompile(`(?i)(height\s+)(\d+)`)
	return &colorizedWriter{
		buffer:           make([]byte, 0, 4096),
		logPattern:       pattern,
		processedPattern: processedPattern,
		heightPattern:    heightPattern,
	}
}

// getLevelColor returns the color code for a log level
func getLevelColor(level string) string {
	level = strings.ToUpper(level)
	switch level {
	case "TRACE":
		return colorCyan
	case "DEBUG":
		return colorBlue
	case "INFO", "INF":
		return colorGreen
	case "WARN":
		return colorYellow
	case "ERROR":
		return colorRed
	case "CRIT":
		return colorRed
	default:
		return colorWhite
	}
}

// colorizeMessage adds colors to specific numbers in the message
func (cw *colorizedWriter) colorizeMessage(message string) string {
	result := message

	// Colorize numbers in "Processed X blocks" pattern (light orange / tangerine)
	result = cw.processedPattern.ReplaceAllStringFunc(result, func(match string) string {
		parts := cw.processedPattern.FindStringSubmatch(match)
		if len(parts) == 4 {
			return parts[1] + colorLightOrange + parts[2] + colorReset + parts[3]
		}
		return match
	})

	// Colorize numbers in "height X" pattern (light blue / cyan blue)
	result = cw.heightPattern.ReplaceAllStringFunc(result, func(match string) string {
		parts := cw.heightPattern.FindStringSubmatch(match)
		if len(parts) == 3 {
			return parts[1] + colorLightBlue + parts[2] + colorReset
		}
		return match
	})

	return result
}

// colorizeLine adds colors to a log line
func (cw *colorizedWriter) colorizeLine(line string) string {
	// Check for and preserve newline (handle both \n and \r\n)
	hasCRLF := strings.HasSuffix(line, "\r\n")
	hasLF := !hasCRLF && strings.HasSuffix(line, "\n")

	// Remove trailing newlines for processing
	line = strings.TrimSuffix(line, "\r\n")
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")

	// Try to match the log pattern
	matches := cw.logPattern.FindStringSubmatch(line)
	if len(matches) == 5 {
		timestamp := matches[1]
		level := matches[2]
		subsystem := matches[3]
		message := matches[4]

		// Build colored output
		var colored strings.Builder
		colored.WriteString(colorGray) // Time in gray
		colored.WriteString(timestamp)
		colored.WriteString(colorReset)
		colored.WriteString(" [")
		colored.WriteString(getLevelColor(level)) // Level color
		colored.WriteString(level)
		colored.WriteString(colorReset)
		colored.WriteString("] ")
		colored.WriteString(colorCyan) // Subsystem in cyan
		colored.WriteString(subsystem)
		colored.WriteString(colorReset)
		colored.WriteString(": ")
		// Colorize message with special number highlighting
		coloredMessage := cw.colorizeMessage(message)
		colored.WriteString(coloredMessage)

		// Add exactly one newline
		if hasCRLF || hasLF {
			colored.WriteString("\n")
		}
		return colored.String()
	}

	// If pattern doesn't match, return original line with newline preserved
	if hasCRLF {
		return line + "\r\n"
	}
	if hasLF {
		return line + "\n"
	}
	return line
}

// Write implements io.Writer interface
func (cw *colorizedWriter) Write(p []byte) (n int, err error) {
	// Write plain version to log rotator (without colors)
	if logRotator != nil {
		cw.plainBuf.Reset()
		cw.plainBuf.Write(p)
		logRotator.Write(cw.plainBuf.Bytes())
	}

	// Process and colorize for stdout
	cw.buffer = append(cw.buffer, p...)

	// Process complete lines
	for {
		newlineIdx := bytes.IndexByte(cw.buffer, '\n')
		if newlineIdx == -1 {
			// No complete line yet, keep buffering
			break
		}

		// Extract line (including newline)
		// Check for \r\n (Windows) or just \n (Unix)
		lineEnd := newlineIdx + 1
		lineBytes := cw.buffer[:lineEnd]
		cw.buffer = cw.buffer[lineEnd:]

		// Convert to string and colorize
		line := string(lineBytes)
		colored := cw.colorizeLine(line)

		// Write colored output to stdout
		_, err = os.Stdout.WriteString(colored)
		if err != nil {
			return n, err
		}
		n += len(lineBytes)
	}

	// Handle remaining buffer (incomplete line) - only flush if buffer is getting too large
	if len(cw.buffer) > 4096 {
		colored := cw.colorizeLine(string(cw.buffer))
		_, err = os.Stdout.WriteString(colored)
		if err != nil {
			return n, err
		}
		n += len(cw.buffer)
		cw.buffer = cw.buffer[:0]
	}

	return n, nil
}

// logWriter implements an io.Writer that outputs to both standard output and
// the write-end pipe of an initialized log rotator.
type logWriter struct {
	colorized *colorizedWriter
}

func newLogWriter() *logWriter {
	// Enable ANSI colors on Windows
	enableWindowsANSIColors()
	return &logWriter{
		colorized: newColorizedWriter(),
	}
}

func (lw *logWriter) Write(p []byte) (n int, err error) {
	return lw.colorized.Write(p)
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
	backendLog = btclog.NewBackend(newLogWriter())

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
