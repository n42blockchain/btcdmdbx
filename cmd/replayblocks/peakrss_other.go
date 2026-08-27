//go:build !darwin && !linux && !windows

// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

func peakRSSBytes() uint64 {
	return 0
}
