// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSourceManifest(t *testing.T) {
	root := t.TempDir()
	first := filepath.Join(root, "000000000.fdb")
	second := filepath.Join(root, "000000001.fdb")
	if err := os.WriteFile(first, []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte("12345"), 0o600); err != nil {
		t.Fatal(err)
	}

	count, total, digest, err := sourceManifest(
		root, []string{second, first},
	)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("file count: got %d, want 2", count)
	}
	if total != 8 {
		t.Fatalf("logical bytes: got %d, want 8", total)
	}

	wantInput := "000000000.fdb\t3\n000000001.fdb\t5\n"
	wantDigest := fmt.Sprintf("%x", sha256.Sum256([]byte(wantInput)))
	if digest != wantDigest {
		t.Fatalf("manifest digest: got %s, want %s", digest, wantDigest)
	}
}

func TestWriteReplayReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	want := replayReport{
		SchemaVersion:         1,
		BlocksReplayed:        42,
		AverageDurableTPS:     1234.5,
		DurableElapsedSeconds: 9.25,
		ExitCode:              0,
	}
	if err := writeReplayReport(path, want); err != nil {
		t.Fatal(err)
	}

	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) == 0 || encoded[len(encoded)-1] != '\n' {
		t.Fatal("JSON report does not end with a newline")
	}

	var got replayReport
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != want.SchemaVersion ||
		got.BlocksReplayed != want.BlocksReplayed ||
		got.AverageDurableTPS != want.AverageDurableTPS ||
		got.DurableElapsedSeconds != want.DurableElapsedSeconds {

		t.Fatalf("report round trip mismatch: got %+v, want %+v", got, want)
	}
}
