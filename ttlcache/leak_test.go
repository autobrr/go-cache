// Copyright (c) 2021 - 2026, the autobrr contributors.
// SPDX-License-Identifier: MIT

package ttlcache

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

// pkgFrame identifies stack frames belonging to this package. It is matched
// against the import path, so it needs updating if the module is renamed.
const pkgFrame = "go-cache/ttlcache."

// leakedGoroutines returns the goroutineleak profile records whose stacks
// touch this package.
//
// The profile reports goroutines the collector can prove will never be
// unblocked, so a goroutine that is merely parked -- a live cache waiting on
// its wake channel -- is not reported. It is process-wide and cumulative
// though, so the records are filtered to this package to avoid blaming it for
// a leak somewhere else.
func leakedGoroutines() ([]string, error) {
	runtime.GC() // the profile is derived from GC reachability.

	var buf bytes.Buffer
	if err := pprof.Lookup("goroutineleak").WriteTo(&buf, 1); err != nil {
		return nil, err
	}

	var leaked []string
	for _, record := range strings.Split(buf.String(), "\n\n") {
		if strings.Contains(record, pkgFrame) {
			leaked = append(leaked, strings.TrimSpace(record))
		}
	}

	return leaked, nil
}

// TestMain re-checks for leaks once the whole suite has finished.
//
// TestNoGoroutineLeak can only see its own workload: the profile is
// process-wide, so that test cannot run in parallel, which means it completes
// before the parallel tests resume. Checking here catches a cache that any
// test left running.
func TestMain(m *testing.M) {
	code := m.Run()

	// Only meaningful on a green run. A failing test may have bailed out
	// before closing its cache -- TestSetDoesNotDeadlockOnFullWakeChannel
	// deliberately does -- and reporting that on top of the real failure
	// buries it.
	if code == 0 {
		leaked, err := leakedGoroutines()
		if err != nil {
			fmt.Fprintf(os.Stderr, "writing goroutineleak profile: %v\n", err)
			code = 1
		} else if len(leaked) != 0 {
			fmt.Fprintf(os.Stderr, "%d goroutine(s) still leaked after the suite finished:\n%s\n",
				len(leaked), strings.Join(leaked, "\n\n"))
			code = 1
		}
	}

	os.Exit(code)
}

// TestNoGoroutineLeak guards the failure mode behind both deadlocks this
// package has had: a goroutine parked forever on a channel or a lock. The
// profile catches either, and names the frame it is stuck on.
//
// Deliberately not parallel -- the underlying profile is process-wide.
func TestNoGoroutineLeak(t *testing.T) {
	for range 20 {
		c := New[int, int](
			SetDefaultTTL(time.Millisecond),
			SetDeallocationFunc(func(key int, value int, reason DeallocationReason) {}),
		)

		for i := range 50 {
			c.Set(i, i, DefaultTTL)
		}

		c.Set(-1, -1, NoTTL) // parks the loop on the wake channel, no timer armed
		c.Get(1)
		c.Delete(2)
		c.Close()
	}

	leaked, err := leakedGoroutines()
	if err != nil {
		t.Fatalf("writing goroutineleak profile: %v", err)
	}

	if len(leaked) != 0 {
		t.Fatalf("%d leaked goroutine(s):\n%s", len(leaked), strings.Join(leaked, "\n\n"))
	}
}
