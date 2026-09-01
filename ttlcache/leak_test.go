// Copyright (c) 2021 - 2026, the autobrr contributors.
// SPDX-License-Identifier: MIT

package ttlcache

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"
)

// pkgFrame identifies stack frames belonging to this package. Derived from the
// package's own import path so a module rename cannot silently strand the
// filter matching nothing -- zero matches is the passing state.
var pkgFrame = reflect.TypeFor[Cache[int, int]]().PkgPath() + "."

// leakedGoroutines returns how many goroutines with stacks touching this
// package the goroutineleak profile reports, along with their records.
//
// The profile reports goroutines the collector can prove will never be
// unblocked. A goroutine that is merely parked is not reported, and neither is
// one holding an armed timer -- a cache left running with expirations pending
// counts as wakeable, so a forgotten Close is only caught once nothing can
// wake its loop. The profile is process-wide and cumulative, so the records
// are filtered to this package to avoid blaming it for a leak somewhere else.
func leakedGoroutines() (int, []string, error) {
	runtime.GC() // the profile is derived from GC reachability.

	var buf bytes.Buffer
	if err := pprof.Lookup("goroutineleak").WriteTo(&buf, 1); err != nil {
		return 0, nil, err
	}

	// the debug=1 format is a "goroutineleak profile: total N" header line,
	// then one record per unique stack separated by blank lines; identical
	// goroutines aggregate into a single record with a leading "N @" count.
	_, records, _ := strings.Cut(buf.String(), "\n")

	total := 0
	var leaked []string
	for _, record := range strings.Split(records, "\n\n") {
		if !strings.Contains(record, pkgFrame) {
			continue
		}

		record = strings.TrimSpace(record)
		count, _, _ := strings.Cut(record, " @")
		if n, err := strconv.Atoi(count); err == nil {
			total += n
		} else {
			total++
		}

		leaked = append(leaked, record)
	}

	return total, leaked, nil
}

// TestMain re-checks for leaks once the whole suite has finished.
//
// TestNoGoroutineLeak can only see its own workload: the profile is
// process-wide, so that test cannot run in parallel, which means it completes
// before the parallel tests resume. Checking here catches a cache that a test
// left parked with nothing left to wake it. It is a backstop, not proof: per
// the leakedGoroutines caveat, a forgotten Close on a cache still holding an
// armed timer goes unreported.
func TestMain(m *testing.M) {
	code := m.Run()

	total, leaked, err := leakedGoroutines()
	if err != nil {
		fmt.Fprintf(os.Stderr, "writing goroutineleak profile: %v\n", err)
		if code == 0 {
			code = 1
		}
	} else if total != 0 {
		// On a red run this is diagnosis, not a verdict -- a failing test may
		// have legitimately bailed out before closing its cache, the way
		// TestSetDoesNotDeadlockOnFullWakeChannel deliberately does -- so it
		// must never mask the original exit code.
		fmt.Fprintf(os.Stderr, "%d goroutine(s) still leaked after the suite finished:\n%s\n",
			total, strings.Join(leaked, "\n\n"))
		if code == 0 {
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

		c.Set(-1, -1, NoTTL) // parks the loop on the wake channel before any timer is armed

		for i := range 50 {
			c.Set(i, i, DefaultTTL)
		}

		c.Get(1)
		c.Delete(2)
		c.Close()
	}

	total, leaked, err := leakedGoroutines()
	if err != nil {
		t.Fatalf("writing goroutineleak profile: %v", err)
	}

	if total != 0 {
		t.Fatalf("%d leaked goroutine(s):\n%s", total, strings.Join(leaked, "\n\n"))
	}
}
