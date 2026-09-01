// Copyright (c) 2021 - 2026, the autobrr contributors.
// SPDX-License-Identifier: MIT

package ttlcache

import (
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestGet(t *testing.T) {
	t.Parallel()
	c := New[int, bool](SetDefaultTTL(1 * time.Second))
	defer c.Close()

	for i := range 10 {
		c.Set(i, true, DefaultTTL)
	}

	for i := range 10 {
		val, ok := c.Get(i)
		if !ok {
			t.Fatalf("missing key: %d", i)
		} else if !val {
			t.Fatalf("bad value on key: %d", i)
		}
	}
}

func TestDefaultTTLWithoutConfiguredDefaultIsNoTTL(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		c := New[int, bool]()
		defer c.Close()

		it := c.SetItem(1, true, DefaultTTL)
		if it.GetDuration() != NoTTL || !it.GetTime().IsZero() {
			t.Fatalf("default TTL was not canonicalized to NoTTL: duration=%s deadline=%s", it.GetDuration(), it.GetTime())
		}

		synctest.Sleep(24 * time.Hour)
		if value, ok := c.Get(1); !ok || !value {
			t.Fatal("item stored without a configured default TTL expired")
		}
	})
}

// sweeps records when the expiration loop handed each key to the deallocation
// callback, so a test can assert on the sweep itself. Get and Keys hide an
// entry past its deadline on their own, so a read cannot tell whether the loop
// ever collected it: a timer that never re-arms passes every read-based check.
// Other reasons are ignored; replacements and deletes have their own tests.
type sweeps[K comparable, V any] struct {
	mu sync.Mutex
	at map[K]time.Time
}

func (s *sweeps[K, V]) record(key K, _ V, reason DeallocationReason) {
	if reason != ReasonTimedOut {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.at == nil {
		s.at = make(map[K]time.Time)
	}
	s.at[key] = time.Now()
}

// sweptAt returns when key was swept, or false if it never was.
func (s *sweeps[K, V]) sweptAt(key K) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	at, ok := s.at[key]
	return at, ok
}

// expect fails t unless key was swept exactly at deadline. Under synctest the
// clock only moves while the bubble is idle, so a sweep lands on its deadline
// to the nanosecond; anything later means the timer was armed late.
func (s *sweeps[K, V]) expect(t *testing.T, key K, deadline time.Time) {
	t.Helper()

	at, ok := s.sweptAt(key)
	if !ok {
		t.Fatalf("key %v was never swept", key)
	}
	if !at.Equal(deadline) {
		t.Fatalf("key %v swept %s after its deadline", key, at.Sub(deadline))
	}
}

func TestExpirations(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var s sweeps[int, bool]
		c := New[int, bool](SetDefaultTTL(200*time.Millisecond), SetDeallocationFunc(s.record))
		defer c.Close()

		start := time.Now()
		for i := range 10 {
			c.Set(i, true, DefaultTTL)
		}

		synctest.Sleep(1 * time.Second)
		for i := range 10 {
			s.expect(t, i, start.Add(200*time.Millisecond))
		}
	})
}

func TestSwaps(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var s sweeps[int, bool]
		c := New[int, bool](SetDefaultTTL(200*time.Millisecond), SetDeallocationFunc(s.record))
		defer c.Close()

		start := time.Now()
		for i := range 10 {
			c.Set(i, true, DefaultTTL)
		}

		synctest.Sleep(1 * time.Second)
		for i := range 10 {
			s.expect(t, i, start.Add(200*time.Millisecond))
		}

		// the map is empty and the timer stopped; a new store must arm it again.
		start = time.Now()
		for i := 10; i < 20; i++ {
			c.Set(i, true, DefaultTTL)
		}

		synctest.Sleep(1 * time.Second)
		for i := 10; i < 20; i++ {
			s.expect(t, i, start.Add(200*time.Millisecond))
		}
	})
}

func TestRetimer(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var s sweeps[int, bool]
		c := New[int, bool](SetDefaultTTL(200*time.Millisecond), SetDeallocationFunc(s.record))
		defer c.Close()

		start := time.Now()
		for i := 1; i < 10; i++ {
			c.Set(i, true, time.Duration(10-i)*100*time.Millisecond)
			// let the loop arm from this deadline before a lower one lands;
			// otherwise one coalesced wake carries the lowest of them and the
			// re-arm is never exercised.
			synctest.Wait()
		}

		// the shortest TTL was set last, so collecting key 9 on time requires
		// the loop to re-arm its timer to the earlier deadline.
		synctest.Sleep(150 * time.Millisecond)
		if _, ok := s.sweptAt(9); !ok {
			t.Fatal("key 9 outlived its 100ms TTL; the timer was not re-armed to the earlier deadline")
		}

		synctest.Sleep(2 * time.Second)
		for i := 1; i < 10; i++ {
			s.expect(t, i, start.Add(time.Duration(10-i)*100*time.Millisecond))
		}
	})
}

func TestSchedule(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var s sweeps[int, bool]
		c := New[int, bool](SetDefaultTTL(1*time.Second), SetDeallocationFunc(s.record))
		defer c.Close()

		start := time.Now()
		for i := 1; i < 10; i++ {
			c.Set(i, true, time.Duration(i)*100*time.Millisecond)
		}

		synctest.Sleep(3 * time.Second)
		for i := 1; i < 10; i++ {
			s.expect(t, i, start.Add(time.Duration(i)*100*time.Millisecond))
		}
	})
}

func TestInterlace(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		c := New[int, bool](SetDefaultTTL(100 * time.Millisecond))
		defer c.Close()
		swap := false
		for i := range 10 {
			swap = !swap
			ttl := DefaultTTL
			if swap {
				ttl = NoTTL
			}
			c.Set(i, true, ttl)
		}

		synctest.Sleep(1 * time.Second)
		swap = false
		for i := range 10 {
			swap = !swap
			if !swap {
				continue
			}

			if _, ok := c.Get(i); !ok {
				t.Fatalf("missing key: %d", i)
			}
		}
	})
}

func TestReschedule(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var s sweeps[int, bool]
		c := New[int, bool](SetDefaultTTL(100*time.Millisecond), SetDeallocationFunc(s.record))
		defer c.Close()

		start := time.Now()
		for i := 1; i < 10; i++ {
			c.Set(i, true, NoTTL)
			c.Set(i, true, DefaultTTL)
		}

		synctest.Sleep(1 * time.Second)
		for i := 1; i < 10; i++ {
			s.expect(t, i, start.Add(100*time.Millisecond))
		}
	})
}

func TestRescheduleNoTTL(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		c := New[int, bool](SetDefaultTTL(100 * time.Millisecond))
		defer c.Close()
		for i := 1; i < 10; i++ {
			c.Set(i, true, DefaultTTL)
			c.Set(i, true, NoTTL)
		}

		synctest.Sleep(1 * time.Second)
		for i := 1; i < 10; i++ {
			if _, ok := c.Get(i); !ok {
				t.Fatalf("missing key: %d", i)
			}
		}
	})
}

func TestDelete(t *testing.T) {
	t.Parallel()
	c := New[int, bool](SetDefaultTTL(100 * time.Millisecond))
	defer c.Close()
	for i := 1; i < 10; i++ {
		c.Set(i, true, NoTTL)
		c.Delete(i)
	}

	for i := 1; i < 10; i++ {
		if _, ok := c.Get(i); ok {
			t.Fatalf("found key: %d", i)
		}
	}
}

func TestDeallocationTimeout(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var hit atomic.Bool // the deallocation runs on the expiration goroutine.
		c := New[int, bool](
			SetDefaultTTL(time.Millisecond*100),
			SetDeallocationFunc(func(key int, value bool, reason DeallocationReason) { hit.Store(reason == ReasonTimedOut) }),
		)
		defer c.Close()

		for i := range 1 {
			c.Set(i, true, DefaultTTL)
		}

		synctest.Sleep(3 * time.Second)
		if !hit.Load() {
			t.Fatalf("Deallocation not hit.")
		}
	})
}

func TestDeallocationDeleted(t *testing.T) {
	t.Parallel()
	hit := false
	c := New[int, bool](
		SetDefaultTTL(time.Millisecond*100),
		SetDeallocationFunc(func(key int, value bool, reason DeallocationReason) { hit = reason == ReasonDeleted }),
	)
	defer c.Close()

	for i := range 1 {
		c.Set(i, true, DefaultTTL)
		c.Delete(i)
	}

	if !hit {
		t.Fatalf("Deallocation not hit.")
	}
}

func TestTimerReset(t *testing.T) {
	t.Parallel()
	ch := make(chan struct{})
	defer close(ch)

	c := New[int, bool](
		SetDefaultTTL(time.Millisecond*100),
		SetDeallocationFunc(func(key int, value bool, reason DeallocationReason) { ch <- struct{}{} }),
	)

	defer c.Close()

	const base = 0
	const rounds = 1
	for i := range rounds {
		c.Set(i, true, DefaultTTL)
	}

	for range rounds {
		<-ch
	}

	for i := range 1 {
		c.Set(i, true, DefaultTTL)
	}

	for range rounds {
		<-ch
	}
}

func TestGetDoesNotClobberSet(t *testing.T) {
	t.Parallel()

	// A fine timer resolution makes the TTL refresh in GetItem fire on nearly
	// every Get, which is what exposes the read-then-write race.
	c := New[int, int](
		SetDefaultTTL(time.Minute),
		SetTimerResolution(10*time.Microsecond),
	)

	defer c.Close()

	const key = 1
	const rounds = 200000

	lost := 0
	for range rounds {
		c.Set(key, 0, DefaultTTL)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			c.Get(key)
		}()
		go func() {
			defer wg.Done()
			c.Set(key, 1, DefaultTTL)
		}()
		wg.Wait()

		if v, _ := c.Get(key); v != 1 {
			lost++
		}
	}

	if lost != 0 {
		t.Fatalf("lost %d writes out of %d rounds", lost, rounds)
	}
}

func TestKeys(t *testing.T) {
	t.Parallel()
	c := New[int, bool](SetDefaultTTL(1 * time.Second))
	defer c.Close()

	const count = 10
	for i := range count {
		c.Set(i, true, DefaultTTL)
	}

	keys := slices.Collect(c.Keys())
	if len(keys) != count {
		t.Fatalf("expected %d keys, got %d: %v", count, len(keys), keys)
	}

	seen := make(map[int]bool, len(keys))
	for _, k := range keys {
		if seen[k] {
			t.Fatalf("duplicate key: %d", k)
		}
		seen[k] = true

		if _, ok := c.Get(k); !ok {
			t.Fatalf("key not in cache: %d", k)
		}
	}
}

func TestKeysStopsEarly(t *testing.T) {
	t.Parallel()
	c := New[int, bool](SetDefaultTTL(1 * time.Second))
	defer c.Close()

	for i := range 10 {
		c.Set(i, true, DefaultTTL)
	}

	n := 0
	for range c.Keys() {
		n++
		break
	}

	if n != 1 {
		t.Fatalf("break did not stop iteration, ran %d times", n)
	}
}

func TestAll(t *testing.T) {
	t.Parallel()
	c := New[int, int](SetDefaultTTL(1 * time.Second))
	defer c.Close()

	const count = 10
	for i := range count {
		c.Set(i, i*10, DefaultTTL)
	}

	got := maps.Collect(c.All())
	if len(got) != count {
		t.Fatalf("expected %d pairs, got %d: %v", count, len(got), got)
	}

	for k, v := range got {
		if v != k*10 {
			t.Fatalf("key %d carried value %d", k, v)
		}
	}
}

// Iterating must not push expirations forward. A loop that merely inspects the
// cache would otherwise keep every item it touched alive indefinitely.
func TestAllDoesNotRefreshExpirations(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		c := New[int, int](SetDefaultTTL(500 * time.Millisecond))
		defer c.Close()

		c.Set(1, 1, DefaultTTL)

		// range across more than the item's whole lifetime
		for range 10 {
			for range c.All() {
			}

			synctest.Sleep(100 * time.Millisecond)
		}

		if _, ok := c.Get(1); ok {
			t.Fatal("item outlived its TTL; iteration refreshed it")
		}
	})
}

// The body runs outside the lock, so it may use the cache. Both deadlocks this
// package has had came from running caller code while holding it.
func TestIterationBodyMayTouchTheCache(t *testing.T) {
	t.Parallel()

	c := New[int, int](SetDefaultTTL(time.Minute))

	for i := range 10 {
		c.Set(i, i, DefaultTTL)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for k, v := range c.All() {
			c.Get(k)
			c.Set(k+100, v, DefaultTTL)
			c.Delete(k)
		}

		for k := range c.Keys() {
			c.Delete(k)
		}
	}()

	select {
	case <-done:
		c.Close()
	case <-time.After(10 * time.Second):
		// Close would block on the same held lock, so leave the cache alone.
		t.Fatal("iteration deadlocked against a body that used the cache")
	}
}

// The key snapshot must be taken when All is called, not when iteration
// begins, matching Keys and the documentation.
func TestAllSnapshotsAtCallTime(t *testing.T) {
	t.Parallel()

	c := New[int, int](SetDefaultTTL(time.Minute))
	defer c.Close()

	c.Set(1, 1, DefaultTTL)
	it := c.All()
	c.Set(2, 2, DefaultTTL) // stored after the snapshot

	seen := maps.Collect(it)
	if _, ok := seen[2]; ok {
		t.Fatal("key stored after All() appeared in the snapshot")
	}
	if _, ok := seen[1]; !ok {
		t.Fatal("missing key: 1")
	}
}

// A key that leaves the cache between the snapshot and the body reaching it
// must be skipped, not yielded with a zero value.
func TestAllSkipsDepartedEntries(t *testing.T) {
	t.Parallel()

	c := New[int, int](SetDefaultTTL(time.Minute))
	defer c.Close()

	const count = 10
	for i := range count {
		c.Set(i, i+1, DefaultTTL) // non-zero values: a zero can only mean a departed entry.
	}

	// the first visit deletes every other key, so the rest of the snapshot is
	// departed by the time the iterator reaches it.
	visited := 0
	for k, v := range c.All() {
		if v == 0 {
			t.Fatalf("departed key %d yielded a zero value", k)
		}
		visited++

		for i := range count {
			if i != k {
				c.Delete(i)
			}
		}
	}

	if visited != 1 {
		t.Fatalf("expected exactly 1 visit before the rest departed, got %d", visited)
	}
}

func TestSetDoesNotDeadlockOnFullWakeChannel(t *testing.T) {
	t.Parallel()

	// A short TTL keeps the expiration loop cycling through expire(), which
	// takes the same write lock a Set holds while it nudges the wake channel.
	// Enough concurrent writers starve the loop at that lock long enough for
	// the channel to fill behind it.
	var swept atomic.Int64
	c := New[int, int](
		SetDefaultTTL(2*time.Millisecond),
		SetDeallocationFunc(func(int, int, DeallocationReason) { swept.Add(1) }),
	)

	const writers = 8
	const perWriter = 25000

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for w := range writers {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := range perWriter {
					c.Set(w*perWriter+i, i, DefaultTTL)
				}
			}(w)
		}
		wg.Wait()
	}()

	select {
	case <-done:
		// A dropped nudge may delay collection, but every sweep re-derives the
		// next wake-up from the whole map, so nothing is stranded. Counted at
		// the callback: getkeys hides a corpse on its own and could not tell.
		const total = writers * perWriter
		deadline := time.Now().Add(30 * time.Second)
		for swept.Load() < total && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}

		if n := swept.Load(); n != total {
			t.Fatalf("%d of %d entries never expired after dropped wake-ups", total-n, total)
		}

		c.Close()
	case <-time.After(30 * time.Second):
		// Close would block on the held lock too, so leave the cache alone.
		t.Fatal("deadlock: Set is blocked sending on the wake channel while holding the write lock")
	}
}

func TestDeallocationReentrancy(t *testing.T) {
	t.Parallel()

	// The callback used to run under the write lock, so touching the cache
	// from inside it deadlocked on both the Delete and the timeout path.
	done := make(chan DeallocationReason, 3)
	var c *Cache[int, int]
	c = New[int, int](
		SetDefaultTTL(100*time.Millisecond),
		SetDeallocationFunc(func(key int, value int, reason DeallocationReason) {
			c.Get(key)
			done <- reason
		}),
	)
	// no deferred Close: on the failure paths a deadlocked goroutine still
	// holds the write lock, and Close would hang the whole binary on it.

	c.Set(1, 1, NoTTL)
	deleted := make(chan struct{})
	go func() {
		c.Delete(1)
		close(deleted)
	}()

	select {
	case <-deleted:
	case <-time.After(10 * time.Second):
		t.Fatal("Delete deadlocked running the deallocation callback")
	}

	c.Set(2, 2, DefaultTTL)

	c.Set(3, 3, NoTTL)
	replaced := make(chan struct{})
	go func() {
		c.Set(3, 33, NoTTL)
		close(replaced)
	}()

	select {
	case <-replaced:
	case <-time.After(10 * time.Second):
		t.Fatal("Set deadlocked running the deallocation callback for the displaced value")
	}

	seen := make(map[DeallocationReason]bool)
	for range 3 {
		select {
		case r := <-done:
			seen[r] = true
		case <-time.After(10 * time.Second):
			t.Fatal("deallocation callback never fired; the expiration goroutine is likely deadlocked")
		}
	}

	if !seen[ReasonDeleted] || !seen[ReasonTimedOut] || !seen[ReasonReplaced] {
		t.Fatalf("expected all three reasons, got: %v", seen)
	}

	c.Close()
}

func TestDroppedWakeUpDoesNotStrandShortTTL(t *testing.T) {
	t.Parallel()

	// A timeout callback that blocks stalls the loop without holding the
	// lock, so writers can fill the wake channel behind it with far-future
	// deadlines. The dropped nudge for a short-TTL item set at that moment
	// must not strand its collection until those deadlines.
	entered := make(chan struct{})
	release := make(chan struct{})
	var first atomic.Bool
	var swept atomic.Bool

	c := New[int, int](
		SetDeallocationFunc(func(key int, value int, reason DeallocationReason) {
			if first.CompareAndSwap(false, true) {
				close(entered)
				<-release
				return
			}
			if key == 7 {
				swept.Store(true)
			}
		}),
	)
	defer c.Close()

	c.Set(0, 0, 5*time.Millisecond)
	<-entered

	for i := range 1100 {
		c.Set(1000+i, i, time.Hour)
	}

	c.Set(7, 7, 5*time.Millisecond)
	close(release)

	deadline := time.Now().Add(10 * time.Second)
	for !swept.Load() {
		if time.Now().After(deadline) {
			t.Fatal("the 5ms item was not collected; its wake-up was lost behind hour-long deadlines")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestGetTreatsExpiredAsMissing(t *testing.T) {
	t.Parallel()

	// While the sweep is stalled in a blocked callback, an entry past its
	// deadline must read as missing everywhere and must not be refreshed
	// back to life.
	entered := make(chan struct{})
	release := make(chan struct{})
	var first atomic.Bool

	c := New[int, int](
		SetDefaultTTL(time.Minute), // refresh stays enabled
		SetDeallocationFunc(func(key int, value int, reason DeallocationReason) {
			if first.CompareAndSwap(false, true) {
				close(entered)
				<-release
			}
		}),
	)
	defer c.Close()
	defer close(release)

	c.Set(0, 0, 5*time.Millisecond)
	<-entered

	c.Set(8, 8, 5*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	if _, ok := c.Get(8); ok {
		t.Fatal("Get returned an entry past its deadline")
	}

	if it, ok := c.GetItem(8); ok || !it.GetTime().IsZero() {
		t.Fatal("GetItem returned or refreshed an entry past its deadline")
	}

	if slices.Contains(c.GetKeys(), 8) {
		t.Fatal("GetKeys still lists an entry past its deadline")
	}

	for k := range c.All() {
		if k == 8 {
			t.Fatal("All yielded an entry past its deadline")
		}
	}
}

func TestStoreOverExpiredDeallocatesTimedOut(t *testing.T) {
	t.Parallel()

	// A Set, GetOrSet, or Delete landing on an expired-but-unswept entry
	// must still hand the corpse to the deallocation callback, as a timeout.
	entered := make(chan struct{})
	release := make(chan struct{})
	var first atomic.Bool
	reasons := make(chan DeallocationReason, 4)

	c := New[int, int](
		DisableUpdateTime(true),
		SetDeallocationFunc(func(key int, value int, reason DeallocationReason) {
			if first.CompareAndSwap(false, true) {
				close(entered)
				<-release
				return
			}
			reasons <- reason
		}),
	)
	defer c.Close()
	defer close(release)

	c.Set(0, 0, 5*time.Millisecond)
	<-entered

	c.Set(9, 9, 5*time.Millisecond)
	c.Set(10, 10, 5*time.Millisecond)
	c.Set(11, 11, 5*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	c.Set(9, 90, NoTTL)        // displaces the corpse under 9
	c.GetOrSet(10, 100, NoTTL) // reads 10 as missing, displaces its corpse
	c.Delete(11)               // removes the corpse under 11

	for range 3 {
		select {
		case r := <-reasons:
			if r != ReasonTimedOut {
				t.Fatalf("corpse removal reported %v, want ReasonTimedOut", r)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a removed expired value never reached the deallocation callback")
		}
	}

	if v, _ := c.Get(9); v != 90 {
		t.Fatalf("key 9 should hold 90, got: %d", v)
	}
	if v, _ := c.Get(10); v != 100 {
		t.Fatalf("key 10 should hold 100, got: %d", v)
	}
}

func TestCloseWaitsForExpirationGoroutine(t *testing.T) {
	t.Parallel()

	// Close's contract is that the loop -- including any timeout callback it
	// is running -- has finished by the time it returns, so callers can tear
	// down resources the callback uses.
	entered := make(chan struct{})
	finished := make(chan struct{})
	var first atomic.Bool

	c := New[int, int](
		SetDeallocationFunc(func(key int, value int, reason DeallocationReason) {
			if first.CompareAndSwap(false, true) {
				close(entered)
				time.Sleep(100 * time.Millisecond)
				close(finished)
			}
		}),
	)

	c.Set(0, 0, 5*time.Millisecond)
	<-entered

	c.Close()

	select {
	case <-finished:
	default:
		t.Fatal("Close returned while a timeout deallocation callback was still running")
	}
}

func TestGetOrSet(t *testing.T) {
	t.Parallel()
	c := New[int, int](SetDefaultTTL(time.Minute))
	defer c.Close()

	if v, loaded := c.GetOrSet(1, 10, DefaultTTL); loaded || v != 10 {
		t.Fatalf("store path: got v=%d loaded=%v", v, loaded)
	}

	if v, loaded := c.GetOrSet(1, 20, DefaultTTL); !loaded || v != 10 {
		t.Fatalf("load path: got v=%d loaded=%v", v, loaded)
	}
}

func TestDeallocationReplaced(t *testing.T) {
	t.Parallel()

	// Set over a live key must hand the displaced value to the callback:
	// callers storing resources rely on it to release the loser of a
	// prepare-then-Set race.
	type call struct {
		value  int
		reason DeallocationReason
	}
	var calls []call
	c := New[int, int](
		SetDefaultTTL(time.Minute),
		SetDeallocationFunc(func(key int, value int, reason DeallocationReason) {
			calls = append(calls, call{value, reason})
		}),
	)
	defer c.Close()

	c.Set(1, 10, DefaultTTL)      // fresh key: no callback
	c.Set(1, 20, DefaultTTL)      // displaces 10
	c.GetOrSet(1, 30, DefaultTTL) // found, nothing displaced: no callback

	if len(calls) != 1 || calls[0].value != 10 || calls[0].reason != ReasonReplaced {
		t.Fatalf("expected one ReasonReplaced callback for value 10, got: %+v", calls)
	}

	if v, _ := c.Get(1); v != 20 {
		t.Fatalf("cache should hold 20, got: %d", v)
	}
}

func TestRefreshIsNotReplacement(t *testing.T) {
	t.Parallel()

	// A fine resolution makes nearly every Get take the write-lock refresh
	// path, which re-stores the item through _s. That self-replacement must
	// not reach the deallocation callback.
	replaced := 0
	c := New[int, int](
		SetDefaultTTL(time.Minute),
		SetTimerResolution(time.Nanosecond),
		SetDeallocationFunc(func(key int, value int, reason DeallocationReason) {
			if reason == ReasonReplaced {
				replaced++
			}
		}),
	)
	defer c.Close()

	c.Set(1, 1, DefaultTTL)
	for range 100 {
		c.Get(1)
	}

	if replaced != 0 {
		t.Fatalf("refresh fired %d ReasonReplaced callbacks", replaced)
	}
}

func TestExplicitTTLFinerThanResolution(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		// A coarse default TTL used to drag short explicit TTLs up to its derived
		// resolution: a 200ms item in this cache lived for seconds.
		c := New[int, int](
			SetDefaultTTL(10*time.Second),
			DisableUpdateTime(true),
		)
		defer c.Close()

		c.Set(1, 1, 200*time.Millisecond)
		if _, ok := c.Get(1); !ok {
			t.Fatal("item missing right after Set")
		}

		synctest.Sleep(201 * time.Millisecond)
		if _, ok := c.Get(1); ok {
			t.Fatal("item with a 200ms TTL still alive after 201ms")
		}
	})
}

func TestExplicitTTLNoOptions(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		// Without a default TTL the clock used to fall back to one-second
		// rounding, so sub-second TTLs were honored a second late.
		c := New[int, int](DisableUpdateTime(true))
		defer c.Close()

		c.Set(1, 1, 100*time.Millisecond)

		synctest.Sleep(101 * time.Millisecond)
		if _, ok := c.Get(1); ok {
			t.Fatal("item with a 100ms TTL still alive after 101ms")
		}
	})
}

func TestShortTTLStillSlides(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		// Refresh batching is capped at half the item's own TTL, so an item finer
		// than the cache resolution still has its expiration pushed by Get.
		c := New[int, int](SetDefaultTTL(10 * time.Second))
		defer c.Close()

		c.Set(1, 1, time.Second)

		deadline := time.Now().Add(2500 * time.Millisecond)
		for time.Now().Before(deadline) {
			if _, ok := c.Get(1); !ok {
				t.Fatal("item expired while being read")
			}
			synctest.Sleep(25 * time.Millisecond)
		}
	})
}

func TestExplicitNanosecondIsNotDefaultTTL(t *testing.T) {
	t.Parallel()

	// The sentinel used to be 1ns, so an explicit 1ns TTL stored the default
	// instead -- and with no default configured it became NoTTL, immortal.
	c := New[int, int](SetDefaultTTL(time.Minute), DisableUpdateTime(true))
	defer c.Close()

	c.Set(1, 1, time.Nanosecond)
	time.Sleep(time.Millisecond)
	if _, ok := c.Get(1); ok {
		t.Fatal("a 1ns item took the default TTL instead of expiring")
	}

	c2 := New[int, int](DisableUpdateTime(true))
	defer c2.Close()

	c2.Set(1, 1, time.Nanosecond)
	time.Sleep(time.Millisecond)
	if _, ok := c2.Get(1); ok {
		t.Fatal("a 1ns item in a no-default cache became immortal")
	}
}

func TestDeallocationFuncTypeMismatchPanics(t *testing.T) {
	t.Parallel()

	// K and V are inferred from the callback, so a callback written against
	// different types than the cache cannot be caught until New binds it.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected a panic from a mismatched deallocation func")
		}

		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "SetDeallocationFunc") {
			t.Fatalf("panic should name the offending option, got: %v", r)
		}
	}()

	New[int, bool](SetDeallocationFunc(func(key string, value int, reason DeallocationReason) {}))
}
