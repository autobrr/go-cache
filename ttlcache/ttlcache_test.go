// Copyright (c) 2021 - 2026, the autobrr contributors.
// SPDX-License-Identifier: MIT

package ttlcache

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGet(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(1 * time.Second))
	defer c.Close()

	for i := 0; i < 10; i++ {
		c.Set(i, true, DefaultTTL)
	}

	for i := 0; i < 10; i++ {
		val, ok := c.Get(i)
		if !ok {
			t.Fatalf("missing key: %d", i)
		} else if !val {
			t.Fatalf("bad value on key: %d", i)
		}
	}
}

func TestExpirations(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(200 * time.Millisecond))
	defer c.Close()
	for i := 0; i < 10; i++ {
		c.Set(i, true, DefaultTTL)
	}

	time.Sleep(1 * time.Second)

	for i := 0; i < 10; i++ {
		if _, ok := c.Get(i); ok {
			t.Fatalf("found key: %d", i)
		}
	}
}

func TestSwaps(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(200 * time.Millisecond))
	defer c.Close()
	for i := 0; i < 10; i++ {
		c.Set(i, true, DefaultTTL)
	}

	time.Sleep(1 * time.Second)
	for i := 0; i < 10; i++ {
		if _, ok := c.Get(i); ok {
			t.Fatalf("found key: %d", i)
		}
	}

	for i := 10; i < 20; i++ {
		c.Set(i, true, DefaultTTL)
		if _, ok := c.Get(i); !ok {
			t.Fatalf("missing key: %d", i)
		}
	}
}

func TestRetimer(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(200 * time.Millisecond))
	defer c.Close()
	for i := 1; i < 10; i++ {
		c.Set(i, true, time.Duration(10-i)*100*time.Millisecond)
	}

	time.Sleep(2 * time.Second)
	for i := 1; i < 10; i++ {
		if _, ok := c.Get(i); ok {
			t.Fatalf("found key: %d", i)
		}
	}
}

func TestSchedule(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(1 * time.Second))
	defer c.Close()
	for i := 1; i < 10; i++ {
		c.Set(i, true, time.Duration(i)*100*time.Millisecond)
	}

	time.Sleep(3 * time.Second)
	for i := 1; i < 10; i++ {
		if _, ok := c.Get(i); ok {
			t.Fatalf("found key: %d", i)
		}
	}
}

func TestInterlace(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(100 * time.Millisecond))
	defer c.Close()
	swap := false
	for i := 0; i < 10; i++ {
		swap = !swap
		ttl := DefaultTTL
		if swap {
			ttl = NoTTL
		}
		c.Set(i, true, ttl)
	}

	time.Sleep(1 * time.Second)
	swap = false
	for i := 0; i < 10; i++ {
		swap = !swap
		if !swap {
			continue
		}

		if _, ok := c.Get(i); !ok {
			t.Fatalf("found key: %d", i)
		}
	}
}

func TestReschedule(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(100 * time.Millisecond))
	defer c.Close()
	for i := 1; i < 10; i++ {
		c.Set(i, true, NoTTL)
		c.Set(i, true, DefaultTTL)
	}

	time.Sleep(1 * time.Second)
	for i := 1; i < 10; i++ {
		if _, ok := c.Get(i); ok {
			t.Fatalf("found key: %d", i)
		}
	}
}

func TestRescheduleNoTTL(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(100 * time.Millisecond))
	defer c.Close()
	for i := 1; i < 10; i++ {
		c.Set(i, true, DefaultTTL)
		c.Set(i, true, NoTTL)
	}

	time.Sleep(1 * time.Second)
	for i := 1; i < 10; i++ {
		if _, ok := c.Get(i); !ok {
			t.Fatalf("found key: %d", i)
		}
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(100 * time.Millisecond))
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
	var hit atomic.Bool // the deallocation runs on the expiration goroutine.
	o := Options[int, bool]{}.
		SetDefaultTTL(time.Millisecond * 100).
		SetDeallocationFunc(func(key int, value bool, reason DeallocationReason) { hit.Store(reason == ReasonTimedOut) })

	c := New[int, bool](o)
	defer c.Close()

	for i := 0; i < 1; i++ {
		c.Set(i, true, DefaultTTL)
	}

	time.Sleep(3 * time.Second)
	if !hit.Load() {
		t.Fatalf("Deallocation not hit.")
	}
}

func TestDeallocationDeleted(t *testing.T) {
	t.Parallel()
	hit := false
	o := Options[int, bool]{}.
		SetDefaultTTL(time.Millisecond * 100).
		SetDeallocationFunc(func(key int, value bool, reason DeallocationReason) { hit = reason == ReasonDeleted })

	c := New[int, bool](o)
	defer c.Close()

	for i := 0; i < 1; i++ {
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

	c := New[int, bool](Options[int, bool]{}.
		SetDefaultTTL(time.Millisecond * 100).
		SetDeallocationFunc(func(key int, value bool, reason DeallocationReason) { ch <- struct{}{} }))

	defer c.Close()

	const base = 0
	const rounds = 1
	for i := base; i < rounds; i++ {
		c.Set(i, true, DefaultTTL)
	}

	for i := base; i < rounds; i++ {
		<-ch
	}

	for i := 0; i < 1; i++ {
		c.Set(i, true, DefaultTTL)
	}

	for i := base; i < rounds; i++ {
		<-ch
	}
}

func TestGetDoesNotClobberSet(t *testing.T) {
	t.Parallel()

	// A fine timer resolution makes the TTL refresh in GetItem fire on nearly
	// every Get, which is what exposes the read-then-write race.
	c := New[int, int](Options[int, int]{}.
		SetDefaultTTL(time.Minute).
		SetTimerResolution(10 * time.Microsecond))

	defer c.Close()

	const key = 1
	const rounds = 200000

	lost := 0
	for i := 0; i < rounds; i++ {
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

func TestGetKeys(t *testing.T) {
	t.Parallel()
	c := New[int, bool](Options[int, bool]{}.SetDefaultTTL(1 * time.Second))
	defer c.Close()

	const count = 10
	for i := 0; i < count; i++ {
		c.Set(i, true, DefaultTTL)
	}

	keys := c.GetKeys()
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

func TestSetDoesNotDeadlockOnFullWakeChannel(t *testing.T) {
	t.Parallel()

	// A short TTL keeps the expiration loop cycling through expire(), which
	// takes the same write lock a Set holds while it nudges the wake channel.
	// Enough concurrent writers starve the loop at that lock long enough for
	// the channel to fill behind it.
	c := New[int, int](Options[int, int]{}.SetDefaultTTL(2 * time.Millisecond))

	const writers = 8
	const perWriter = 25000

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < perWriter; i++ {
					c.Set(w*perWriter+i, i, DefaultTTL)
				}
			}(w)
		}
		wg.Wait()
	}()

	select {
	case <-done:
		// A dropped nudge may delay collection, but every sweep re-derives the
		// next wake-up from the whole map, so nothing is stranded.
		deadline := time.Now().Add(30 * time.Second)
		for len(c.GetKeys()) > 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}

		if n := len(c.GetKeys()); n != 0 {
			t.Fatalf("%d entries never expired after dropped wake-ups", n)
		}

		c.Close()
	case <-time.After(30 * time.Second):
		// Close would block on the held lock too, so leave the cache alone.
		t.Fatal("deadlock: Set is blocked sending on the wake channel while holding the write lock")
	}
}
