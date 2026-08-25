// Copyright (c) 2021 - 2026, the autobrr contributors.
// SPDX-License-Identifier: MIT

package ttlcache

import "time"

func (c *Cache[K, V]) get(key K) (Item[V], bool) {
	c.l.RLock()
	defer c.l.RUnlock()
	return c._g(key)
}

func (c *Cache[K, V]) _g(key K) (Item[V], bool) {
	v, ok := c.m[key]
	if !ok {
		return v, ok
	}

	return v, ok
}

// getRefresh returns the item stored under key and pushes its expiration
// forward. The lookup and the write-back happen inside the same critical
// section: reading under RLock and writing the copy back afterwards let a Set
// that landed in between get overwritten by the stale copy.
func (c *Cache[K, V]) getRefresh(key K) (Item[V], bool) {
	c.l.RLock()
	it, ok := c._g(key)
	needs := ok && c.needsRefresh(it)
	c.l.RUnlock()

	if !needs {
		return it, ok
	}

	c.l.Lock()
	defer c.l.Unlock()

	// the item may have been replaced or deleted while the lock was upgraded.
	it, ok = c._g(key)
	if !ok || !c.needsRefresh(it) {
		return it, ok
	}

	return c._s(key, it), true
}

// needsRefresh reports whether a Get should push the item's expiration
// forward. Refreshes are batched to the cache resolution, capped at half the
// item's own TTL, so hot keys do not take the write lock on every read.
func (c *Cache[K, V]) needsRefresh(it Item[V]) bool {
	if c.o.noUpdateTime || it.t.IsZero() {
		return false
	}

	res := c.res
	if h := it.d / 2; h < res {
		res = h
	}

	_, t := c.getDuration(it.d)
	return t.Sub(it.t) > res
}

func (c *Cache[K, V]) set(key K, it Item[V]) Item[V] {
	c.l.Lock()
	defer c.l.Unlock()
	return c._s(key, it)
}

func (c *Cache[K, V]) _s(key K, it Item[V]) Item[V] {
	it.d, it.t = c.getDuration(it.d)
	c.m[key] = it
	c.wake(it.t)
	return it
}

// wake nudges the expiration loop to reconsider when it next sweeps.
//
// The send must not block. Callers hold the write lock and the loop takes that
// same lock in expire(), so a blocking send deadlocks the two against each
// other as soon as the channel fills: the sender waits for room while the loop
// waits for the lock.
//
// Dropping a nudge costs precision, not correctness. A full channel means a
// thousand wake-ups are already queued, so a sweep is coming regardless, and
// expire() re-derives the next wake-up from a full scan of the map. The worst
// case is that this item is collected on that sweep instead of exactly on time.
//
// Callers must hold the write lock; that is what makes the closed check sound
// against a concurrent close, and the send after it safe.
func (c *Cache[K, V]) wake(t time.Time) {
	if c.closed {
		return // the loop is gone; nothing left to wake.
	}

	if t.IsZero() {
		return // NoTTL never expires, and the loop discards these anyway.
	}

	select {
	case c.ch <- t:
	default:
	}
}

func (c *Cache[K, V]) getOrSet(key K, it Item[V]) (Item[V], bool) {
	c.l.Lock()
	defer c.l.Unlock()
	return c._gos(key, it)
}

func (c *Cache[K, V]) _gos(key K, it Item[V]) (Item[V], bool) {
	if g, ok := c._g(key); ok {
		return g, ok
	}

	return c._s(key, it), true
}

// delete removes key and then runs the deallocation callback outside the
// lock: a callback that calls back into the cache would otherwise deadlock
// against the write lock held here.
func (c *Cache[K, V]) delete(key K, reason DeallocationReason) {
	c.l.Lock()
	v, ok := c.m[key]
	if !ok {
		c.l.Unlock()
		return
	}

	delete(c.m, key)
	c.l.Unlock()

	if c.deallocationFn != nil {
		c.deallocationFn(key, v.v, reason)
	}
}

func (c *Cache[K, V]) getkeys() []K {
	c.l.RLock()
	defer c.l.RUnlock()

	keys := make([]K, 0, len(c.m))
	for k := range c.m {
		keys = append(keys, k)
	}

	return keys
}

func (c *Cache[K, V]) close() {
	c.l.Lock()
	defer c.l.Unlock()

	if c.closed {
		return
	}

	c.closed = true
	close(c.ch)
}

func (c *Cache[K, V]) getDuration(d time.Duration) (time.Duration, time.Time) {
	switch d {
	case NoTTL:
	case DefaultTTL:
		return c.o.defaultTTL, time.Now().Add(c.o.defaultTTL)
	default:
		return d, time.Now().Add(d)
	}

	return NoTTL, time.Time{}
}

func (i *Item[V]) getDuration() time.Duration {
	return i.d
}

func (i *Item[V]) getTime() time.Time {
	return i.t
}

func (i *Item[V]) getValue() V {
	return i.v
}
