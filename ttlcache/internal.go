// Copyright (c) 2021 - 2026, the autobrr contributors.
// SPDX-License-Identifier: MIT

package ttlcache

import "time"

func (c *Cache[K, V]) get(key K) (Item[V], bool) {
	c.l.RLock()
	defer c.l.RUnlock()
	return c._g(key)
}

// _g treats an entry past its deadline as missing: the sweep may lag, and a
// read must not observe -- or resurrect -- an item that already timed out.
func (c *Cache[K, V]) _g(key K) (Item[V], bool) {
	v, ok := c.m[key]
	if !ok || v.expired(time.Now()) {
		return Item[V]{}, false
	}

	return v, true
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

// set stores it under key and hands any displaced value to the deallocation
// callback outside the lock. A displaced value that was still live is
// reported as replaced; one whose deadline had already passed timed out, the
// store merely collected it before the sweep did. getRefresh deliberately
// bypasses this by calling _s directly: re-stamping an item is not a
// replacement.
func (c *Cache[K, V]) set(key K, it Item[V]) Item[V] {
	c.l.Lock()
	old, displaced := c.m[key]
	it = c._s(key, it)
	c.l.Unlock()

	if displaced && c.deallocationFn != nil {
		reason := ReasonReplaced
		if old.expired(time.Now()) {
			reason = ReasonTimedOut
		}

		c.deallocationFn(key, old.v, reason)
	}

	return it
}

func (c *Cache[K, V]) _s(key K, it Item[V]) Item[V] {
	it.d, it.t = c.getDuration(it.d)
	c.m[key] = it

	if !it.t.IsZero() && (c.next.IsZero() || c.next.After(it.t)) {
		c.next = it.t
		c.wake()
	}

	return it
}

// wake nudges the expiration loop to re-read next.
//
// The send must not block. Callers hold the write lock and the loop takes that
// same lock in expire(), so a blocking send deadlocks the two against each
// other as soon as the channel fills: the sender waits for room while the loop
// waits for the lock.
//
// Dropping the send is safe in a way it was not when the channel carried
// deadlines: next is updated under the lock before the send is attempted, so
// a full channel means a signal is already pending, and whenever the loop
// gets to it, it re-reads a next that includes this caller's update.
//
// Callers must hold the write lock; that is what makes the closed check sound
// against a concurrent close, and the send after it safe.
func (c *Cache[K, V]) wake() {
	if c.closed {
		return // the loop is gone; nothing left to wake.
	}

	select {
	case c.ch <- struct{}{}:
	default:
	}
}

// getOrSet returns the existing item and true, or stores it and returns the
// new item and false. Storing displaces at most an expired corpse the read
// treated as missing; it is handed to the deallocation callback as a timeout.
func (c *Cache[K, V]) getOrSet(key K, it Item[V]) (Item[V], bool) {
	c.l.Lock()
	if g, ok := c._g(key); ok {
		c.l.Unlock()
		return g, true
	}

	old, displaced := c.m[key]
	it = c._s(key, it)
	c.l.Unlock()

	if displaced && c.deallocationFn != nil {
		c.deallocationFn(key, old.v, ReasonTimedOut)
	}

	return it, false
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
		if v.expired(time.Now()) {
			reason = ReasonTimedOut
		}

		c.deallocationFn(key, v.v, reason)
	}
}

func (c *Cache[K, V]) getkeys() []K {
	c.l.RLock()
	defer c.l.RUnlock()

	now := time.Now()
	keys := make([]K, 0, len(c.m))
	for k, v := range c.m {
		if v.expired(now) {
			continue
		}

		keys = append(keys, k)
	}

	return keys
}

// close stops the loop and waits for it to exit. The wait must happen after
// the lock is released: the loop may need that lock to finish a sweep before
// it can observe the closed channel.
func (c *Cache[K, V]) close() {
	c.l.Lock()
	already := c.closed
	c.closed = true
	if !already {
		close(c.ch)
	}
	c.l.Unlock()

	<-c.done
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

// expired reports whether the item's deadline has passed; NoTTL items never
// expire.
func (i Item[V]) expired(now time.Time) bool {
	return !i.t.IsZero() && !i.t.After(now)
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
