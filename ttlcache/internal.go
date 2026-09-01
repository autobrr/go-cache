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
// It reads the monotonic clock at most once, and not at all for NoTTL items.
func (c *Cache[K, V]) _g(key K) (Item[V], bool) {
	v, ok := c.m[key]
	if !ok {
		return Item[V]{}, false
	}

	if v.d != NoTTL && time.Until(v.t) <= 0 {
		return Item[V]{}, false
	}

	return v, true
}

// getRefresh returns the item stored under key and pushes its expiration
// forward. A refresh rechecks the item under the write lock before storing it:
// writing the first read's copy would overwrite a Set that landed while the
// lock was being upgraded.
func (c *Cache[K, V]) getRefresh(key K) (Item[V], bool) {
	c.l.RLock()
	it, ok := c.m[key]
	if !ok {
		c.l.RUnlock()
		return Item[V]{}, false
	}
	if it.d == NoTTL {
		c.l.RUnlock()
		return it, true
	}

	remaining := time.Until(it.t)
	if remaining <= 0 {
		c.l.RUnlock()
		return Item[V]{}, false
	}
	needs := c.needsRefresh(it, remaining)
	c.l.RUnlock()

	if !needs {
		return it, ok
	}
	return c.refresh(key)
}

// refresh completes the uncommon write-lock half of getRefresh. Keeping it
// separate leaves the read-only hit path free of write-back bookkeeping.
func (c *Cache[K, V]) refresh(key K) (Item[V], bool) {
	c.l.Lock()
	defer c.l.Unlock()

	// the item may have been replaced, deleted, or expired while the lock was
	// upgraded, so nothing from the read pass is trusted. The clock is read
	// once, under the lock, and serves both the recheck and the new stamp.
	now := time.Now()
	it, ok := c.m[key]
	if !ok || it.expired(now) {
		return Item[V]{}, false
	}

	if it.d == NoTTL || !c.needsRefresh(it, it.t.Sub(now)) {
		return it, true
	}

	return c._s(key, it, now), true
}

// needsRefresh reports whether a Get should push the item's expiration
// forward. Refreshes are batched to the cache resolution, capped at half the
// item's own TTL, so hot keys do not take the write lock on every read.
func (c *Cache[K, V]) needsRefresh(it Item[V], remaining time.Duration) bool {
	if c.o.noUpdateTime {
		return false
	}

	res := c.res
	if h := it.d / 2; h < res {
		res = h
	}

	return it.d-remaining > res
}

// set stores it under key and hands any displaced value to the deallocation
// callback outside the lock. A displaced value that was still live is
// reported as replaced; one whose deadline had already passed timed out, the
// store merely collected it before the sweep did. getRefresh deliberately
// bypasses this by calling _s directly: re-stamping an item is not a
// replacement.
func (c *Cache[K, V]) set(key K, it Item[V]) Item[V] {
	c.l.Lock()
	// the clock is read under the lock so stamps are monotone in lock order:
	// sampled outside, a writer that queued longer stores an earlier deadline
	// than next, and the pointless wake signals convoy the loop against the
	// writers.
	now := time.Now()
	old, displaced := c.m[key]
	reason := ReasonReplaced
	if displaced && old.expired(now) {
		// classified at removal, while the lock is still held: judging after
		// the unlock would let a deadline passing in between report a value
		// that was displaced live as timed out.
		reason = ReasonTimedOut
	}
	it = c._s(key, it, now)
	c.l.Unlock()

	if displaced && c.deallocationFn != nil {
		c.deallocationFn(key, old.v, reason)
	}

	return it
}

func (c *Cache[K, V]) _s(key K, it Item[V], now time.Time) Item[V] {
	it.d, it.t = c.getDuration(it.d, now)
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
	now := time.Now() // under the lock; see set.
	old, present := c.m[key]
	if present && !old.expired(now) {
		c.l.Unlock()
		return old, true
	}

	it = c._s(key, it, now)
	c.l.Unlock()

	if present && c.deallocationFn != nil {
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

	if v.expired(time.Now()) {
		// classified at removal, while the lock is still held; see set.
		reason = ReasonTimedOut
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

// getDuration resolves the TTL an item was stored with into the duration and
// deadline it carries. DefaultTTL takes the configured default, and a default
// of NoTTL means exactly that: resolving it here rather than at the public
// entry points keeps every store path from minting an entry with no TTL but
// a deadline.
func (c *Cache[K, V]) getDuration(d time.Duration, now time.Time) (time.Duration, time.Time) {
	if d == DefaultTTL {
		d = c.o.defaultTTL
	}

	if d == NoTTL {
		return NoTTL, time.Time{}
	}

	return d, now.Add(d)
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
