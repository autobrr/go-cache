// Copyright (c) 2021 - 2026, the autobrr contributors.
// SPDX-License-Identifier: MIT

package ttlcache

import (
	"fmt"
	"iter"
	"slices"
	"time"
)

func New[K comparable, V any](opts ...Option) *Cache[K, V] {
	var options Options
	for _, opt := range opts {
		opt(&options)
	}

	c := Cache[K, V]{
		o:  options,
		ch: make(chan time.Time, 1000),
		m:  make(map[K]Item[V]),
	}

	if options.deallocationFunc != nil {
		f, ok := options.deallocationFunc.(DeallocationFunc[K, V])
		if !ok {
			panic(fmt.Sprintf("ttlcache: SetDeallocationFunc was given a %T, but this cache needs a %T",
				options.deallocationFunc, DeallocationFunc[K, V](nil)))
		}

		c.deallocationFn = f
	}

	// an unset resolution follows the default TTL; without either, batch
	// refreshes to a second.
	c.res = options.defaultResolution
	if c.res == 0 {
		c.res = options.defaultTTL / 2
	}
	if c.res <= 0 {
		c.res = time.Second
	}

	go c.startExpirations()
	return &c
}

func (c *Cache[K, V]) Get(key K) (V, bool) {
	it, ok := c.GetItem(key)
	if !ok {
		return *new(V), ok
	}

	return it.GetValue(), ok
}

func (c *Cache[K, V]) GetItem(key K) (Item[V], bool) {
	return c.getRefresh(key)
}

func (c *Cache[K, V]) GetOrSet(key K, value V, duration time.Duration) (V, bool) {
	it, ok := c.GetOrSetItem(key, value, duration)
	if !ok {
		return *new(V), ok
	}

	return it.GetValue(), ok
}

func (c *Cache[K, V]) fixupDuration(duration time.Duration) time.Duration {
	if c.o.defaultTTL == NoTTL && duration == DefaultTTL {
		return NoTTL
	}

	return duration
}

func (c *Cache[K, V]) GetOrSetItem(key K, value V, duration time.Duration) (Item[V], bool) {
	it, ok := c.getOrSet(key, Item[V]{v: value, d: c.fixupDuration(duration)})
	if !ok {
		return Item[V]{}, ok
	}

	return it, ok
}

func (c *Cache[K, V]) Set(key K, value V, duration time.Duration) bool {
	c.SetItem(key, value, duration)
	return true
}

func (c *Cache[K, V]) SetItem(key K, value V, duration time.Duration) Item[V] {
	return c.set(key, Item[V]{v: value, d: c.fixupDuration(duration)})
}

func (c *Cache[K, V]) Delete(key K) {
	c.delete(key, ReasonDeleted)
}

// GetKeys returns the keys as a slice the caller owns.
//
// Deprecated: use Keys.
func (c *Cache[K, V]) GetKeys() []K {
	return c.getkeys()
}

// Keys returns an iterator over the keys in the cache.
//
// The cache is not locked while the loop body runs, so the body may call back
// into the cache -- the same reason deallocation callbacks run outside the
// lock. The keys are a snapshot taken when Keys is called, so an entry may
// leave the cache before the body reaches it.
func (c *Cache[K, V]) Keys() iter.Seq[K] {
	return slices.Values(c.getkeys())
}

// All returns an iterator over the key/value pairs in the cache. Entries that
// left the cache between the snapshot and the body reaching them are skipped.
// Like Get, that means removed, not stale: an entry past its TTL that the
// sweep has not collected yet is still yielded.
//
// Unlike Get, iterating does not push expirations forward: ranging over the
// cache to inspect it would otherwise slide every item's TTL.
func (c *Cache[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for _, k := range c.getkeys() {
			it, ok := c.get(k) // c.get, not GetItem: observing must not refresh.
			if !ok {
				continue
			}

			if !yield(k, it.v) {
				return
			}
		}
	}
}

// Close stops the expiration goroutine. Items stored after Close are kept but
// never expire -- no sweeper is left to collect them -- so writers and
// iterating bodies should be done before closing.
func (c *Cache[K, V]) Close() {
	c.close()
}

func (i *Item[V]) GetDuration() time.Duration {
	return i.getDuration()
}

func (i *Item[V]) GetTime() time.Time {
	return i.getTime()
}

func (i *Item[V]) GetValue() V {
	return i.getValue()
}

// SetTimerResolution sets how often a Get may push an item's expiration
// forward. It defaults to half the default TTL.
func SetTimerResolution(d time.Duration) Option {
	return func(o *Options) {
		o.defaultResolution = d
	}
}

// SetDefaultTTL sets the duration applied to items stored with DefaultTTL.
func SetDefaultTTL(d time.Duration) Option {
	return func(o *Options) {
		o.defaultTTL = d
	}
}

// DisableUpdateTime stops a Get from extending the item's expiration.
func DisableUpdateTime(val bool) Option {
	return func(o *Options) {
		o.noUpdateTime = val
	}
}

// SetDeallocationFunc registers f to run whenever an item leaves the cache,
// whether it timed out or was deleted. f runs after the item is already gone
// and outside the cache's lock, so it may call back into the cache; timeouts
// invoke it on the expiration goroutine.
//
// K and V are inferred from f, so the call site never spells them out. Unlike
// the other options this one cannot be checked at compile time: Options is not
// generic, so f travels as any and New binds it to the cache's own K and V.
// A callback that disagrees with the cache panics there, at construction.
func SetDeallocationFunc[K comparable, V any](f DeallocationFunc[K, V]) Option {
	return func(o *Options) {
		o.deallocationFunc = f
	}
}
