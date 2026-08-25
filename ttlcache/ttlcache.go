// Copyright (c) 2021 - 2026, the autobrr contributors.
// SPDX-License-Identifier: MIT

package ttlcache

import (
	"fmt"
	"iter"
	"slices"
	"sync"
	"time"
)

const NoTTL time.Duration = 0
const DefaultTTL time.Duration = time.Nanosecond * 1

type Cache[K comparable, V any] struct {
	l              sync.RWMutex
	o              Options
	res            time.Duration // refresh batching granularity; see SetTimerResolution.
	next           time.Time     // earliest deadline of any stored item; guarded by l. Stale-early after deletes, never late.
	ch             chan struct{} // coalesced wake signal for the expiration loop; see wake.
	done           chan struct{} // closed when the expiration loop returns; Close waits on it.
	m              map[K]Item[V]
	deallocationFn DeallocationFunc[K, V]
	closed         bool
}

// deallocation carries a removed entry out of the critical section so its
// callback can run after the lock is released.
type deallocation[K comparable, V any] struct {
	key   K
	value V
}

type Item[V any] struct {
	t time.Time
	d time.Duration
	v V
}

// Options carries the settings an Option can reach. It is deliberately not
// generic: that is what lets Option be a plain func(*Options), so options need
// no type arguments at the call site. The price is that a deallocation func
// has nowhere typed to live here, and is held as any until New binds it to the
// cache's own K and V.
type Options struct {
	defaultTTL        time.Duration
	defaultResolution time.Duration
	noUpdateTime      bool
	deallocationFunc  any // DeallocationFunc[K, V], bound in New.
}

// Option configures a Cache at construction. See SetDefaultTTL,
// SetTimerResolution and DisableUpdateTime.
type Option func(*Options)

type DeallocationReason int

const (
	ReasonTimedOut = DeallocationReason(iota)
	ReasonDeleted  = DeallocationReason(iota)
	ReasonReplaced = DeallocationReason(iota)
)

type DeallocationFunc[K comparable, V any] func(key K, value V, reason DeallocationReason)

func New[K comparable, V any](opts ...Option) *Cache[K, V] {
	var options Options
	for _, opt := range opts {
		opt(&options)
	}

	c := Cache[K, V]{
		o:    options,
		ch:   make(chan struct{}, 1),
		done: make(chan struct{}),
		m:    make(map[K]Item[V]),
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

// GetOrSet returns the value stored under key if present, or stores value
// under it. The bool reports whether the key was already present, like
// sync.Map.LoadOrStore. Unlike Get, a hit does not push the expiration.
func (c *Cache[K, V]) GetOrSet(key K, value V, duration time.Duration) (V, bool) {
	it, loaded := c.GetOrSetItem(key, value, duration)
	return it.GetValue(), loaded
}

func (c *Cache[K, V]) fixupDuration(duration time.Duration) time.Duration {
	if c.o.defaultTTL == NoTTL && duration == DefaultTTL {
		return NoTTL
	}

	return duration
}

// GetOrSetItem is GetOrSet returning the full item; the bool again means the
// key was already present.
func (c *Cache[K, V]) GetOrSetItem(key K, value V, duration time.Duration) (Item[V], bool) {
	return c.getOrSet(key, Item[V]{v: value, d: c.fixupDuration(duration)})
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

// All returns an iterator over the key/value pairs in the cache. The keys are
// a snapshot taken when All is called; each value is fetched as the body
// reaches its key, so an entry that left the cache in between is skipped, and
// a key deleted and stored again yields its current value, not the one from
// snapshot time. An entry past its deadline counts as gone, like everywhere
// else.
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

// Close stops the expiration goroutine and waits for it to return, including
// any deallocation callback it is running, so resources the callback uses can
// be torn down safely once Close returns. Close must not be called from any
// deallocation callback: one receiving ReasonTimedOut cannot tell whether it
// is on the expiration goroutine, where the wait would deadlock. Items stored
// after Close are kept but never expire -- no sweeper is left to collect
// them -- so writers and iterating bodies should be done before closing.
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

// SetDeallocationFunc registers f to run whenever an item leaves the cache:
// it timed out, was deleted, or was displaced by a Set storing a new value
// under its key. f runs after the item is already gone and outside the
// cache's lock, so it may call back into the cache -- except Close; see
// there. It runs on whichever goroutine removed the item: sweeps invoke it on
// the expiration goroutine, while a Set, GetOrSet, or Delete that collects an
// entry invokes it on the caller, so invocations may run concurrently and f
// must be safe for that. Re-storing a value that is already cached counts as
// replacing it, and items still in the cache at Close are not deallocated.
// A value whose deadline had already passed when it was removed is always
// reported as timed out, however it left the cache.
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
