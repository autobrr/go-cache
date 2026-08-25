// Copyright (c) 2021 - 2026, the autobrr contributors.
// SPDX-License-Identifier: MIT

package ttlcache

import (
	"fmt"
	"time"

	"github.com/autobrr/go-cache/timecache"
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

	if options.defaultTTL != NoTTL && options.defaultResolution == 0 {
		c.tc = *timecache.New(timecache.Options{}.Round(options.defaultTTL / 2))
	} else if options.defaultResolution != 0 {
		c.tc = *timecache.New(timecache.Options{}.Round(options.defaultResolution))
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

func (c *Cache[K, V]) GetKeys() []K {
	return c.getkeys()
}

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

// SetTimerResolution sets how coarsely the cache reads the clock. It defaults
// to half the default TTL.
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
// whether it timed out or was deleted.
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
