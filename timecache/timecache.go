// Copyright (c) 2021 - 2026, the autobrr contributors.
// SPDX-License-Identifier: MIT

package timecache

import (
	"sync/atomic"
	"time"
)

type Cache struct {
	t atomic.Pointer[time.Time]
	d time.Duration
}

// Options carries the settings an Option can reach.
type Options struct {
	round time.Duration
}

// Option configures a Cache at construction. See Round.
type Option func(*Options)

func New(opts ...Option) *Cache {
	var options Options
	for _, opt := range opts {
		opt(&options)
	}

	d := options.round
	if d <= time.Nanosecond {
		d = time.Second
	}

	return &Cache{d: d}
}

func (c *Cache) Now() time.Time {
	for {
		if p := c.t.Load(); p != nil {
			return *p
		}

		now := time.Now().Round(c.d)
		if p := &now; c.t.CompareAndSwap(nil, p) {
			go c.expire(p, c.d)
			return now
		}
	}
}

func (c *Cache) expire(p *time.Time, d time.Duration) {
	if d > time.Nanosecond {
		d /= 2
	}

	time.Sleep(d)
	c.t.CompareAndSwap(p, nil)
}

// Round sets the resolution Now is rounded to; the cached value lives for
// half of it. Anything at or below a nanosecond, including unset, falls back
// to a second.
func Round(d time.Duration) Option {
	return func(o *Options) {
		o.round = d
	}
}
