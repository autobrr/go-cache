// Copyright (c) 2021 - 2026, the autobrr contributors.
// SPDX-License-Identifier: MIT

package timecache

import (
	"sync"
	"time"
)

type Cache struct {
	m sync.RWMutex
	t time.Time
	o Options
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

	c := Cache{
		o: options,
	}

	return &c
}

func (t *Cache) Now() time.Time {
	t.m.RLock()
	if !t.t.IsZero() {
		defer t.m.RUnlock()
		return t.t
	}

	t.m.RUnlock()
	return t.update()
}

func (t *Cache) update() time.Time {
	t.m.Lock()
	defer t.m.Unlock()
	if !t.t.IsZero() {
		return t.t
	}

	var d time.Duration
	if t.o.round > time.Nanosecond {
		d = t.o.round
	} else {
		d = time.Second * 1
	}

	t.t = time.Now().Round(d)

	go func(duration time.Duration) {
		if t.o.round > time.Nanosecond {
			duration = t.o.round / 2
		}

		time.Sleep(duration)
		t.reset()
	}(d)

	return t.t
}

func (t *Cache) reset() {
	t.m.Lock()
	defer t.m.Unlock()
	t.t = time.Time{}
}

// Round sets the resolution Now is rounded to; the cached value lives for
// half of it. Anything at or below a nanosecond, including unset, falls back
// to a second.
func Round(d time.Duration) Option {
	return func(o *Options) {
		o.round = d
	}
}
