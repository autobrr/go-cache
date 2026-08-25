// Copyright (c) 2021 - 2026, the autobrr contributors.
// SPDX-License-Identifier: MIT

package ttlcache

import (
	"time"
)

func (c *Cache[K, V]) startExpirations() {
	defer close(c.done)

	timer := time.NewTimer(1 * time.Second)
	timer.Stop() // wasteful, but makes the loop cleaner because this is initialized.
	defer timer.Stop()

	for {
		select {
		case _, ok := <-c.ch:
			if !ok {
				return
			}

		case <-timer.C:
			c.expire()
		}

		// re-arm from the earliest known deadline, whether a store just
		// lowered it or a sweep just recomputed it. Since Go 1.23 a receive
		// after Reset cannot deliver a value from the previous setting, so
		// Reset alone is enough.
		c.l.RLock()
		next := c.next
		c.l.RUnlock()

		if next.IsZero() {
			timer.Stop()
		} else {
			timer.Reset(time.Until(next))
		}
	}
}

// expire sweeps the map, removes every item whose time has passed, and
// leaves the earliest surviving deadline in next for the loop to re-arm
// from. The deallocation callbacks run after the lock is released so they
// may call back into the cache; see delete.
func (c *Cache[K, V]) expire() {
	t := time.Now()
	var soon time.Time
	var timedOut []deallocation[K, V]

	c.l.Lock()
	for k, v := range c.m {
		if v.t.IsZero() {
			continue
		} else if v.t.After(t) {
			if soon.IsZero() || soon.After(v.t) {
				soon = v.t
			}
			continue
		}

		delete(c.m, k)
		if c.deallocationFn != nil {
			timedOut = append(timedOut, deallocation[K, V]{key: k, value: v.v})
		}
	}
	c.next = soon
	c.l.Unlock()

	for _, d := range timedOut {
		c.deallocationFn(d.key, d.value, ReasonTimedOut)
	}
}
