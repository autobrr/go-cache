// Copyright (c) 2021 - 2026, the autobrr contributors.
// SPDX-License-Identifier: MIT

package ttlcache

import (
	"time"
)

func (c *Cache[K, V]) startExpirations() {
	timer := time.NewTimer(1 * time.Second)
	timer.Stop() // wasteful, but makes the loop cleaner because this is initialized.
	defer timer.Stop()

	var timeSleep time.Time
	for {
		select {
		case t, ok := <-c.ch:
			if !ok {
				return
			} else if t.IsZero() {
				continue
			}

			if timeSleep.IsZero() || timeSleep.After(t) {
				timeSleep = t
				// Since Go 1.23 a receive after Reset cannot deliver a value
				// from the previous setting, so Reset alone is enough.
				timer.Reset(time.Until(timeSleep))
			}

		case <-timer.C:
			c.expire()
			timeSleep = time.Time{}
		}
	}
}

// expire sweeps the map and removes every item whose time has passed. The
// deallocation callbacks run after the lock is released so they may call back
// into the cache; see delete.
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
	c.wake(soon) // wake-up feedback loop
	c.l.Unlock()

	for _, d := range timedOut {
		c.deallocationFn(d.key, d.value, ReasonTimedOut)
	}
}
