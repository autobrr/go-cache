// Copyright (c) 2021 - 2026, the autobrr contributors.
// SPDX-License-Identifier: MIT

package ttlcache

import (
	"time"
)

func (c *Cache[K, V]) startExpirations() {
	timer := time.NewTimer(1 * time.Second)
	stopTimer(timer) // wasteful, but makes the loop cleaner because this is initialized.
	defer stopTimer(timer)

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
				restartTimer(timer, time.Until(timeSleep))
			}

		case <-timer.C:
			stopTimer(timer)
			c.expire()
			timeSleep = time.Time{}
		}
	}
}

func restartTimer(t *time.Timer, d time.Duration) {
	stopTimer(t)
	t.Reset(d)
}

func stopTimer(t *time.Timer) {
	t.Stop()

	// go < 1.23 returns stale values on expired timers.
	if len(t.C) != 0 {
		<-t.C
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
