// Copyright (c) 2021 - 2026, the autobrr contributors.
// SPDX-License-Identifier: MIT

package ttlcache

import (
	"sync"
	"time"

	"github.com/autobrr/go-cache/timecache"
)

const NoTTL time.Duration = 0
const DefaultTTL time.Duration = time.Nanosecond * 1

type Cache[K comparable, V any] struct {
	tc             timecache.Cache
	l              sync.RWMutex
	o              Options
	ch             chan time.Time
	m              map[K]Item[V]
	deallocationFn DeallocationFunc[K, V]
	closed         bool
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
)

type DeallocationFunc[K comparable, V any] func(key K, value V, reason DeallocationReason)
