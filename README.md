# go-cache

Caching packages used across autobrr: a generic TTL cache, a coarse cached
clock, and a compiled-regexp cache.

Requires Go 1.27.

```bash
go get github.com/autobrr/go-cache
```

## ttlcache

An in-memory generic TTL cache with a single background expiration goroutine.

- per-item TTLs, a configurable default, and `NoTTL` for items that never expire
- reads never return an entry past its deadline, even before the sweep collects it
- `Get` pushes an item's expiration forward (sliding TTL); disable with `DisableUpdateTime(true)`
- a deallocation callback runs whenever an item times out, is deleted, or is displaced by a `Set`; it runs outside the cache lock, on whichever goroutine removed the item, and may call back into the cache (but not `Close`)
- `Keys` and `All` iterate over a key snapshot without holding the lock while the loop body runs; `All` fetches each value at visit time, skipping entries that left in between
- `GetOrSet` stores only when the key is absent and reports whether it found or stored; `GetItem` exposes the stored duration and deadline
- `Close` stops the expiration goroutine

```go
package main

import (
	"fmt"
	"time"

	"github.com/autobrr/go-cache/ttlcache"
)

func main() {
	c := ttlcache.New[string, int](
		ttlcache.SetDefaultTTL(5 * time.Minute),
	)
	defer c.Close()

	c.Set("a", 1, ttlcache.DefaultTTL) // expires in 5 minutes
	c.Set("b", 2, 30*time.Second)      // explicit TTL
	c.Set("c", 3, ttlcache.NoTTL)      // never expires

	if v, ok := c.Get("a"); ok { // pushes a's expiration forward
		fmt.Println(v)
	}

	for k, v := range c.All() {
		fmt.Println(k, v)
	}
}
```

Options:

- `SetDefaultTTL(d)` sets the duration applied to items stored with `DefaultTTL`.
- `SetTimerResolution(d)` sets how often a `Get` may push an item's expiration
  forward. Defaults to half the default TTL.
- `DisableUpdateTime(true)` stops a `Get` from extending the item's expiration.
- `SetDeallocationFunc(f)` registers a callback for items leaving the cache,
  with the reason (`ReasonTimedOut`, `ReasonDeleted` or `ReasonReplaced`):

```go
c := ttlcache.New[string, net.Conn](
	ttlcache.SetDefaultTTL(time.Minute),
	ttlcache.SetDeallocationFunc(func(key string, conn net.Conn, reason ttlcache.DeallocationReason) {
		conn.Close()
	}),
)
```

The callback's key and value types must match the cache's; a mismatch panics
in `New`, since `Options` is not generic and cannot catch it at compile time.

## timecache

A cached clock for hot paths that read the time constantly and can settle for
coarse answers. `Now` returns `time.Now()` rounded to the configured
resolution and serves the cached value for half that long.

```go
tc := timecache.New(timecache.Round(time.Second))

now := tc.Now() // rounded to the second, cached for 500ms
```

## regexcache

Drop-in replacements for `regexp.Compile`, `regexp.MustCompile` and their
POSIX variants, backed by a shared `ttlcache`. Compiled patterns are cached
for 15 minutes and stay cached while they keep being used. The `Must`
variants cache forever, since their patterns are typically package-level
constants.

```go
re := regexcache.MustCompile(`\d+`)

re, err := regexcache.Compile(pattern)
if err != nil {
	return err
}
```
