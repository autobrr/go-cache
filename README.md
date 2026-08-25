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
- `Get` pushes an item's expiration forward (sliding TTL); disable with `DisableUpdateTime(true)`
- a deallocation callback runs whenever an item times out or is deleted; it runs outside the cache lock, so it may call back into the cache
- `Keys` and `All` iterate over a snapshot without holding the lock while the loop body runs
- `GetOrSet` stores only when the key is absent; `GetItem` exposes the stored duration and deadline
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
  with the reason (`ReasonTimedOut` or `ReasonDeleted`):

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
tc := timecache.New(timecache.Options{}.Round(time.Second))

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
