# Backoff

A simple exponential backoff counter in Go (Golang)

[![GoDoc](https://godoc.org/github.com/jpillora/backoff?status.svg)](https://godoc.org/github.com/jpillora/backoff)
[![Build Status](https://github.com/jpillora/backoff/actions/workflows/build.yml/badge.svg)](https://github.com/jpillora/backoff/actions/workflows/build.yml)

### Install

Requires Go 1.19 or later.

```
$ go get -v github.com/jpillora/backoff
```

### Usage

Backoff is a `time.Duration` counter. It starts at `Min`. After every call to `Duration()` it is  multiplied by `Factor`. It is capped at `Max`. It returns to `Min` on every call to `Reset()`. `Jitter` adds randomness ([see below](#example-using-jitter)). Used in conjunction with the `time` package.

---

#### Simple example

``` go

b := &backoff.Backoff{
	//These are the defaults
	Min:    100 * time.Millisecond,
	Max:    10 * time.Second,
	Factor: 2,
	Jitter: false,
}

fmt.Printf("%s\n", b.Duration())
fmt.Printf("%s\n", b.Duration())
fmt.Printf("%s\n", b.Duration())

fmt.Printf("Reset!\n")
b.Reset()

fmt.Printf("%s\n", b.Duration())
```

```
100ms
200ms
400ms
Reset!
100ms
```

---

#### Example using `net` package

``` go
b := &backoff.Backoff{
    Max:    5 * time.Minute,
}

for {
	conn, err := net.Dial("tcp", "example.com:5309")
	if err != nil {
		d := b.Duration()
		fmt.Printf("%s, reconnecting in %s", err, d)
		time.Sleep(d)
		continue
	}
	//connected
	b.Reset()
	conn.Write([]byte("hello world!"))
	// ... Read ... Write ... etc
	conn.Close()
	//disconnected
}

```

---

#### Example using `Jitter`

Enabling `Jitter` adds some randomization to the backoff durations. [See Amazon's writeup of performance gains using jitter](http://www.awsarchitectureblog.com/2015/03/backoff.html).

```go
b := &backoff.Backoff{
	Jitter: true,
}

fmt.Printf("%s\n", b.Duration())
fmt.Printf("%s\n", b.Duration())
fmt.Printf("%s\n", b.Duration())

fmt.Printf("Reset!\n")
b.Reset()

fmt.Printf("%s\n", b.Duration())
fmt.Printf("%s\n", b.Duration())
fmt.Printf("%s\n", b.Duration())
```

Illustrative output (durations may differ):

```
100ms
106.600049ms
281.228155ms
Reset!
100ms
104.381845ms
214.957989ms
```

#### Persistent, generation-fenced backoff

A plain `Backoff` loses its attempt counter when the process restarts, so a
Kubernetes replacement replica starts the exponential curve over and hammers
the failing downstream again. `PersistentBackoff` keeps the same
`ForAttempt`/`Max`/`Jitter` semantics but stores the attempt count, next
attempt time, cumulative duration and a random-source generation in a
serializable `State` behind a compare-and-swap `Store`.

```go
store := backoff.MemoryStore{} // or any Store backed by etcd, SQL, ...
pb := &backoff.PersistentBackoff{
    Backoff: &backoff.Backoff{Max: 5 * time.Minute},
    Store:   &store,
}

// On startup: load and clock-calibrate the persisted state. Each recovery
// opens a new random generation, fencing off zombie replicas.
if err := pb.Recover(ctx); err != nil {
    log.Fatal(err)
}

for {
    if d, err := pb.Delay(); err != nil {
        log.Fatal(err)
    } else if d > 0 {
        time.Sleep(d) // an already-elapsed wait returns zero immediately
    }

    if err := doWork(); err != nil {
        // Computes the next duration exactly like Backoff.Duration(), then
        // commits it; an old generation or attempt rollback is rejected.
        if _, err := pb.Advance(ctx); err != nil {
            log.Fatal(err)
        }
        continue
    }
    break
}
```

Guarantees:

- `State` round-trips through `MarshalState`/`UnmarshalState` (JSON); corrupt
  or zero-value payloads fail with `ErrCorruptState`/`ErrZeroValue` without
  disturbing the caller's current valid state.
- `Recover` calibrates against the external clock: expired waits resume at
  once, future waits remain capped at the effective `Max`.
- Commits are fenced. An older random generation fails with
  `ErrStaleGeneration`; a smaller attempt count within a generation fails
  with `ErrAttemptRollback`; a lost compare-and-swap fails with
  `ErrConflict`.
- Concurrent `Recover`/`Advance`/`State` calls fail fast with
  `ErrConcurrentAccess` rather than returning a torn state.

#### Documentation

https://godoc.org/github.com/jpillora/backoff

#### Credits

Forked from [some JavaScript](https://github.com/segmentio/backo) written by [@tj](https://github.com/tj)
