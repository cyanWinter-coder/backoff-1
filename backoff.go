// Package backoff provides an exponential-backoff implementation.
package backoff

import (
	"math"
	"math/rand"
	"sync/atomic"
	"time"
)

// Backoff is a time.Duration counter, starting at Min. After every call to
// the Duration method the current timing is multiplied by Factor, but it
// never exceeds Max.
//
// Backoff is not generally concurrent-safe, but the ForAttempt method can
// be used concurrently.
// A Backoff must not be copied after first use. Use Copy to copy its constraints.
type Backoff struct {
	attempt atomic.Uint64

	// Factor is the multiplying factor for each increment step.
	//
	// Defaults to 2.
	Factor float64

	// Jitter eases contention by randomizing backoff steps.
	//
	// Defaults to false.
	Jitter bool

	// Minimum value of the counter.
	//
	// Defaults to 100 milliseconds.
	Min time.Duration

	// Maximum value of the counter.
	//
	// Defaults to 10 seconds.
	Max time.Duration
}

// Duration returns the duration for the current attempt before incrementing
// the attempt counter. See ForAttempt.
func (b *Backoff) Duration() time.Duration {
	d := b.ForAttempt(float64(b.attempt.Add(1) - 1))
	return d
}

const maxInt64 = float64(math.MaxInt64 - 512)

// ForAttempt returns the duration for a specific attempt. This is useful if
// you have a large number of independent Backoffs, but don't want use
// unnecessary memory storing the Backoff parameters per Backoff. The first
// attempt should be 0.
//
// ForAttempt is concurrent-safe.
func (b *Backoff) ForAttempt(attempt float64) time.Duration {
	return b.forAttempt(attempt, rand.Float64)
}

// bounds applies the zero-value defaults and returns the effective minimum
// and maximum durations.
func (b *Backoff) bounds() (min, max time.Duration) {
	// Zero-values are nonsensical, so we use
	// them to apply defaults
	min = b.Min
	if min <= 0 {
		min = 100 * time.Millisecond
	}
	max = b.Max
	if max <= 0 {
		max = 10 * time.Second
	}
	return min, max
}

// forAttempt computes the duration for a specific attempt, drawing jitter
// randomness from rng when Jitter is enabled. Passing rand.Float64 makes it
// behave exactly like ForAttempt.
func (b *Backoff) forAttempt(attempt float64, rng func() float64) time.Duration {
	min, max := b.bounds()
	if min >= max {
		// short-circuit
		return max
	}
	factor := b.Factor
	if factor <= 0 {
		factor = 2
	}
	//calculate this duration
	minf := float64(min)
	durf := minf * math.Pow(factor, attempt)
	if b.Jitter {
		durf = rng()*(durf-minf) + minf
	}
	//ensure float64 wont overflow int64
	if durf > maxInt64 {
		return max
	}
	dur := time.Duration(durf)
	//keep within bounds
	if dur < min {
		return min
	}
	if dur > max {
		return max
	}
	return dur
}

// Reset restarts the current attempt counter at zero.
func (b *Backoff) Reset() {
	b.attempt.Store(0)
}

// Attempt returns the current attempt counter value.
func (b *Backoff) Attempt() float64 {
	return float64(b.attempt.Load())
}

// Copy returns a backoff with equals constraints as the original
func (b *Backoff) Copy() *Backoff {
	return &Backoff{
		Factor: b.Factor,
		Jitter: b.Jitter,
		Min:    b.Min,
		Max:    b.Max,
	}
}
