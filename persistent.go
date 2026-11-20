package backoff

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

// Sentinel errors returned by the persistent state machine. Wraps use
// errors.Is, so callers can distinguish a fencing rejection from a corrupt
// payload without parsing error strings.
var (
	// ErrZeroValue is returned when an uninitialized (zero-value)
	// PersistentBackoff or State is used.
	ErrZeroValue = errors.New("backoff: zero value")

	// ErrNotRecovered is returned when Advance or Delay is called before a
	// successful Recover.
	ErrNotRecovered = errors.New("backoff: state not recovered")

	// ErrCorruptState is returned when persisted bytes cannot be decoded or
	// the decoded state fails validation.
	ErrCorruptState = errors.New("backoff: corrupt persisted state")

	// ErrConcurrentAccess is returned when a Store is already locked by
	// another caller, or when a concurrent Recover, Advance or State read is
	// attempted on the same PersistentBackoff.
	ErrConcurrentAccess = errors.New("backoff: concurrent access")

	// ErrStaleGeneration is returned when a replica from an older random
	// source generation tries to commit over a newer generation.
	ErrStaleGeneration = errors.New("backoff: stale generation")

	// ErrAttemptRollback is returned when a commit would move the attempt
	// counter backwards.
	ErrAttemptRollback = errors.New("backoff: attempt counter cannot roll back")

	// ErrConflict is returned when a commit loses a compare-and-swap race
	// against an equally advanced state.
	ErrConflict = errors.New("backoff: conflicting concurrent commit")
)

// State is the serializable backoff state. It survives process restarts so a
// replacement replica can resume the retry schedule instead of restarting the
// exponential backoff from attempt zero.
type State struct {
	// Attempt is the number of failures recorded so far.
	Attempt uint64 `json:"attempt"`

	// NextAttemptAt is the earliest wall-clock time at which the next
	// attempt may run. It is zero only for a fresh state with no failures.
	NextAttemptAt time.Time `json:"next_attempt_at"`

	// TotalDuration is the cumulative time spent backing off across all
	// attempts, including durations persisted by previous replicas.
	TotalDuration time.Duration `json:"total_duration"`

	// RandGeneration identifies the random source epoch. Every new replica
	// that takes over starts a fresh generation; commits carrying an older
	// generation are rejected so a zombie replica cannot overwrite newer
	// progress.
	RandGeneration uint64 `json:"rand_generation"`
}

// validate rejects zero-value and internally inconsistent states.
func (s *State) validate() error {
	if s.RandGeneration == 0 {
		return ErrZeroValue
	}
	if s.Attempt == 0 {
		if !s.NextAttemptAt.IsZero() || s.TotalDuration != 0 {
			return ErrCorruptState
		}
		return nil
	}
	if s.NextAttemptAt.IsZero() || s.TotalDuration <= 0 {
		return ErrCorruptState
	}
	return nil
}

// MarshalState serializes a State into self-describing JSON.
func MarshalState(s *State) ([]byte, error) {
	if s == nil {
		return nil, ErrZeroValue
	}
	if err := s.validate(); err != nil {
		return nil, err
	}
	return json.Marshal(s)
}

// UnmarshalState decodes and validates a State produced by MarshalState.
// Unknown fields are rejected so a payload written by a newer, incompatible
// version is reported as corrupt rather than silently misread.
func UnmarshalState(data []byte) (*State, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, ErrCorruptState
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	s := &State{}
	if err := dec.Decode(s); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrCorruptState, err)
	}
	if dec.More() {
		return nil, ErrCorruptState
	}
	if err := s.validate(); err != nil {
		return nil, err
	}
	return s, nil
}

// validateTransition enforces the generation and attempt fencing rules shared
// by every commit path: the random source generation never moves backwards,
// and within any generation the attempt counter never moves backwards.
func validateTransition(current, next *State) error {
	if next.RandGeneration < current.RandGeneration {
		return ErrStaleGeneration
	}
	if next.RandGeneration == current.RandGeneration && next.Attempt < current.Attempt {
		return ErrAttemptRollback
	}
	return nil
}

// Clock is the wall-clock source used to calibrate recovered schedules.
type Clock interface {
	Now() time.Time
}

// ClockFunc adapts a plain function to the Clock interface.
type ClockFunc func() time.Time

// Now calls the underlying function.
func (f ClockFunc) Now() time.Time { return f() }

// Store is the durability boundary for serialized State. Implementations are
// expected to be backed by a compare-and-swap capable system (a key with a
// revision, an etcd transaction, a SQL row version, ...).
//
// Load returns ErrNotFound-like nil data when nothing has been persisted yet.
// Commit must write next only if the stored bytes still equal expect; on a
// mismatch it returns an error wrapping ErrConflict. Both operations fail with
// ErrConcurrentAccess when the store is already locked by another caller.
type Store interface {
	// Load returns the currently persisted state bytes, or nil if no state
	// has been committed yet.
	Load(ctx context.Context) ([]byte, error)

	// Commit writes next when the stored value still equals expect. A nil
	// expect means the store must still be empty.
	Commit(ctx context.Context, expect, next []byte) error
}

// MemoryStore is a reference, concurrent-safe Store kept entirely in memory.
// A Load or Commit fails fast with ErrConcurrentAccess while another caller
// holds the lock instead of blocking, which makes torn-read races explicit.
// It is intended for tests and single-process use; cross-process fencing
// needs a durable Store implementation.
type MemoryStore struct {
	mu   sync.Mutex
	data []byte
}

// Load returns the persisted bytes, or nil when empty.
func (m *MemoryStore) Load(context.Context) ([]byte, error) {
	if !m.mu.TryLock() {
		return nil, ErrConcurrentAccess
	}
	defer m.mu.Unlock()
	if len(m.data) == 0 {
		return nil, nil
	}
	// Return a copy so callers cannot mutate the stored payload.
	out := make([]byte, len(m.data))
	copy(out, m.data)
	return out, nil
}

// Commit applies a compare-and-swap on the stored bytes.
func (m *MemoryStore) Commit(_ context.Context, expect, next []byte) error {
	if !m.mu.TryLock() {
		return ErrConcurrentAccess
	}
	defer m.mu.Unlock()
	if !bytesEqual(m.data, expect) {
		return ErrConflict
	}
	if len(next) == 0 {
		return ErrZeroValue
	}
	m.data = append([]byte(nil), next...)
	return nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) == 0 {
		return len(b) == 0
	}
	return bytes.Equal(a, b)
}

// PersistentBackoff wraps a Backoff with durable, generation-fenced state. It
// preserves the Backoff's ForAttempt, Max and Jitter semantics exactly; the
// attempt counter, next-attempt time, cumulative duration and random source
// generation live in a State that can be serialized and recovered by another
// process.
//
// Recover, Advance, Delay and State are fail-fast under concurrency: they
// return ErrConcurrentAccess instead of blocking when another call on the
// same PersistentBackoff is in flight. A PersistentBackoff must not be copied
// after first use.
type PersistentBackoff struct {
	mu      sync.Mutex
	Backoff *Backoff
	Store   Store

	// Clock defaults to time.Now. Tests may substitute a fixed clock.
	Clock Clock

	// newRand builds the random source for a generation. Defaults to a
	// time-seeded rand.Rand.
	newRand func() *rand.Rand

	generation uint64
	rng        *rand.Rand
	state      *State
	raw        []byte
	recovered  bool
}

func (p *PersistentBackoff) clock() Clock {
	if p.Clock != nil {
		return p.Clock
	}
	return ClockFunc(time.Now)
}

func (p *PersistentBackoff) random() *rand.Rand {
	if p.newRand != nil {
		return p.newRand()
	}
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

func (p *PersistentBackoff) validateConfig() error {
	if p == nil || p.Backoff == nil || p.Store == nil {
		return ErrZeroValue
	}
	return nil
}

// Recover loads the persisted state and calibrates it against the current
// wall clock. A wait that has already elapsed is dropped so the next attempt
// proceeds immediately; a future wait is kept but capped at the effective Max
// duration. On first use it starts a fresh state.
//
// Every recovery opens a new random source generation and immediately commits
// it, fencing off every replica from older generations. Recover never mutates
// the in-memory state when it returns an error.
func (p *PersistentBackoff) Recover(ctx context.Context) error {
	if err := p.validateConfig(); err != nil {
		return err
	}
	if !p.mu.TryLock() {
		return ErrConcurrentAccess
	}
	defer p.mu.Unlock()

	raw, err := p.Store.Load(ctx)
	if err != nil {
		return err
	}

	var loaded *State
	if len(raw) != 0 {
		loaded, err = UnmarshalState(raw)
		if err != nil {
			// Keep the caller's current valid state untouched.
			return err
		}
	}

	// A new replica always starts a new random source generation.
	generation := uint64(1)
	if loaded != nil {
		generation = loaded.RandGeneration + 1
	}
	if generation == 0 || generation == math.MaxUint64 {
		return ErrCorruptState
	}

	now := p.clock().Now()
	next := &State{RandGeneration: generation}
	if loaded != nil {
		next.Attempt = loaded.Attempt
		next.TotalDuration = loaded.TotalDuration
		next.NextAttemptAt = loaded.NextAttemptAt
		next.calibrate(now, p.maxBackoff())
	}

	encoded, err := MarshalState(next)
	if err != nil {
		return err
	}
	if err := p.Store.Commit(ctx, raw, encoded); err != nil {
		return p.classifyConflict(err, next)
	}

	p.generation = generation
	p.rng = p.random()
	p.state = next
	p.raw = encoded
	p.recovered = true
	return nil
}

// calibrate moves an already-due next-attempt time to now (the expired wait is
// over), and clamps a future wait so it never extends beyond max beyond now.
func (s *State) calibrate(now time.Time, max time.Duration) {
	if s.Attempt == 0 || s.NextAttemptAt.IsZero() {
		return
	}
	if !s.NextAttemptAt.After(now) {
		s.NextAttemptAt = now
		return
	}
	limit := now.Add(max)
	if s.NextAttemptAt.After(limit) {
		s.NextAttemptAt = limit
	}
}

func (p *PersistentBackoff) maxBackoff() time.Duration {
	_, max := p.Backoff.bounds()
	return max
}

// Delay returns how much longer the caller must wait before the next attempt:
// zero when the scheduled time has elapsed, otherwise the remaining wait
// capped at the effective Max duration.
func (p *PersistentBackoff) Delay() (time.Duration, error) {
	if err := p.validateConfig(); err != nil {
		return 0, err
	}
	if !p.mu.TryLock() {
		return 0, ErrConcurrentAccess
	}
	defer p.mu.Unlock()
	if !p.recovered {
		return 0, ErrNotRecovered
	}

	now := p.clock().Now()
	if p.state.Attempt == 0 || !p.state.NextAttemptAt.After(now) {
		return 0, nil
	}
	wait := p.state.NextAttemptAt.Sub(now)
	if max := p.maxBackoff(); wait > max {
		return max, nil
	}
	return wait, nil
}

// Advance records a failed attempt: it computes the next backoff with the
// embedded Backoff (identical bounds, jitter and ForAttempt behavior as the
// in-memory mode), increments the attempt counter, accumulates the duration and
// schedules the next attempt, then commits the state through the Store.
//
// The commit is fenced: an older generation is rejected with
// ErrStaleGeneration and a smaller attempt count with ErrAttemptRollback. A
// failed commit leaves the current valid state in place.
func (p *PersistentBackoff) Advance(ctx context.Context) (time.Duration, error) {
	if err := p.validateConfig(); err != nil {
		return 0, err
	}
	if !p.mu.TryLock() {
		return 0, ErrConcurrentAccess
	}
	defer p.mu.Unlock()
	if !p.recovered {
		return 0, ErrNotRecovered
	}

	now := p.clock().Now()

	d := p.Backoff.forAttempt(float64(p.state.Attempt), p.rng.Float64)
	next := &State{
		Attempt:        p.state.Attempt + 1,
		NextAttemptAt:  now.Add(d),
		TotalDuration:  p.state.TotalDuration + d,
		RandGeneration: p.generation,
	}
	if next.Attempt == 0 || next.TotalDuration <= 0 {
		return 0, ErrCorruptState
	}

	encoded, err := MarshalState(next)
	if err != nil {
		return 0, err
	}
	if err := p.Store.Commit(ctx, p.raw, encoded); err != nil {
		return 0, p.classifyConflict(err, next)
	}

	p.state = next
	p.raw = encoded
	return d, nil
}

// classifyConflict translates a failed compare-and-swap into the specific
// fencing error by decoding the currently stored state. When the stored bytes
// cannot be read or decoded, the original conflict error is returned.
func (p *PersistentBackoff) classifyConflict(conflict error, attempted *State) error {
	raw, err := p.Store.Load(context.Background())
	if err != nil || len(raw) == 0 {
		return conflict
	}
	current, err := UnmarshalState(raw)
	if err != nil {
		return conflict
	}
	if err := validateTransition(current, attempted); err != nil {
		return err
	}
	return conflict
}

// State returns a copy of the current in-memory state. It fails with
// ErrConcurrentAccess while a Recover or Advance on the same
// PersistentBackoff is in flight, and never returns a partially updated state.
func (p *PersistentBackoff) State() (*State, error) {
	if err := p.validateConfig(); err != nil {
		return nil, err
	}
	if !p.mu.TryLock() {
		return nil, ErrConcurrentAccess
	}
	defer p.mu.Unlock()
	if !p.recovered {
		return nil, ErrNotRecovered
	}
	copy := *p.state
	return &copy, nil
}
