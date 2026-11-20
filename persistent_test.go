package backoff

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	t time.Time
}

func (f *fakeClock) Now() time.Time      { return f.t }
func (f *fakeClock) add(d time.Duration) { f.t = f.t.Add(d) }

func newTestBackoff() *Backoff {
	return &Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
	}
}

func newPersistent(clock Clock) (*PersistentBackoff, *MemoryStore) {
	store := &MemoryStore{}
	p := &PersistentBackoff{
		Backoff: newTestBackoff(),
		Store:   store,
		Clock:   clock,
		newRand: func() *rand.Rand { return rand.New(rand.NewSource(42)) },
	}
	return p, store
}

func TestPersistentRecoverFresh(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1000, 0)}
	p, store := newPersistent(clock)

	if err := p.Recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	s, err := p.State()
	equals(t, err, nil)
	equals(t, s.Attempt, uint64(0))
	equals(t, s.TotalDuration, time.Duration(0))
	equals(t, s.NextAttemptAt.IsZero(), true)
	equals(t, s.RandGeneration, uint64(1))

	// The fresh state is committed immediately so old replicas get fenced.
	raw, err := store.Load(context.Background())
	equals(t, err, nil)
	if len(raw) == 0 {
		t.Fatal("expected committed state in store")
	}

	// State returns a copy; mutating it must not change the backoff.
	s.Attempt = 99
	again, _ := p.State()
	equals(t, again.Attempt, uint64(0))

	delay, err := p.Delay()
	equals(t, err, nil)
	equals(t, delay, time.Duration(0))
}

func TestPersistentAdvanceSchedule(t *testing.T) {
	start := time.Unix(1000, 0)
	clock := &fakeClock{t: start}
	p, _ := newPersistent(clock)
	equals(t, p.Recover(context.Background()), nil)

	d, err := p.Advance(context.Background())
	equals(t, err, nil)
	equals(t, d, 100*time.Millisecond)
	s, _ := p.State()
	equals(t, s.Attempt, uint64(1))
	equals(t, s.TotalDuration, 100*time.Millisecond)
	equals(t, s.NextAttemptAt, start.Add(100*time.Millisecond))

	// Future wait counts down with the external clock.
	delay, _ := p.Delay()
	equals(t, delay, 100*time.Millisecond)
	clock.add(60 * time.Millisecond)
	delay, _ = p.Delay()
	equals(t, delay, 40*time.Millisecond)

	// A second advance accumulates duration and reschedules.
	d, err = p.Advance(context.Background())
	equals(t, err, nil)
	equals(t, d, 200*time.Millisecond)
	s, _ = p.State()
	equals(t, s.Attempt, uint64(2))
	equals(t, s.TotalDuration, 300*time.Millisecond)
	equals(t, s.NextAttemptAt, clock.t.Add(200*time.Millisecond))

	// Once the wait elapses, Delay reports zero.
	clock.add(200 * time.Millisecond)
	delay, _ = p.Delay()
	equals(t, delay, time.Duration(0))
}

func TestPersistentRecoverAcrossRestart(t *testing.T) {
	start := time.Unix(2000, 0)
	clock1 := &fakeClock{t: start}
	p1, store := newPersistent(clock1)
	equals(t, p1.Recover(context.Background()), nil)
	// Three failures, 100+200+400 = 700ms of backoff.
	for i := 0; i < 3; i++ {
		_, err := p1.Advance(context.Background())
		equals(t, err, nil)
	}
	s1, _ := p1.State()
	equals(t, s1.Attempt, uint64(3))
	equals(t, s1.TotalDuration, 700*time.Millisecond)
	nextAt := s1.NextAttemptAt

	// Replacement replica starts after the scheduled wait has expired: the
	// expired wait is dropped and the next attempt runs immediately, while
	// the attempt counter and cumulative duration survive.
	clock2 := &fakeClock{t: nextAt.Add(5 * time.Second)}
	p2 := &PersistentBackoff{
		Backoff: newTestBackoff(),
		Store:   store,
		Clock:   clock2,
		newRand: func() *rand.Rand { return rand.New(rand.NewSource(7)) },
	}
	equals(t, p2.Recover(context.Background()), nil)
	s2, err := p2.State()
	equals(t, err, nil)
	equals(t, s2.Attempt, uint64(3))
	equals(t, s2.TotalDuration, 700*time.Millisecond)
	equals(t, s2.RandGeneration, s1.RandGeneration+1)
	equals(t, s2.NextAttemptAt, clock2.t)
	delay, _ := p2.Delay()
	equals(t, delay, time.Duration(0))

	// The next advance continues the exponential curve at attempt 3.
	d, err := p2.Advance(context.Background())
	equals(t, err, nil)
	equals(t, d, 800*time.Millisecond)
	s2, _ = p2.State()
	equals(t, s2.Attempt, uint64(4))
	equals(t, s2.TotalDuration, 1500*time.Millisecond)
}

func TestPersistentRecoverFutureWaitCappedAtMax(t *testing.T) {
	start := time.Unix(3000, 0)
	clock1 := &fakeClock{t: start}
	p1, store := newPersistent(clock1)
	equals(t, p1.Recover(context.Background()), nil)
	_, err := p1.Advance(context.Background())
	equals(t, err, nil)

	// Simulate a wildly future-dated schedule (clock skew, long pause).
	far := &State{
		Attempt:        1,
		NextAttemptAt:  start.Add(time.Hour),
		TotalDuration:  100 * time.Millisecond,
		RandGeneration: 1,
	}
	raw, err := MarshalState(far)
	equals(t, err, nil)
	equals(t, store.Commit(context.Background(), p1.raw, raw), nil)

	clock2 := &fakeClock{t: start.Add(time.Minute)}
	p2 := &PersistentBackoff{
		Backoff: newTestBackoff(), // default Max = 10s
		Store:   store,
		Clock:   clock2,
		newRand: func() *rand.Rand { return rand.New(rand.NewSource(7)) },
	}
	equals(t, p2.Recover(context.Background()), nil)
	delay, err := p2.Delay()
	equals(t, err, nil)
	equals(t, delay, 10*time.Second)
	s, _ := p2.State()
	equals(t, s.NextAttemptAt, clock2.t.Add(10*time.Second))
}

func TestPersistentStaleGenerationRejected(t *testing.T) {
	clock := &fakeClock{t: time.Unix(4000, 0)}
	p1, store := newPersistent(clock)
	equals(t, p1.Recover(context.Background()), nil)

	// New replica takes over and bumps the generation.
	p2 := &PersistentBackoff{
		Backoff: newTestBackoff(),
		Store:   store,
		Clock:   clock,
		newRand: func() *rand.Rand { return rand.New(rand.NewSource(8)) },
	}
	equals(t, p2.Recover(context.Background()), nil)
	s2, _ := p2.State()
	equals(t, s2.RandGeneration, uint64(2))

	// The old replica's commit must be rejected and must not roll anything back.
	before, _ := p1.State()
	_, err := p1.Advance(context.Background())
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("expected ErrStaleGeneration, got %v", err)
	}
	after, _ := p1.State()
	equals(t, after, before)

	// The store still holds the newer generation's state.
	raw, _ := store.Load(context.Background())
	stored, err := UnmarshalState(raw)
	equals(t, err, nil)
	equals(t, stored.RandGeneration, uint64(2))
	equals(t, stored.Attempt, uint64(0))
}

func TestPersistentAttemptRollbackAndConflict(t *testing.T) {
	clock := &fakeClock{t: time.Unix(5000, 0)}
	p, store := newPersistent(clock)
	equals(t, p.Recover(context.Background()), nil)

	// Another writer in the SAME generation advances to attempt 5.
	ahead := &State{
		Attempt:        5,
		NextAttemptAt:  clock.t.Add(time.Second),
		TotalDuration:  time.Second,
		RandGeneration: 1,
	}
	aheadRaw, err := MarshalState(ahead)
	equals(t, err, nil)
	equals(t, store.Commit(context.Background(), p.raw, aheadRaw), nil)

	before, _ := p.State()
	_, err = p.Advance(context.Background())
	if !errors.Is(err, ErrAttemptRollback) {
		t.Fatalf("expected ErrAttemptRollback, got %v", err)
	}
	after, _ := p.State()
	equals(t, after, before)

	// A re-recovery opens generation 2 but keeps the progressed attempt.
	equals(t, p.Recover(context.Background()), nil)
	recovered, _ := p.State()
	equals(t, recovered.Attempt, uint64(5))
	equals(t, recovered.RandGeneration, uint64(2))

	// A competing writer commits a different payload at the same generation
	// and attempt. Neither fencing rule trips, so the lost compare-and-swap
	// surfaces as a plain conflict.
	rival := &State{
		Attempt:        5,
		NextAttemptAt:  clock.t.Add(2 * time.Second),
		TotalDuration:  2 * time.Second,
		RandGeneration: 2,
	}
	rivalRaw, err := MarshalState(rival)
	equals(t, err, nil)
	equals(t, store.Commit(context.Background(), p.raw, rivalRaw), nil)
	_, err = p.Advance(context.Background())
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
	// The failed commit leaves the recovered valid state in place.
	after, _ = p.State()
	equals(t, after, recovered)

	// The raw store contract: a mismatched expect always fails, nil data
	// commits are rejected as zero values.
	if err := store.Commit(context.Background(), []byte("stale"), rivalRaw); !errors.Is(err, ErrConflict) {
		t.Fatalf("raw CAS: expected ErrConflict, got %v", err)
	}
}

func TestPersistentCorruptAndZeroStates(t *testing.T) {
	clock := &fakeClock{t: time.Unix(6000, 0)}

	cases := map[string][]byte{
		"malformed json":       []byte("{not json"),
		"unknown field":        []byte(`{"attempt":0,"next_attempt_at":"0001-01-01T00:00:00Z","total_duration":0,"rand_generation":1,"extra":1}`),
		"trailing payload":     []byte(`{"attempt":0,"next_attempt_at":"0001-01-01T00:00:00Z","total_duration":0,"rand_generation":1}{}`),
		"zero generation":      []byte(`{"attempt":0,"next_attempt_at":"0001-01-01T00:00:00Z","total_duration":0,"rand_generation":0}`),
		"attempt without time": []byte(`{"attempt":3,"next_attempt_at":"0001-01-01T00:00:00Z","total_duration":1000000000,"rand_generation":1}`),
		"zero total":           []byte(`{"attempt":3,"next_attempt_at":"2020-01-01T00:00:00Z","total_duration":0,"rand_generation":1}`),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			store := &MemoryStore{}
			equals(t, store.Commit(context.Background(), nil, append([]byte(nil), data...)), nil)
			p := &PersistentBackoff{Backoff: newTestBackoff(), Store: store, Clock: clock}
			err := p.Recover(context.Background())
			if !errors.Is(err, ErrCorruptState) && !errors.Is(err, ErrZeroValue) {
				t.Fatalf("expected corrupt/zero error, got %v", err)
			}
			// Nothing was installed: the caller has no half-loaded state.
			if _, err := p.State(); !errors.Is(err, ErrNotRecovered) {
				t.Fatalf("expected ErrNotRecovered, got %v", err)
			}
		})
	}

	// A later corruption must not dislodge an already-valid in-memory state.
	p, store := newPersistent(clock)
	equals(t, p.Recover(context.Background()), nil)
	good, _ := p.State()
	store.mu.Lock()
	store.data = []byte("garbage")
	store.mu.Unlock()
	err := p.Recover(context.Background())
	if !errors.Is(err, ErrCorruptState) {
		t.Fatalf("expected ErrCorruptState, got %v", err)
	}
	still, err := p.State()
	equals(t, err, nil)
	equals(t, still, good)

	// Direct zero-value handling.
	if _, err := UnmarshalState(nil); !errors.Is(err, ErrCorruptState) {
		t.Fatalf("nil data: expected ErrCorruptState, got %v", err)
	}
	if _, err := MarshalState(nil); !errors.Is(err, ErrZeroValue) {
		t.Fatalf("nil state: expected ErrZeroValue, got %v", err)
	}
	if err := (&State{}).validate(); !errors.Is(err, ErrZeroValue) {
		t.Fatalf("zero state: expected ErrZeroValue, got %v", err)
	}
	var empty MemoryStore
	if err := empty.Commit(context.Background(), nil, nil); !errors.Is(err, ErrZeroValue) {
		t.Fatalf("empty commit: expected ErrZeroValue, got %v", err)
	}
}

func TestPersistentZeroConfigAndNotRecovered(t *testing.T) {
	p := &PersistentBackoff{}
	if err := p.Recover(context.Background()); !errors.Is(err, ErrZeroValue) {
		t.Fatalf("recover: expected ErrZeroValue, got %v", err)
	}
	if _, err := p.Advance(context.Background()); !errors.Is(err, ErrZeroValue) {
		t.Fatalf("advance: expected ErrZeroValue, got %v", err)
	}
	if _, err := p.Delay(); !errors.Is(err, ErrZeroValue) {
		t.Fatalf("delay: expected ErrZeroValue, got %v", err)
	}
	if _, err := p.State(); !errors.Is(err, ErrZeroValue) {
		t.Fatalf("state: expected ErrZeroValue, got %v", err)
	}

	clock := &fakeClock{t: time.Unix(7000, 0)}
	valid, _ := newPersistent(clock)
	if _, err := valid.Advance(context.Background()); !errors.Is(err, ErrNotRecovered) {
		t.Fatalf("advance: expected ErrNotRecovered, got %v", err)
	}
	if _, err := valid.Delay(); !errors.Is(err, ErrNotRecovered) {
		t.Fatalf("delay: expected ErrNotRecovered, got %v", err)
	}
	if _, err := valid.State(); !errors.Is(err, ErrNotRecovered) {
		t.Fatalf("state: expected ErrNotRecovered, got %v", err)
	}
}

func TestPersistentConcurrentAccess(t *testing.T) {
	clock := &fakeClock{t: time.Unix(8000, 0)}
	p, store := newPersistent(clock)

	// A locked store makes the contention explicit instead of blocking.
	store.mu.Lock()
	if err := p.Recover(context.Background()); !errors.Is(err, ErrConcurrentAccess) {
		t.Fatalf("locked store: expected ErrConcurrentAccess, got %v", err)
	}
	store.mu.Unlock()

	equals(t, p.Recover(context.Background()), nil)

	// Hold the backoff's own lock: every entry point must fail fast.
	equals(t, p.mu.TryLock(), true)
	if _, err := p.State(); !errors.Is(err, ErrConcurrentAccess) {
		t.Fatalf("state: expected ErrConcurrentAccess, got %v", err)
	}
	if _, err := p.Delay(); !errors.Is(err, ErrConcurrentAccess) {
		t.Fatalf("delay: expected ErrConcurrentAccess, got %v", err)
	}
	if _, err := p.Advance(context.Background()); !errors.Is(err, ErrConcurrentAccess) {
		t.Fatalf("advance: expected ErrConcurrentAccess, got %v", err)
	}
	if err := p.Recover(context.Background()); !errors.Is(err, ErrConcurrentAccess) {
		t.Fatalf("recover: expected ErrConcurrentAccess, got %v", err)
	}
	p.mu.Unlock()

	// Concurrent readers either win the lock or get the explicit error;
	// they must never observe a torn state.
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := p.State()
			if errors.Is(err, ErrConcurrentAccess) {
				return
			}
			if err != nil || s == nil || s.RandGeneration != 1 {
				t.Errorf("unexpected state read: %v %+v", err, s)
			}
		}()
	}
	wg.Wait()
}

func TestPersistentForAttemptParity(t *testing.T) {
	clock := &fakeClock{t: time.Unix(9000, 0)}

	// Non-jitter: persisted durations must match the pure in-memory Backoff
	// exactly, including the Max cap and the Min >= Max short-circuit.
	b := newTestBackoff()
	p, _ := newPersistent(clock)
	equals(t, p.Recover(context.Background()), nil)
	for i := 0; i < 12; i++ {
		got, err := p.Advance(context.Background())
		equals(t, err, nil)
		equals(t, got, b.ForAttempt(float64(i)))
	}

	shortCircuit := &PersistentBackoff{
		Backoff: &Backoff{Min: 500 * time.Second, Max: 100 * time.Second, Factor: 1},
		Store:   &MemoryStore{},
		Clock:   clock,
		newRand: func() *rand.Rand { return rand.New(rand.NewSource(1)) },
	}
	equals(t, shortCircuit.Recover(context.Background()), nil)
	d, err := shortCircuit.Advance(context.Background())
	equals(t, err, nil)
	equals(t, d, 100*time.Second)

	// Jitter: identical RNG stream must produce identical durations to the
	// shared forAttempt computation, and every value stays within bounds.
	jb := &Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: true,
	}
	jp := &PersistentBackoff{
		Backoff: jb,
		Store:   &MemoryStore{},
		Clock:   clock,
		newRand: func() *rand.Rand { return rand.New(rand.NewSource(99)) },
	}
	equals(t, jp.Recover(context.Background()), nil)
	ref := rand.New(rand.NewSource(99))
	for i := 0; i < 20; i++ {
		got, err := jp.Advance(context.Background())
		equals(t, err, nil)
		want := jb.forAttempt(float64(i), ref.Float64)
		equals(t, got, want)
		if got < 100*time.Millisecond || got > 10*time.Second {
			t.Fatalf("jitter duration %s out of bounds", got)
		}
	}
	s, _ := jp.State()
	equals(t, s.Attempt, uint64(20))
}

func TestPersistentStateRoundTrip(t *testing.T) {
	at := time.Unix(10000, 123).UTC()
	original := &State{
		Attempt:        7,
		NextAttemptAt:  at,
		TotalDuration:  12345 * time.Millisecond,
		RandGeneration: 42,
	}
	data, err := MarshalState(original)
	equals(t, err, nil)
	decoded, err := UnmarshalState(data)
	equals(t, err, nil)
	equals(t, decoded, original)
}
