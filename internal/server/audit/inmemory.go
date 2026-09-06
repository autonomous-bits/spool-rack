package audit

import (
	"context"
	"sync"
)

// InMemorySink is a lightweight, thread-safe Sink implementation suitable
// for local development, tests, and deployments that do not yet require a
// Postgres-backed audit trail. It never returns an error.
type InMemorySink struct {
	mu     sync.Mutex
	events []Event
}

// NewInMemorySink constructs an empty InMemorySink.
func NewInMemorySink() *InMemorySink {
	return &InMemorySink{}
}

// Record appends event to the in-memory log. It always succeeds.
func (s *InMemorySink) Record(_ context.Context, event Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

// Events returns a copy of all recorded events, in emission order.
func (s *InMemorySink) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}
