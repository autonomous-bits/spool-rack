package audit

import (
	"context"
	"errors"
	"testing"
)

func TestLoggerRecordsEventFields(t *testing.T) {
	sink := NewInMemorySink()
	logger := NewLogger(sink, nil)

	logger.Emit(Event{
		CorrelationID:   "corr-1",
		Actor:           "user-1",
		TenantID:        "tenant-1",
		RepoID:          "repo-1",
		Branch:          "main",
		Action:          "push",
		Outcome:         "verified",
		ContractVersion: 3,
		PreviousRef:     "base-commit",
		NewRef:          "target-commit",
	})
	logger.Close()

	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 recorded event, got %d", len(events))
	}

	got := events[0]
	if got.CorrelationID != "corr-1" {
		t.Fatalf("expected correlationId corr-1, got %q", got.CorrelationID)
	}
	if got.Actor != "user-1" {
		t.Fatalf("expected actor user-1, got %q", got.Actor)
	}
	if got.TenantID != "tenant-1" {
		t.Fatalf("expected tenantId tenant-1, got %q", got.TenantID)
	}
	if got.RepoID != "repo-1" {
		t.Fatalf("expected repoId repo-1, got %q", got.RepoID)
	}
	if got.Branch != "main" {
		t.Fatalf("expected branch main, got %q", got.Branch)
	}
	if got.Action != "push" {
		t.Fatalf("expected action push, got %q", got.Action)
	}
	if got.Outcome != "verified" {
		t.Fatalf("expected outcome verified, got %q", got.Outcome)
	}
	if got.ContractVersion != 3 {
		t.Fatalf("expected contractVersion 3, got %d", got.ContractVersion)
	}
	if got.PreviousRef != "base-commit" {
		t.Fatalf("expected previousRef base-commit, got %q", got.PreviousRef)
	}
	if got.NewRef != "target-commit" {
		t.Fatalf("expected newRef target-commit, got %q", got.NewRef)
	}
	if got.Timestamp.IsZero() {
		t.Fatal("expected a non-zero timestamp to be assigned")
	}
}

type erroringSink struct{}

func (erroringSink) Record(context.Context, Event) error {
	return errors.New("simulated sink failure")
}

func TestLoggerEmitNeverBlocksOrFailsOnSinkError(t *testing.T) {
	logger := NewLogger(erroringSink{}, nil)

	done := make(chan struct{})
	go func() {
		logger.Emit(Event{Action: "push", Outcome: "verified"})
		close(done)
	}()

	<-done
	// Emit must have returned promptly regardless of Sink outcome; Close
	// drains the background writer so we know the erroring Record call has
	// completed without panicking or otherwise propagating.
	logger.Close()
}

func TestLoggerNilSinkIsNoop(t *testing.T) {
	var logger *Logger
	logger.Emit(Event{Action: "push"}) // must not panic on a nil Logger
	logger.Close()

	logger = NewLogger(nil, nil)
	logger.Emit(Event{Action: "push"}) // must not panic with no configured sink
	logger.Close()
}
