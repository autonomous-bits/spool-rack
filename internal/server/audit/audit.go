// Package audit implements the Rack audit event logger described by
// comp-audit-event-logger: a non-blocking, append-only recorder of security,
// mutation, and synchronization events. Emission never blocks or fails the
// originating HTTP request, and events must never carry graph content or
// secrets — only identity, scope, outcome, and correlation metadata, per
// req-tenant-audit-and-access-logging and constraint-tenant-data-sovereignty.
package audit

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Event captures one authenticated, tenant-scoped operation for compliance
// auditing. Fields are deliberately limited to identity, scope, and outcome
// metadata; callers must never populate graph content, credentials, or other
// secrets here.
type Event struct {
	// Timestamp records when the event was emitted. Set by Emit if zero.
	Timestamp time.Time
	// CorrelationID ties this event back to the originating CLI/Rack request,
	// per pattern-tenant-context-propagation and the CLI/Rack error contract.
	CorrelationID string
	// Actor is the authenticated caller's stable subject identifier.
	Actor string
	// TenantID is the tenant the operation was scoped to.
	TenantID string
	// RepoID is the repository the operation was scoped to.
	RepoID string
	// Branch is the branch the operation targeted, when applicable.
	Branch string
	// Action identifies the operation, e.g. "push", "pull", "merge.apply".
	Action string
	// Outcome summarizes the result, e.g. "verified", "rejected", "error".
	Outcome string
	// ContractVersion is the graphcontract pack format version relevant to
	// the operation, when known.
	ContractVersion uint32
	// PreviousRef is the branch ref observed before the operation, when
	// applicable (e.g. the push base commit or pull known commit).
	PreviousRef string
	// NewRef is the branch ref observed or produced after the operation, when
	// applicable (e.g. the push target commit or merge head commit).
	NewRef string
	// Detail is an optional short, sanitized description free of graph
	// content and secrets (e.g. a stable rejection reason).
	Detail string
}

// Sink persists a single audit Event. Implementations may be backed by
// Postgres partitioned tables (comp-postgres-metadata-store) or, for
// lightweight deployments and tests, an in-memory store. Record may return
// an error; Logger guarantees such errors never propagate to the caller
// that emitted the event.
type Sink interface {
	Record(ctx context.Context, event Event) error
}

var defaultLogger = slog.New(slog.NewJSONHandler(os.Stderr, nil))

// defaultBufferSize bounds the number of in-flight events queued for the
// background sink writer. When full, Emit drops the event and logs a
// warning rather than blocking the caller.
const defaultBufferSize = 256

// Logger asynchronously records audit Events to a Sink. Emit never blocks
// the caller and never surfaces Sink errors to it.
type Logger struct {
	sink   Sink
	logger *slog.Logger
	events chan Event
	done   chan struct{}

	mu     sync.RWMutex
	closed bool
}

// NewLogger constructs a Logger that asynchronously drains emitted Events to
// sink on a background goroutine. A nil sink disables recording entirely
// (Emit becomes a no-op) so callers can construct a Logger unconditionally.
func NewLogger(sink Sink, logger *slog.Logger) *Logger {
	if logger == nil {
		logger = defaultLogger
	}
	l := &Logger{
		sink:   sink,
		logger: logger,
		events: make(chan Event, defaultBufferSize),
		done:   make(chan struct{}),
	}
	go l.run()
	return l
}

func (l *Logger) run() {
	defer close(l.done)
	for event := range l.events {
		l.record(event)
	}
}

func (l *Logger) record(event Event) {
	defer func() {
		if r := recover(); r != nil {
			l.logger.Error("audit sink panicked", "action", event.Action, "recover", r)
		}
	}()
	if err := l.sink.Record(context.Background(), event); err != nil {
		l.logger.Warn(
			"audit event recording failed",
			"action", event.Action,
			"correlationId", event.CorrelationID,
			"error", err,
		)
	}
}

// Emit records event asynchronously, never blocking the caller and never
// surfacing Sink failures. If the internal buffer is full or the Logger has
// no configured Sink, the event is dropped and a warning is logged; Emit
// still never fails or blocks the request path.
func (l *Logger) Emit(event Event) {
	if l == nil || l.sink == nil {
		return
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return
	}

	select {
	case l.events <- event:
	default:
		l.logger.Warn("audit event dropped: buffer full", "action", event.Action, "correlationId", event.CorrelationID)
	}
}

// Close stops accepting new events and blocks until all buffered events have
// been drained to the Sink. It is primarily useful for graceful shutdown and
// deterministic tests; ordinary request handling never needs to call it.
// Close is safe to call concurrently with Emit and is idempotent.
func (l *Logger) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	close(l.events)
	l.mu.Unlock()
	<-l.done
}
