package gateway

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
)

const (
	// ErrorCodeUnauthorized identifies unauthorized request errors.
	ErrorCodeUnauthorized = "unauthorized"
	// ErrorCodeBadRequest identifies malformed or invalid request errors.
	ErrorCodeBadRequest = "bad_request"
	// ErrorCodeForbidden identifies authenticated requests lacking authorization.
	ErrorCodeForbidden = "forbidden"
	// ErrorCodeConflict identifies requests rejected due to conflicting state.
	ErrorCodeConflict = "conflict"
	// ErrorCodeNotImplemented identifies features not configured on this server.
	ErrorCodeNotImplemented = "not_implemented"
)

type errorEnvelope struct {
	Error       string `json:"error"`
	Message     string `json:"message"`
	CurrentHead string `json:"currentHead,omitempty"`
}

var defaultLogger = slog.New(slog.NewJSONHandler(os.Stderr, nil))

// writeJSONError writes a JSON error response and records a structured log entry.
func writeJSONError(w http.ResponseWriter, r *http.Request, logger *slog.Logger, status int, code, message string) {
	writeJSONErrorEnvelope(w, r, logger, status, errorEnvelope{
		Error:   code,
		Message: message,
	})
}

func writeJSONErrorEnvelope(w http.ResponseWriter, r *http.Request, logger *slog.Logger, status int, envelope errorEnvelope) {
	if logger == nil {
		logger = defaultLogger
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(envelope)

	logger.Warn(
		"request rejected",
		"status", status,
		"code", envelope.Error,
		"method", r.Method,
		"path", r.URL.Path,
		"message", envelope.Message,
	)
}
