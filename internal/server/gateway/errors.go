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
)

type errorEnvelope struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

var defaultLogger = slog.New(slog.NewJSONHandler(os.Stderr, nil))

// writeJSONError writes a JSON error response and records a structured log entry.
func writeJSONError(w http.ResponseWriter, r *http.Request, logger *slog.Logger, status int, code, message string) {
	if logger == nil {
		logger = defaultLogger
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorEnvelope{
		Error:   code,
		Message: message,
	})

	logger.Warn(
		"request rejected",
		"status", status,
		"code", code,
		"method", r.Method,
		"path", r.URL.Path,
		"message", message,
	)
}
