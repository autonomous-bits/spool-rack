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
	// ErrorCodeNotFound identifies a requested branch or resource that does not exist.
	ErrorCodeNotFound = "not_found"
	// ErrorCodeInternal identifies an unexpected server-side failure.
	ErrorCodeInternal = "internal_error"
	// ErrorCodeBranchAlreadyExists identifies a branch create request that
	// reused a name already taken within the repository.
	ErrorCodeBranchAlreadyExists = "branch_already_exists"
	// ErrorCodeBranchSourceNotFound identifies a branch create request whose
	// sourceBranch or sourceCommit could not be resolved.
	ErrorCodeBranchSourceNotFound = "branch_source_not_found"
	// ErrorCodeBranchNotFound identifies a branch lifecycle request (get
	// default, delete) targeting a branch that does not exist.
	ErrorCodeBranchNotFound = "branch_not_found"
	// ErrorCodeBranchProtected identifies a delete request rejected because
	// the target branch is the repository's default branch, per
	// req-remote-branch-lifecycle-and-safe-deletion.
	ErrorCodeBranchProtected = "branch_protected"
	// ErrorCodeUnsupportedContractVersion identifies a request rejected
	// because the client declared a graphcontract wire-format version
	// outside the range this server currently accepts (see
	// spec-cli-rack-contract-and-metadata-migrations); GET /healthz
	// advertises the accepted range.
	ErrorCodeUnsupportedContractVersion = "unsupported_contract_version"
)

type errorEnvelope struct {
	Error         string `json:"error"`
	Message       string `json:"message"`
	CurrentHead   string `json:"currentHead,omitempty"`
	CorrelationID string `json:"correlationId"`
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
	if envelope.CorrelationID == "" {
		if correlationID, ok := CorrelationIDFromContext(r.Context()); ok {
			envelope.CorrelationID = correlationID
		}
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
		"correlationId", envelope.CorrelationID,
	)
}

// logRejection records the full internal detail of err server-side only,
// for cases where the client-visible errorEnvelope message must instead be a
// stable, sanitized string free of internal detail or cross-tenant data
// (per spec-cli-rack-operational-error-and-audit-contract). It never writes
// anything to the response.
func logRejection(logger *slog.Logger, r *http.Request, action string, err error) {
	if logger == nil {
		logger = defaultLogger
	}
	correlationID, _ := CorrelationIDFromContext(r.Context())
	logger.Warn(
		action,
		"method", r.Method,
		"path", r.URL.Path,
		"error", err,
		"correlationId", correlationID,
	)
}
