package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/autonomous-bits/spool/graphcontract"
)

func TestHealthz(t *testing.T) {
	gw := New()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK, got %d", rec.Code)
	}

	var resp healthzResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode JSON response: %v", err)
	}

	if resp.Status != "healthy" {
		t.Errorf("expected status 'healthy', got '%s'", resp.Status)
	}

	if resp.GraphContract.PackFormatVersion != graphcontract.PackFormatVersion {
		t.Errorf("expected packFormatVersion %d, got %d", graphcontract.PackFormatVersion, resp.GraphContract.PackFormatVersion)
	}
	if resp.GraphContract.PackIndexFormatVersion != graphcontract.PackIndexFormatVersion {
		t.Errorf("expected packIndexFormatVersion %d, got %d", graphcontract.PackIndexFormatVersion, resp.GraphContract.PackIndexFormatVersion)
	}
	if resp.GraphContract.PackManifestFormatVersion != graphcontract.PackManifestFormatVersion {
		t.Errorf("expected packManifestFormatVersion %d, got %d", graphcontract.PackManifestFormatVersion, resp.GraphContract.PackManifestFormatVersion)
	}
}
