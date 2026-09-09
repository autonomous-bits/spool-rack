package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/autonomous-bits/spool/graphcontract"

	serversync "github.com/autonomous-bits/spool-rack/internal/server/sync"
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

	if resp.GraphContract.PackFormatVersion != serversync.PackFormatV3 {
		t.Errorf("expected packFormatVersion %d, got %d", serversync.PackFormatV3, resp.GraphContract.PackFormatVersion)
	}
	if resp.GraphContract.PackIndexFormatVersion != graphcontract.PackIndexFormatVersion {
		t.Errorf("expected packIndexFormatVersion %d, got %d", graphcontract.PackIndexFormatVersion, resp.GraphContract.PackIndexFormatVersion)
	}
	if resp.GraphContract.PackManifestFormatVersion != graphcontract.PackManifestFormatVersion {
		t.Errorf("expected packManifestFormatVersion %d, got %d", graphcontract.PackManifestFormatVersion, resp.GraphContract.PackManifestFormatVersion)
	}

	// By default (no rollout window configured), the accepted min/max range
	// accepts PackFormatV2 through PackFormatV3.
	if resp.GraphContract.PackFormatMinVersion != serversync.PackFormatV2 || resp.GraphContract.PackFormatMaxVersion != serversync.PackFormatV3 {
		t.Errorf("expected packFormat min=%d max=%d, got min=%d max=%d", serversync.PackFormatV2, serversync.PackFormatV3, resp.GraphContract.PackFormatMinVersion, resp.GraphContract.PackFormatMaxVersion)
	}
	if resp.GraphContract.PackIndexFormatMinVersion != graphcontract.PackIndexFormatVersion || resp.GraphContract.PackIndexFormatMaxVersion != graphcontract.PackIndexFormatVersion {
		t.Errorf("expected packIndexFormat min/max to both be %d, got min=%d max=%d", graphcontract.PackIndexFormatVersion, resp.GraphContract.PackIndexFormatMinVersion, resp.GraphContract.PackIndexFormatMaxVersion)
	}
	if resp.GraphContract.PackManifestFormatMinVersion != graphcontract.PackManifestFormatVersion || resp.GraphContract.PackManifestFormatMaxVersion != graphcontract.PackManifestFormatVersion {
		t.Errorf("expected packManifestFormat min/max to both be %d, got min=%d max=%d", graphcontract.PackManifestFormatVersion, resp.GraphContract.PackManifestFormatMinVersion, resp.GraphContract.PackManifestFormatMaxVersion)
	}
}

// TestHealthzAdvertisesWidenedRolloutWindow verifies that when operators
// widen the accepted pack format window (e.g. via RACK_MIN_PACK_FORMAT_VERSION
// or WithPackFormatWindow during a rolling deploy), /healthz advertises the
// widened range rather than only the current version.
func TestHealthzAdvertisesWidenedRolloutWindow(t *testing.T) {
	gw := New(WithPackFormatWindow(VersionWindow{Min: graphcontract.PackFormatVersion - 1, Max: graphcontract.PackFormatVersion}))
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	gw.Routes().ServeHTTP(rec, req)

	var resp healthzResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode JSON response: %v", err)
	}
	if resp.GraphContract.PackFormatMinVersion != graphcontract.PackFormatVersion-1 {
		t.Errorf("expected widened packFormatMinVersion %d, got %d", graphcontract.PackFormatVersion-1, resp.GraphContract.PackFormatMinVersion)
	}
	if resp.GraphContract.PackFormatMaxVersion != graphcontract.PackFormatVersion {
		t.Errorf("expected packFormatMaxVersion %d, got %d", graphcontract.PackFormatVersion, resp.GraphContract.PackFormatMaxVersion)
	}
}
