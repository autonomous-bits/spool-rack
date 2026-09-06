package review

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool/graphcontract"
)

func TestSnapshotRoundTripPreservesCanonicalContract(t *testing.T) {
	t.Parallel()

	snapshot := testSnapshot(testNode("node", "Person", "external_id", graphcontract.IntegerPropertyValue(1)))
	data, err := MarshalSnapshotCBOR(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeSnapshotCBOR(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, snapshot) {
		t.Fatalf("decoded snapshot = %#v, want %#v", decoded, snapshot)
	}
	if _, err := DecodeSnapshotJSON([]byte(`{}`)); !errors.Is(err, ErrUnsupportedSnapshotVersion) {
		t.Fatalf("DecodeSnapshotJSON() error = %v, want unsupported version", err)
	}
	if _, err := MarshalSnapshotJSON(snapshot); !errors.Is(err, ErrUnsupportedSnapshotVersion) {
		t.Fatalf("MarshalSnapshotJSON() error = %v, want unsupported version", err)
	}
}

func TestDecodeSnapshotObjectRejectsLegacyJSONWithMigrationGuidance(t *testing.T) {
	t.Parallel()

	_, err := DecodeSnapshotObject([]byte(`{"version":1,"schema":{},"nodes":[],"edges":[]}`))
	if !errors.Is(err, ErrUnsupportedSnapshotVersion) ||
		!strings.Contains(err.Error(), "legacy JSON review snapshots are unsupported") {
		t.Fatalf("DecodeSnapshotObject() error = %v, want actionable legacy-format rejection", err)
	}
}

func TestDecodeSnapshotLoadsCanonicalObjectAndPreservesCASError(t *testing.T) {
	t.Parallel()

	scope, err := cas.NewScope("tenant-a", "repo-a")
	if err != nil {
		t.Fatal(err)
	}
	data, err := MarshalSnapshotCBOR(testSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	root := strings.Repeat("a", 64)
	store := fakeSnapshotStore{data: map[string][]byte{root: data}}
	if _, err := DecodeSnapshot(context.Background(), &store, scope, root); err != nil {
		t.Fatal(err)
	}
	if store.gotHash != root || store.gotScope != scope {
		t.Fatalf("Get() scope/hash = %#v/%q, want %#v/%q", store.gotScope, store.gotHash, scope, root)
	}
	if _, err := DecodeSnapshot(context.Background(), &fakeSnapshotStore{err: cas.ErrNotFound}, scope, root); !errors.Is(err, cas.ErrNotFound) {
		t.Fatalf("DecodeSnapshot() error = %v, want wrapped not found", err)
	}
}

func TestSchemaParityFixtures(t *testing.T) {
	fixtureRoot := spoolSchemaFixtureRoot(t)
	paths, err := filepath.Glob(filepath.Join(fixtureRoot, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			var fixture schemaFixture
			decodeFixture(t, path, &fixture)
			schemaData, err := os.ReadFile(filepath.Join(fixtureRoot, fixture.Schema))
			if err != nil {
				t.Fatal(err)
			}
			schema, err := graphcontract.DecodeSchemaTOML(schemaData)
			if err != nil {
				t.Fatal(err)
			}
			err = graphcontract.ValidateSchemaSnapshot(schema, fixture.Nodes, fixture.Edges)
			if len(fixture.Violations) == 0 {
				if err != nil {
					t.Fatalf("validate %s fixture: %v", fixture.Candidate, err)
				}
				return
			}
			var validation *graphcontract.SchemaValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("validation error = %v, want normalized schema violations", err)
			}
			if !reflect.DeepEqual(validation.Violations, fixture.Violations) {
				t.Fatalf("violations = %#v, want %#v", validation.Violations, fixture.Violations)
			}
			snapshot := Snapshot{Version: SnapshotVersion, Schema: schema, Nodes: fixture.Nodes, Edges: fixture.Edges}
			if err := snapshot.Validate(); !errors.Is(err, graphcontract.ErrSchemaValidation) {
				t.Fatalf("Rack snapshot validation = %v, want canonical schema error", err)
			}
		})
	}
}

func TestSchemaParityRejectsInvalidSchemaFixture(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(spoolSchemaFixtureRoot(t), "invalid-schema.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graphcontract.DecodeSchemaTOML(data); !errors.Is(err, graphcontract.ErrInvalidSchemaTOML) {
		t.Fatalf("DecodeSchemaTOML() error = %v, want canonical TOML error", err)
	}
}

type schemaFixture struct {
	FormatVersion int                             `json:"format_version"`
	Candidate     string                          `json:"candidate"`
	Schema        string                          `json:"schema"`
	Nodes         map[string]graphcontract.Node   `json:"nodes"`
	Edges         map[string]graphcontract.Edge   `json:"edges"`
	Violations    []graphcontract.SchemaViolation `json:"violations"`
}

func decodeFixture(t *testing.T, path string, destination any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, destination); err != nil {
		t.Fatal(err)
	}
}

func spoolSchemaFixtureRoot(t *testing.T) string {
	t.Helper()
	command := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/autonomous-bits/spool")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("locate Spool module: %v", err)
	}
	return filepath.Join(strings.TrimSpace(string(output)), "graphcontract", "testdata", "schema", "v1")
}

type fakeSnapshotStore struct {
	data     map[string][]byte
	err      error
	gotHash  string
	gotScope cas.Scope
}

func (s *fakeSnapshotStore) Get(_ context.Context, scope cas.Scope, hash string) ([]byte, error) {
	s.gotHash, s.gotScope = hash, scope
	if s.err != nil {
		return nil, s.err
	}
	return s.data[hash], nil
}
