package review

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
)

func TestDecodeSnapshotJSON_ValidatesVersionedGraphAndSchema(t *testing.T) {
	t.Parallel()

	snapshot, err := DecodeSnapshotJSON([]byte(`{
		"version": 1,
		"schema": {
			"nodeLabels": [
				{"label": "Person", "properties": [{"name": "name", "type": "string", "required": true}]},
				{"label": "Team", "properties": []}
			],
			"edgeLabels": [{"label": "MEMBER_OF", "properties": []}],
			"cardinalities": [{"edgeLabel": "MEMBER_OF", "fromLabel": "Person", "toLabel": "Team", "min": 1, "max": 1}]
		},
		"nodes": [
			{"id": "team-1", "labels": ["Team"], "properties": []},
			{"id": "user-1", "labels": ["Person"], "properties": [{"name": "name", "type": "string", "value": "Ada"}]}
		],
		"edges": [
			{"id": "membership-1", "from": "user-1", "to": "team-1", "labels": ["MEMBER_OF"], "properties": []}
		]
	}`))
	if err != nil {
		t.Fatalf("DecodeSnapshotJSON() error = %v", err)
	}
	if snapshot.Version != SnapshotVersion {
		t.Fatalf("Version = %d, want %d", snapshot.Version, SnapshotVersion)
	}
	if got := snapshot.Nodes[1].Properties[0].Value; string(got) != `"Ada"` {
		t.Fatalf("property value = %s, want %q", got, `"Ada"`)
	}
	if got := *snapshot.Schema.Cardinalities[0].Max; got != 1 {
		t.Fatalf("cardinality max = %d, want 1", got)
	}
	encoded, err := MarshalSnapshotJSON(snapshot)
	if err != nil {
		t.Fatalf("MarshalSnapshotJSON() error = %v", err)
	}
	if strings.Contains(string(encoded), "\n") {
		t.Fatalf("MarshalSnapshotJSON() = %q, want compact deterministic JSON", encoded)
	}
	if _, err := DecodeSnapshotJSON(encoded); err != nil {
		t.Fatalf("DecodeSnapshotJSON(MarshalSnapshotJSON()) error = %v", err)
	}
}

func TestDecodeSnapshotJSON_RejectsStrictContractViolations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		json string
		err  error
	}{
		{
			name: "unknown field",
			json: `{"version":1,"schema":{"nodeLabels":[],"edgeLabels":[],"cardinalities":[]},"nodes":[],"edges":[],"other":true}`,
			err:  ErrInvalidSnapshot,
		},
		{
			name: "duplicate field",
			json: `{"version":1,"version":1,"schema":{"nodeLabels":[],"edgeLabels":[],"cardinalities":[]},"nodes":[],"edges":[]}`,
			err:  ErrInvalidSnapshot,
		},
		{
			name: "malformed JSON",
			json: `{"version":1,"schema":{"nodeLabels":[],"edgeLabels":[],"cardinalities":[]},"nodes":[],"edges":[]`,
			err:  ErrInvalidSnapshot,
		},
		{
			name: "unsupported version",
			json: `{"version":2,"schema":{"nodeLabels":[],"edgeLabels":[],"cardinalities":[]},"nodes":[],"edges":[]}`,
			err:  ErrUnsupportedSnapshotVersion,
		},
		{
			name: "nondeterministic node order",
			json: `{"version":1,"schema":{"nodeLabels":[],"edgeLabels":[],"cardinalities":[]},"nodes":[{"id":"b","labels":["N"],"properties":[]},{"id":"a","labels":["N"],"properties":[]}],"edges":[]}`,
			err:  ErrInvalidSnapshot,
		},
		{
			name: "typed property mismatch",
			json: `{"version":1,"schema":{"nodeLabels":[],"edgeLabels":[],"cardinalities":[]},"nodes":[{"id":"a","labels":["N"],"properties":[{"name":"count","type":"number","value":"one"}]}],"edges":[]}`,
			err:  ErrInvalidSnapshot,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeSnapshotJSON([]byte(tc.json))
			if !errors.Is(err, tc.err) {
				t.Fatalf("DecodeSnapshotJSON() error = %v, want errors.Is(..., %v)", err, tc.err)
			}
		})
	}
}

func TestDecodeSnapshotJSON_ReportsSchemaAndCardinalityContext(t *testing.T) {
	t.Parallel()

	_, err := DecodeSnapshotJSON([]byte(`{
		"version": 1,
		"schema": {
			"nodeLabels": [{"label": "Person", "properties": [{"name": "name", "type": "string", "required": true}]}],
			"edgeLabels": [{"label": "MEMBER_OF", "properties": []}],
			"cardinalities": [{"edgeLabel": "MEMBER_OF", "fromLabel": "Person", "toLabel": "Team", "min": 1}]
		},
		"nodes": [{"id": "user-1", "labels": ["Person"], "properties": []}],
		"edges": []
	}`))
	if !errors.Is(err, ErrSchemaViolation) {
		t.Fatalf("DecodeSnapshotJSON() error = %v, want schema violation", err)
	}
	if !strings.Contains(err.Error(), `node "user-1"`) || !strings.Contains(err.Error(), `property "name"`) {
		t.Fatalf("schema error = %q, want node and property context", err)
	}

	_, err = DecodeSnapshotJSON([]byte(`{
		"version": 1,
		"schema": {
			"nodeLabels": [
				{"label": "Person", "properties": [{"name": "name", "type": "string", "required": true}]},
				{"label": "Team", "properties": []}
			],
			"edgeLabels": [{"label": "MEMBER_OF", "properties": []}],
			"cardinalities": [{"edgeLabel": "MEMBER_OF", "fromLabel": "Person", "toLabel": "Team", "min": 1}]
		},
		"nodes": [
			{"id": "team-1", "labels": ["Team"], "properties": []},
			{"id": "user-1", "labels": ["Person"], "properties": [{"name": "name", "type": "string", "value": "Ada"}]}
		],
		"edges": []
	}`))
	if !errors.Is(err, ErrSchemaViolation) {
		t.Fatalf("DecodeSnapshotJSON() error = %v, want cardinality violation", err)
	}
	if !strings.Contains(err.Error(), `node "user-1"`) || !strings.Contains(err.Error(), "below minimum") {
		t.Fatalf("cardinality error = %q, want node and lower-bound context", err)
	}
}

func TestDecodeSnapshot_LoadsCASObjectAndPreservesCASError(t *testing.T) {
	t.Parallel()

	scope, err := cas.NewScope("tenant-a", "repo-a")
	if err != nil {
		t.Fatalf("NewScope() error = %v", err)
	}
	root := strings.Repeat("a", 64)
	data, err := MarshalSnapshotCBOR(Snapshot{
		Version: SnapshotVersion,
		Schema:  Schema{NodeLabels: []LabelRule{}, EdgeLabels: []LabelRule{}, Cardinalities: []CardinalityRule{}},
		Nodes:   []Node{},
		Edges:   []Edge{},
	})
	if err != nil {
		t.Fatalf("MarshalSnapshotCBOR() error = %v", err)
	}
	store := fakeSnapshotStore{data: map[string][]byte{
		root: data,
	}}

	snapshot, err := DecodeSnapshot(context.Background(), &store, scope, root)
	if err != nil {
		t.Fatalf("DecodeSnapshot() error = %v", err)
	}
	if len(snapshot.Nodes) != 0 || len(snapshot.Edges) != 0 {
		t.Fatalf("DecodeSnapshot() = %+v, want empty graph", snapshot)
	}
	if store.gotHash != root || store.gotScope != scope {
		t.Fatalf("Get() called with hash=%q scope=%+v, want hash=%q scope=%+v", store.gotHash, store.gotScope, root, scope)
	}

	_, err = DecodeSnapshot(context.Background(), &fakeSnapshotStore{err: cas.ErrNotFound}, scope, root)
	if !errors.Is(err, cas.ErrNotFound) {
		t.Fatalf("DecodeSnapshot() error = %v, want wrapped cas.ErrNotFound", err)
	}
	_, err = DecodeSnapshot(context.Background(), &store, scope, "not-a-cas-hash")
	if !errors.Is(err, ErrInvalidSnapshotRoot) {
		t.Fatalf("DecodeSnapshot() invalid root error = %v, want ErrInvalidSnapshotRoot", err)
	}
}

type fakeSnapshotStore struct {
	data     map[string][]byte
	err      error
	gotHash  string
	gotScope cas.Scope
}

func (s *fakeSnapshotStore) Get(_ context.Context, scope cas.Scope, hash string) ([]byte, error) {
	s.gotHash = hash
	s.gotScope = scope
	if s.err != nil {
		return nil, s.err
	}
	return s.data[hash], nil
}
