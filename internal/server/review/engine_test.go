package review

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool/graphcontract"
)

func TestPreviewMergeUsesTenantScopedReadOnlyInputs(t *testing.T) {
	base := testSnapshot(testNode("n", "Person", "name", graphcontract.StringPropertyValue("base")))
	source := testSnapshot(testNode("n", "Person", "name", graphcontract.StringPropertyValue("source")))
	store, objects := testPreviewDependencies(t, map[string]Snapshot{testRoot('a'): base, testRoot('b'): source},
		map[string]string{"feature": "source", "main": "target"}, map[string]string{"target": testRoot('a'), "source": testRoot('b')})
	store.fastForward = true

	preview, err := NewPreviewEngine(objects, store).PreviewMerge(context.Background(), "tenant-1", "repo-1", "feature", "main")
	if err != nil {
		t.Fatal(err)
	}
	if !preview.CanFastForward || preview.HasConflicts || len(preview.CleanChanges) != 1 {
		t.Fatalf("preview = %#v, want one clean fast-forward change", preview)
	}
	if store.setTenantCalls != 1 || len(objects.scopes) != 3 {
		t.Fatalf("scoped dependency calls = tenant:%d CAS:%d, want 1/3", store.setTenantCalls, len(objects.scopes))
	}
}

func TestPreviewMergeReportsPropertyAndStructuralConflicts(t *testing.T) {
	t.Run("property", func(t *testing.T) {
		base := testSnapshot(testNode("n", "Person", "name", graphcontract.StringPropertyValue("base")))
		source := testSnapshot(testNode("n", "Person", "name", graphcontract.StringPropertyValue("source")))
		target := testSnapshot(testNode("n", "Person", "name", graphcontract.StringPropertyValue("target")))
		preview := mergeSnapshots(base, source, target)
		assertSingleConflict(t, &preview, ConflictProperty, "property:node:n:name")
	})
	t.Run("structural", func(t *testing.T) {
		base := testSnapshot(testNode("n", "Person"))
		source := testSnapshot(testNode("n", "Source"))
		target := testSnapshot(testNode("n", "Target"))
		preview := mergeSnapshots(base, source, target)
		assertSingleConflict(t, &preview, ConflictStructural, "structural:node:n")
	})
}

func TestPreviewMergePreservesCanonicalViolationOrder(t *testing.T) {
	fixtureRoot := spoolSchemaFixtureRoot(t)
	var fixture schemaFixture
	decodeFixture(t, filepath.Join(fixtureRoot, "merge-invalid.json"), &fixture)
	schemaData, err := os.ReadFile(filepath.Join(fixtureRoot, fixture.Schema))
	if err != nil {
		t.Fatal(err)
	}
	schema, err := graphcontract.DecodeSchemaTOML(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := Snapshot{Version: SnapshotVersion, Schema: schema, Nodes: fixture.Nodes, Edges: fixture.Edges}
	preview := MergePreview{Violations: []graphcontract.SchemaViolation{}}
	validateMergedSchema(snapshot, &preview)
	if !reflect.DeepEqual(preview.Violations, fixture.Violations) {
		t.Fatalf("violations = %#v, want %#v", preview.Violations, fixture.Violations)
	}
	assertSingleConflict(t, &preview, ConflictSchema, "schema:schema")
}

func assertSingleConflict(t *testing.T, preview *MergePreview, wantType ConflictType, wantToken string) {
	t.Helper()
	if !preview.HasConflicts || len(preview.Conflicts) != 1 || preview.Conflicts[0].Type != wantType || preview.Conflicts[0].Token != wantToken {
		t.Fatalf("conflicts = %#v, want %s", preview.Conflicts, wantToken)
	}
}

var errTest = errors.New("test read error")

type fakePreviewStore struct {
	branchHeads                             map[string]string
	snapshotRoots                           map[string]string
	fastForward                             bool
	setTenantCalls, ancestorCalls, lcaCalls int
}

func (s *fakePreviewStore) SetTenantContext(ctx context.Context, tenantID string) (context.Context, error) {
	s.setTenantCalls++
	if tenantID != "tenant-1" {
		return nil, errTest
	}
	return ctx, nil
}
func (s *fakePreviewStore) GetBranchRef(_ context.Context, _ string, branch string) (string, error) {
	head, ok := s.branchHeads[branch]
	if !ok {
		return "", errTest
	}
	return head, nil
}
func (s *fakePreviewStore) IsAncestor(_ context.Context, _ string, _, _ string) (bool, error) {
	s.ancestorCalls++
	return s.fastForward, nil
}
func (s *fakePreviewStore) FindLowestCommonAncestor(_ context.Context, _ string, _, _ string) (string, error) {
	s.lcaCalls++
	return "base", nil
}
func (s *fakePreviewStore) GetCommitSnapshotRoot(_ context.Context, _ string, commit string) (string, error) {
	root, ok := s.snapshotRoots[commit]
	if !ok {
		return "", errTest
	}
	return root, nil
}

type fakePreviewObjects struct {
	data   map[string][]byte
	scopes []cas.Scope
}

func (s *fakePreviewObjects) Get(_ context.Context, scope cas.Scope, root string) ([]byte, error) {
	s.scopes = append(s.scopes, scope)
	data, ok := s.data[root]
	if !ok {
		return nil, errTest
	}
	return data, nil
}

func testPreviewDependencies(t *testing.T, snapshots map[string]Snapshot, heads, roots map[string]string) (*fakePreviewStore, *fakePreviewObjects) {
	t.Helper()
	data := make(map[string][]byte, len(snapshots))
	for root, snapshot := range snapshots {
		encoded, err := MarshalSnapshotCBOR(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		data[root] = encoded
	}
	return &fakePreviewStore{branchHeads: heads, snapshotRoots: roots}, &fakePreviewObjects{data: data}
}

func testSnapshot(nodes ...Node) Snapshot {
	byID := make(map[string]Node, len(nodes))
	for _, node := range nodes {
		byID[node.ID] = node
	}
	return Snapshot{Version: SnapshotVersion, Schema: graphcontract.BuiltinSchemaSnapshot(), Nodes: byID, Edges: map[string]Edge{}}
}

func testNode(id, label string, properties ...any) Node {
	values := make(map[string]graphcontract.PropertyValue, len(properties)/2)
	for i := 0; i < len(properties); i += 2 {
		values[properties[i].(string)] = properties[i+1].(graphcontract.PropertyValue)
	}
	node, err := graphcontract.NewNode(id, "", []string{label}, values)
	if err != nil {
		panic(err)
	}
	return node
}

func testRoot(character rune) string { return strings.Repeat(string(character), 64) }
