package review

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
)

func TestPreviewMerge_FastForwardUsesTenantScopedReadOnlyInputs(t *testing.T) {
	base := testSnapshot(testNode("n", testProperty("name", `"base"`)))
	source := testSnapshot(testNode("n", testProperty("name", `"source"`)))
	store, objects := testPreviewDependencies(t, map[string]Snapshot{
		testRoot('a'): base, testRoot('b'): source,
	}, map[string]string{"feature": "source", "main": "target"}, map[string]string{
		"target": testRoot('a'), "source": testRoot('b'),
	})
	store.fastForward = true

	preview, err := NewPreviewEngine(objects, store).PreviewMerge(context.Background(), "tenant-1", "repo-1", "feature", "main")
	if err != nil {
		t.Fatalf("PreviewMerge() error = %v", err)
	}
	if !preview.CanFastForward || preview.HasConflicts {
		t.Fatalf("PreviewMerge() fast-forward/conflicts = %t/%t, want true/false", preview.CanFastForward, preview.HasConflicts)
	}
	if preview.BaseCommit.ID != "target" || preview.SourceCommit.ID != "source" || preview.TargetCommit.ID != "target" {
		t.Fatalf("PreviewMerge() commit identities = %+v/%+v/%+v", preview.BaseCommit, preview.SourceCommit, preview.TargetCommit)
	}
	if len(preview.CleanChanges) != 1 || preview.CleanChanges[0].Kind != ChangeKindProperty || preview.CleanChanges[0].Branch != "source" {
		t.Fatalf("PreviewMerge() clean changes = %+v, want one source property update", preview.CleanChanges)
	}
	if store.lcaCalls != 0 || store.setTenantCalls != 1 || store.ancestorCalls != 1 {
		t.Fatalf("metadata read calls = tenant:%d ancestor:%d lca:%d, want 1/1/0", store.setTenantCalls, store.ancestorCalls, store.lcaCalls)
	}
	if len(objects.scopes) != 3 {
		t.Fatalf("CAS reads = %d, want 3", len(objects.scopes))
	}
	for _, scope := range objects.scopes {
		if scope.TenantID() != "tenant-1" || scope.RepoID() != "repo-1" {
			t.Fatalf("CAS scope = tenant %q repo %q, want tenant-1/repo-1", scope.TenantID(), scope.RepoID())
		}
	}
	if store.branchHeads["feature"] != "source" || store.branchHeads["main"] != "target" {
		t.Fatalf("PreviewMerge() mutated branch heads: %#v", store.branchHeads)
	}
}

func TestPreviewMerge_ReportsDeterministicPropertyAndDeleteUpdateConflicts(t *testing.T) {
	t.Run("property", func(t *testing.T) {
		base := testSnapshot(testNode("n", testProperty("name", `"base"`)))
		source := testSnapshot(testNode("n", testProperty("name", `"source"`)))
		target := testSnapshot(testNode("n", testProperty("name", `"target"`)))
		store, objects := testPreviewDependencies(t, map[string]Snapshot{
			testRoot('a'): base, testRoot('b'): source, testRoot('c'): target,
		}, map[string]string{"feature": "source", "main": "target"}, map[string]string{
			"base": testRoot('a'), "source": testRoot('b'), "target": testRoot('c'),
		})

		preview, err := NewPreviewEngine(objects, store).PreviewMerge(context.Background(), "tenant-1", "repo-1", "feature", "main")
		if err != nil {
			t.Fatalf("PreviewMerge() error = %v", err)
		}
		if store.lcaCalls != 1 || preview.BaseCommit.ID != "base" {
			t.Fatalf("LCA calls/base = %d/%q, want 1/base", store.lcaCalls, preview.BaseCommit.ID)
		}
		assertSingleConflict(t, preview, ConflictProperty, "property:node:n:name")
	})

	t.Run("delete versus update", func(t *testing.T) {
		base := testSnapshot(testNode("n", testProperty("name", `"base"`)))
		source := testSnapshot()
		target := testSnapshot(testNode("n", testProperty("name", `"target"`)))
		store, objects := testPreviewDependencies(t, map[string]Snapshot{
			testRoot('a'): base, testRoot('b'): source, testRoot('c'): target,
		}, map[string]string{"feature": "source", "main": "target"}, map[string]string{
			"base": testRoot('a'), "source": testRoot('b'), "target": testRoot('c'),
		})

		preview, err := NewPreviewEngine(objects, store).PreviewMerge(context.Background(), "tenant-1", "repo-1", "feature", "main")
		if err != nil {
			t.Fatalf("PreviewMerge() error = %v", err)
		}
		assertSingleConflict(t, preview, ConflictDeleteUpdate, "delete_update:node:n")
	})
}

func TestPreviewMerge_ReportsStructuralAndSchemaConflicts(t *testing.T) {
	t.Run("structural", func(t *testing.T) {
		base := testSnapshot(Node{ID: "n", Labels: []string{"node"}, Properties: []Property{}})
		source := testSnapshot(Node{ID: "n", Labels: []string{"source"}, Properties: []Property{}})
		target := testSnapshot(Node{ID: "n", Labels: []string{"target"}, Properties: []Property{}})
		store, objects := testPreviewDependencies(t, map[string]Snapshot{
			testRoot('a'): base, testRoot('b'): source, testRoot('c'): target,
		}, map[string]string{"feature": "source", "main": "target"}, map[string]string{
			"base": testRoot('a'), "source": testRoot('b'), "target": testRoot('c'),
		})

		preview, err := NewPreviewEngine(objects, store).PreviewMerge(context.Background(), "tenant-1", "repo-1", "feature", "main")
		if err != nil {
			t.Fatalf("PreviewMerge() error = %v", err)
		}
		assertSingleConflict(t, preview, ConflictStructural, "structural:node:n")
	})

	t.Run("schema", func(t *testing.T) {
		base := testSnapshot()
		source := testSnapshot()
		source.Schema.NodeLabels = []LabelRule{{Label: "Source", Properties: []PropertyRule{}}}
		target := testSnapshot()
		target.Schema.NodeLabels = []LabelRule{{Label: "Target", Properties: []PropertyRule{}}}
		store, objects := testPreviewDependencies(t, map[string]Snapshot{
			testRoot('a'): base, testRoot('b'): source, testRoot('c'): target,
		}, map[string]string{"feature": "source", "main": "target"}, map[string]string{
			"base": testRoot('a'), "source": testRoot('b'), "target": testRoot('c'),
		})

		preview, err := NewPreviewEngine(objects, store).PreviewMerge(context.Background(), "tenant-1", "repo-1", "feature", "main")
		if err != nil {
			t.Fatalf("PreviewMerge() error = %v", err)
		}
		assertSingleConflict(t, preview, ConflictSchema, "schema:schema")
	})
}

func TestPreviewMerge_ReportsCardinalityConflictAfterCleanStructuralChanges(t *testing.T) {
	max := 1
	schema := Schema{
		NodeLabels: []LabelRule{}, EdgeLabels: []LabelRule{},
		Cardinalities: []CardinalityRule{{EdgeLabel: "links", FromLabel: "from", ToLabel: "to", Min: 0, Max: &max}},
	}
	nodes := []Node{
		{ID: "a", Labels: []string{"from"}, Properties: []Property{}},
		{ID: "b", Labels: []string{"to"}, Properties: []Property{}},
		{ID: "c", Labels: []string{"to"}, Properties: []Property{}},
	}
	base := Snapshot{Version: SnapshotVersion, Schema: schema, Nodes: nodes, Edges: []Edge{}}
	source := Snapshot{Version: SnapshotVersion, Schema: schema, Nodes: nodes, Edges: []Edge{{ID: "e1", From: "a", To: "b", Labels: []string{"links"}, Properties: []Property{}}}}
	target := Snapshot{Version: SnapshotVersion, Schema: schema, Nodes: nodes, Edges: []Edge{{ID: "e2", From: "a", To: "c", Labels: []string{"links"}, Properties: []Property{}}}}
	store, objects := testPreviewDependencies(t, map[string]Snapshot{
		testRoot('a'): base, testRoot('b'): source, testRoot('c'): target,
	}, map[string]string{"feature": "source", "main": "target"}, map[string]string{
		"base": testRoot('a'), "source": testRoot('b'), "target": testRoot('c'),
	})

	preview, err := NewPreviewEngine(objects, store).PreviewMerge(context.Background(), "tenant-1", "repo-1", "feature", "main")
	if err != nil {
		t.Fatalf("PreviewMerge() error = %v", err)
	}
	assertSingleConflict(t, preview, ConflictCardinality, "cardinality:schema")
	if len(preview.CleanChanges) != 2 {
		t.Fatalf("clean changes = %+v, want two independent edge additions", preview.CleanChanges)
	}
}

func TestPreviewMerge_WrapsReadErrors(t *testing.T) {
	store, objects := testPreviewDependencies(t, nil, map[string]string{"feature": "source", "main": "target"}, nil)
	store.ancestorErr = errTest
	_, err := NewPreviewEngine(objects, store).PreviewMerge(context.Background(), "tenant-1", "repo-1", "feature", "main")
	if !errors.Is(err, errTest) {
		t.Fatalf("PreviewMerge() error = %v, want wrapped %v", err, errTest)
	}
}

func assertSingleConflict(t *testing.T, preview *MergePreview, wantType ConflictType, wantToken string) {
	t.Helper()
	if !preview.HasConflicts || len(preview.Conflicts) != 1 || preview.Conflicts[0].Type != wantType || preview.Conflicts[0].Token != wantToken {
		t.Fatalf("conflicts = %+v, want one %s (%s)", preview.Conflicts, wantType, wantToken)
	}
	if len(preview.Resolutions) != 1 || !preview.Resolutions[0].Required || preview.Resolutions[0].Token != wantToken {
		t.Fatalf("resolution requirements = %+v, want required resolution for %s", preview.Resolutions, wantToken)
	}
}

var errTest = errors.New("test read error")

type fakePreviewStore struct {
	branchHeads                             map[string]string
	snapshotRoots                           map[string]string
	fastForward                             bool
	ancestorErr                             error
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
	return s.fastForward, s.ancestorErr
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
			t.Fatalf("marshal snapshot: %v", err)
		}
		data[root] = encoded
	}
	return &fakePreviewStore{branchHeads: heads, snapshotRoots: roots}, &fakePreviewObjects{data: data}
}

func testSnapshot(nodes ...Node) Snapshot {
	return Snapshot{Version: SnapshotVersion, Schema: Schema{NodeLabels: []LabelRule{}, EdgeLabels: []LabelRule{}, Cardinalities: []CardinalityRule{}}, Nodes: append([]Node{}, nodes...), Edges: []Edge{}}
}

func testNode(id string, properties ...Property) Node {
	return Node{ID: id, Labels: []string{"node"}, Properties: properties}
}

func testProperty(name, value string) Property {
	return Property{Name: name, Type: ValueTypeString, Value: json.RawMessage(value)}
}

func testRoot(character rune) string {
	return strings.Repeat(string(character), 64)
}
