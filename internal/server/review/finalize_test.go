package review

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/autonomous-bits/spool/graphcontract"
)

func TestMaterializeMergeAppliesCanonicalPropertyResolution(t *testing.T) {
	t.Parallel()

	base := testSnapshot(testNode("n", "Person", "name", graphcontract.StringPropertyValue("base")))
	source := testSnapshot(testNode("n", "Person", "name", graphcontract.StringPropertyValue("source")))
	target := testSnapshot(testNode("n", "Person", "name", graphcontract.StringPropertyValue("target")))
	preview := mergeSnapshots(base, source, target)
	merged, err := materializeMerge(base, source, target, &preview, []Resolution{{Token: "property:node:n:name", Choice: "source"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := merged.Nodes["n"].Properties["name"]; !got.Equal(graphcontract.StringPropertyValue("source")) {
		t.Fatalf("resolved property = %#v, want source value", got)
	}
}

func TestManualResolutionDecodesCanonicalTypedValue(t *testing.T) {
	t.Parallel()

	data, err := json.Marshal(graphcontract.StringPropertyValue("manual"))
	if err != nil {
		t.Fatal(err)
	}
	value, exists, err := resolutionProperty(Snapshot{}, Snapshot{}, ConflictToken{Property: "name"}, Resolution{Choice: "manual", Value: data})
	if err != nil || !exists || !value.Equal(graphcontract.StringPropertyValue("manual")) {
		t.Fatalf("resolutionProperty() = %#v, %t, %v", value, exists, err)
	}
}

func TestMaterializeMergeRejectsCanonicalSchemaViolations(t *testing.T) {
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
	if err := snapshot.Validate(); !errors.Is(err, graphcontract.ErrSchemaValidation) {
		t.Fatalf("Snapshot.Validate() error = %v, want canonical validation error", err)
	}
}
