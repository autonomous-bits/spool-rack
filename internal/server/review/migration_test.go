package review

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	serversync "github.com/autonomous-bits/spool-rack/internal/server/sync"
)

func TestSnapshotMigratorResumesStagedDAGCutoverAndRollback(t *testing.T) {
	t.Parallel()

	driver, store, scope := migrationFixture(t)
	store.failPackStageOnce = true
	migrator := NewSnapshotMigrator(driver, store)

	if _, err := migrator.Migrate(context.Background(), "tenant-1", "repo-1"); err == nil {
		t.Fatal("first Migrate() error = nil, want interrupted staging error")
	}
	if store.status != "frozen" || len(store.objectMappings) != 2 || len(store.commitMappings) != 2 {
		t.Fatalf("interrupted stage = status %q, objects %d, commits %d; want frozen, 2, 2", store.status, len(store.objectMappings), len(store.commitMappings))
	}
	if store.completeCalls != 0 {
		t.Fatalf("CompleteSnapshotMigration() calls = %d, want 0 before staging succeeds", store.completeCalls)
	}

	result, err := migrator.Migrate(context.Background(), "tenant-1", "repo-1")
	if err != nil {
		t.Fatalf("resumed Migrate() error = %v", err)
	}
	if result.Generation != "migration-1" || result.Snapshots != 2 || result.Commits != 2 || result.Packs != 2 {
		t.Fatalf("Migrate() result = %+v, want complete staged counts", result)
	}
	if store.status != "cutover" || store.completeCalls != 1 {
		t.Fatalf("cutover status/calls = %q/%d, want cutover/1", store.status, store.completeCalls)
	}
	if store.head != store.commitMappings["legacy-child"] {
		t.Fatalf("branch head = %q, want staged v2 child %q", store.head, store.commitMappings["legacy-child"])
	}
	if store.commitMappings["legacy-root"] == "legacy-root" || store.commitMappings["legacy-child"] == "legacy-child" {
		t.Fatal("legacy IDs were reused instead of framing v2 commits")
	}

	for legacyRoot, cborRoot := range store.objectMappings {
		data, err := driver.Get(context.Background(), scope, cborRoot)
		if err != nil {
			t.Fatalf("Get(shadow %s) for legacy %s: %v", cborRoot, legacyRoot, err)
		}
		if _, err := DecodeSnapshotCBOR(data); err != nil {
			t.Fatalf("DecodeSnapshotCBOR(shadow %s): %v", cborRoot, err)
		}
		if _, err := driver.Get(context.Background(), scope, legacyRoot); err != nil {
			t.Fatalf("legacy snapshot %s was not retained: %v", legacyRoot, err)
		}
	}
	for legacyHash, v2Hash := range store.packMappings {
		pack, err := driver.OpenPack(context.Background(), scope, v2Hash)
		if err != nil {
			t.Fatalf("OpenPack(shadow %s): %v", v2Hash, err)
		}
		data, readErr := io.ReadAll(pack)
		closeErr := pack.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("read shadow pack: %v / %v", readErr, closeErr)
		}
		frame, err := serversync.UnmarshalPackFrameV2(data)
		if err != nil {
			t.Fatalf("UnmarshalPackFrameV2() error = %v", err)
		}
		if frame.LegacyPackID != legacyHash {
			t.Fatalf("legacy pack ID = %q, want opaque ID %q", frame.LegacyPackID, legacyHash)
		}
		legacy := migrationPackByHash(t, store.packs, legacyHash)
		legacyPack, err := driver.OpenPack(context.Background(), scope, legacyHash)
		if err != nil {
			t.Fatalf("OpenPack(legacy %s): %v", legacyHash, err)
		}
		legacyPayload, readErr := io.ReadAll(legacyPack)
		closeErr = legacyPack.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("read legacy pack: %v / %v", readErr, closeErr)
		}
		if !bytes.Equal(frame.LegacyPayload, legacyPayload) {
			t.Fatalf("legacy payload for %s was not retained verbatim", legacyHash)
		}
		if frame.Target != serversync.V2CommitIdentity(store.commitMappings[legacy.TargetID]) {
			t.Fatalf("migrated pack target = %+v, want mapped target %q", frame.Target, store.commitMappings[legacy.TargetID])
		}
		if len(frame.Commits) == 0 || len(frame.Objects) == 0 {
			t.Fatalf("migrated pack %s omitted v2 commits or converted snapshots", legacyHash)
		}
		last, err := frame.Commits[len(frame.Commits)-1].Identity()
		if err != nil {
			t.Fatalf("Identity(final migrated frame): %v", err)
		}
		if last != frame.Target {
			t.Fatalf("final migrated frame = %+v, want pack target %+v", last, frame.Target)
		}
		for _, object := range frame.Objects {
			if _, err := DecodeSnapshotCBOR(object.Data); err != nil {
				t.Fatalf("migrated pack object %s is not canonical CBOR: %v", object.ID, err)
			}
			if object.ID != serversync.ContentID(object.Data) {
				t.Fatalf("migrated pack object %s has invalid content ID", object.ID)
			}
		}
	}

	if err := migrator.Rollback(context.Background(), "tenant-1", "repo-1"); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if store.status != "rolled_back" || store.head != "legacy-child" {
		t.Fatalf("rollback = status %q head %q, want rolled_back/legacy-child", store.status, store.head)
	}
}

func migrationPackByHash(t *testing.T, packs []postgres.SnapshotMigrationPack, hash string) postgres.SnapshotMigrationPack {
	t.Helper()
	for _, pack := range packs {
		if pack.Hash == hash {
			return pack
		}
	}
	t.Fatalf("legacy pack %s is absent from migration fixture", hash)
	return postgres.SnapshotMigrationPack{}
}

func TestSnapshotMigratorPreflightRejectsLossyNumbersBeforeShadowWrites(t *testing.T) {
	t.Parallel()

	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	scope, err := cas.NewScope("tenant-1", "repo-1")
	if err != nil {
		t.Fatal(err)
	}
	legacy := Snapshot{
		Version: SnapshotVersion,
		Schema:  Schema{NodeLabels: []LabelRule{}, EdgeLabels: []LabelRule{}, Cardinalities: []CardinalityRule{}},
		Nodes: []Node{{
			ID: "n", Labels: []string{"N"},
			Properties: []Property{{Name: "unsafe", Type: ValueTypeNumber, Value: json.RawMessage("1234567890123456789012345678901234567890")}},
		}},
		Edges: []Edge{},
	}
	data, err := MarshalSnapshotJSON(legacy)
	if err != nil {
		t.Fatal(err)
	}
	root := serversync.ContentID(data)
	if err := driver.Put(context.Background(), scope, root, data); err != nil {
		t.Fatal(err)
	}
	store := &memoryMigrationStore{
		commits: []postgres.SnapshotMigrationCommit{{ID: "legacy", SnapshotRoot: root, Author: "Ada", Message: "unsafe"}},
	}
	_, err = NewSnapshotMigrator(driver, store).Migrate(context.Background(), "tenant-1", "repo-1")
	if !errors.Is(err, ErrMigrationPreflight) {
		t.Fatalf("Migrate() error = %v, want preflight error", err)
	}
	if store.status != "rolled_back" || len(store.objectMappings) != 0 || len(store.commitMappings) != 0 || store.completeCalls != 0 {
		t.Fatalf("preflight migration = status=%q objects=%d commits=%d complete=%d", store.status, len(store.objectMappings), len(store.commitMappings), store.completeCalls)
	}
}

func TestSnapshotMigratorMergePackIncludesSideHistory(t *testing.T) {
	t.Parallel()

	driver, store, scope := migrationFixture(t)
	sourceSnapshot := migrationSnapshot(`"source"`)
	mergeSnapshot := migrationSnapshot(`"merge"`)
	sourceData, err := MarshalSnapshotJSON(sourceSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	mergeData, err := MarshalSnapshotJSON(mergeSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	sourceRoot, mergeRoot := serversync.ContentID(sourceData), serversync.ContentID(mergeData)
	for root, data := range map[string][]byte{sourceRoot: sourceData, mergeRoot: mergeData} {
		if err := driver.Put(context.Background(), scope, root, data); err != nil {
			t.Fatal(err)
		}
	}
	sourcePack, mergePack := []byte("opaque legacy source pack"), []byte("opaque legacy merge pack")
	sourceHash, mergeHash := serversync.ContentID(sourcePack), serversync.ContentID(mergePack)
	for hash, data := range map[string][]byte{sourceHash: sourcePack, mergeHash: mergePack} {
		if err := driver.WritePack(context.Background(), scope, hash, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
	}
	store.commits = append(store.commits,
		postgres.SnapshotMigrationCommit{ID: "legacy-source", SnapshotRoot: sourceRoot, Author: "Ada", Message: "source", Parents: []string{"legacy-root"}},
		postgres.SnapshotMigrationCommit{ID: "legacy-merge", SnapshotRoot: mergeRoot, Author: "Ada", Message: "merge", Parents: []string{"legacy-child", "legacy-source"}},
	)
	store.packs = append(store.packs,
		postgres.SnapshotMigrationPack{Hash: sourceHash, BaseID: "legacy-root", TargetID: "legacy-source"},
		postgres.SnapshotMigrationPack{Hash: mergeHash, BaseID: "legacy-child", TargetID: "legacy-merge"},
	)
	store.head = "legacy-merge"

	if _, err := NewSnapshotMigrator(driver, store).Migrate(context.Background(), "tenant-1", "repo-1"); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	pack, err := driver.OpenPack(context.Background(), scope, store.packMappings[mergeHash])
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(pack)
	closeErr := pack.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read migrated merge pack: %v / %v", readErr, closeErr)
	}
	frame, err := serversync.UnmarshalPackFrameV2(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(frame.Commits) != 1 || len(frame.SupplementalCommits) != 1 {
		t.Fatalf("merge pack frames = %d primary / %d supplemental, want 1 / 1", len(frame.Commits), len(frame.SupplementalCommits))
	}
	sourceID := serversync.V2CommitIdentity(store.commitMappings["legacy-source"])
	supplemental, err := frame.SupplementalCommits[0].Identity()
	if err != nil || supplemental != sourceID {
		t.Fatalf("supplemental merge parent = %+v, %v; want %+v", supplemental, err, sourceID)
	}
	sourceCBOR := store.objectMappings[sourceRoot]
	foundSourceObject := false
	for _, object := range frame.Objects {
		if object.ID == sourceCBOR {
			foundSourceObject = true
		}
	}
	if !foundSourceObject {
		t.Fatalf("migrated merge pack omitted source snapshot %s", sourceCBOR)
	}
}

func migrationFixture(t *testing.T) (*cas.LocalDriver, *memoryMigrationStore, cas.Scope) {
	t.Helper()
	driver, err := cas.NewLocalDriver(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	scope, err := cas.NewScope("tenant-1", "repo-1")
	if err != nil {
		t.Fatal(err)
	}
	rootSnapshot := migrationSnapshot(`"root"`)
	childSnapshot := migrationSnapshot(`"child"`)
	rootData, err := MarshalSnapshotJSON(rootSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	childData, err := MarshalSnapshotJSON(childSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	rootObject := serversync.ContentID(rootData)
	childObject := serversync.ContentID(childData)
	for hash, data := range map[string][]byte{rootObject: rootData, childObject: childData} {
		if err := driver.Put(context.Background(), scope, hash, data); err != nil {
			t.Fatal(err)
		}
	}
	rootPack := []byte("opaque legacy root pack")
	childPack := []byte("opaque legacy child pack")
	rootPackHash, childPackHash := serversync.ContentID(rootPack), serversync.ContentID(childPack)
	for hash, data := range map[string][]byte{rootPackHash: rootPack, childPackHash: childPack} {
		if err := driver.WritePack(context.Background(), scope, hash, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
	}
	return driver, &memoryMigrationStore{
		commits: []postgres.SnapshotMigrationCommit{
			{ID: "legacy-root", SnapshotRoot: rootObject, Author: "Ada", Message: "root"},
			{ID: "legacy-child", SnapshotRoot: childObject, Author: "Ada", Message: "child", Parents: []string{"legacy-root"}},
		},
		packs: []postgres.SnapshotMigrationPack{
			{Hash: rootPackHash, TargetID: "legacy-root"},
			{Hash: childPackHash, BaseID: "legacy-root", TargetID: "legacy-child"},
		},
		head: "legacy-child",
	}, scope
}

func migrationSnapshot(value string) Snapshot {
	return Snapshot{
		Version: SnapshotVersion,
		Schema:  Schema{NodeLabels: []LabelRule{}, EdgeLabels: []LabelRule{}, Cardinalities: []CardinalityRule{}},
		Nodes: []Node{{
			ID: "n", Labels: []string{"N"},
			Properties: []Property{{Name: "value", Type: ValueTypeString, Value: json.RawMessage(value)}},
		}},
		Edges: []Edge{},
	}
}

type memoryMigrationStore struct {
	status            string
	generation        string
	commits           []postgres.SnapshotMigrationCommit
	packs             []postgres.SnapshotMigrationPack
	objectMappings    map[string]string
	commitMappings    map[string]string
	packMappings      map[string]string
	head              string
	completeCalls     int
	failPackStageOnce bool
}

func (s *memoryMigrationStore) SetTenantContext(ctx context.Context, tenantID string) (context.Context, error) {
	if tenantID != "tenant-1" {
		return nil, fmt.Errorf("unexpected tenant %q", tenantID)
	}
	return ctx, nil
}

func (s *memoryMigrationStore) BeginSnapshotMigration(context.Context, string) (postgres.SnapshotMigration, error) {
	if s.status == "cutover" {
		return postgres.SnapshotMigration{}, postgres.ErrSnapshotMigrationComplete
	}
	if s.generation == "" {
		s.generation = "migration-1"
	}
	if s.status == "" || s.status == "rolled_back" {
		s.status = "frozen"
	}
	return postgres.SnapshotMigration{RepoID: "repo-1", Generation: s.generation, Status: s.status}, nil
}

func (s *memoryMigrationStore) AbortSnapshotMigration(context.Context, string) error {
	if s.status != "frozen" {
		return errors.New("not frozen")
	}
	s.status = "rolled_back"
	return nil
}

func (s *memoryMigrationStore) ListSnapshotMigrationCommits(context.Context, string) ([]postgres.SnapshotMigrationCommit, error) {
	if s.status != "frozen" {
		return nil, errors.New("writers not frozen")
	}
	return append([]postgres.SnapshotMigrationCommit(nil), s.commits...), nil
}

func (s *memoryMigrationStore) ListSnapshotMigrationPacks(context.Context, string) ([]postgres.SnapshotMigrationPack, error) {
	if s.status != "frozen" {
		return nil, errors.New("writers not frozen")
	}
	return append([]postgres.SnapshotMigrationPack(nil), s.packs...), nil
}

func (s *memoryMigrationStore) StageSnapshotObjectMapping(_ context.Context, _ string, legacyRoot, cborRoot string) error {
	if s.objectMappings == nil {
		s.objectMappings = map[string]string{}
	}
	return stageMemoryMapping(s.objectMappings, legacyRoot, cborRoot)
}

func (s *memoryMigrationStore) StageSnapshotCommitMapping(_ context.Context, _ string, legacyID, v2ID, _, _ string) error {
	if s.commitMappings == nil {
		s.commitMappings = map[string]string{}
	}
	return stageMemoryMapping(s.commitMappings, legacyID, v2ID)
}

func (s *memoryMigrationStore) StageSnapshotPackMapping(_ context.Context, _ string, legacyHash, v2Hash, _, _, _, _ string) error {
	if s.failPackStageOnce {
		s.failPackStageOnce = false
		return errors.New("interrupted after shadow pack write")
	}
	if s.packMappings == nil {
		s.packMappings = map[string]string{}
	}
	return stageMemoryMapping(s.packMappings, legacyHash, v2Hash)
}

func (s *memoryMigrationStore) CompleteSnapshotMigration(_ context.Context, _ string) error {
	if s.status != "frozen" {
		return errors.New("not frozen")
	}
	if len(s.commitMappings) != len(s.commits) || len(s.packMappings) != len(s.packs) {
		return errors.New("incomplete staging")
	}
	s.completeCalls++
	s.status = "cutover"
	s.head = s.commitMappings[s.head]
	return nil
}

func (s *memoryMigrationStore) RollbackSnapshotMigration(_ context.Context, _ string) error {
	if s.status != "cutover" {
		return errors.New("not cut over")
	}
	for legacy, v2 := range s.commitMappings {
		if s.head == v2 {
			s.head = legacy
			s.status = "rolled_back"
			return nil
		}
	}
	return errors.New("unsafe rollback")
}

func stageMemoryMapping(mappings map[string]string, legacy, v2 string) error {
	if existing, ok := mappings[legacy]; ok && existing != v2 {
		return errors.New("nondeterministic mapping")
	}
	mappings[legacy] = v2
	return nil
}
