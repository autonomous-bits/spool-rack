package sync

import (
	"bytes"
	"errors"
	"testing"
)

func TestContentIDBLAKE3Vector(t *testing.T) {
	t.Parallel()

	const emptyBLAKE3 = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
	if got := ContentID(nil); got != emptyBLAKE3 {
		t.Fatalf("ContentID(nil) = %q, want BLAKE3 empty vector %q", got, emptyBLAKE3)
	}
}

func TestPackFrameV2CanonicalRoundTrip(t *testing.T) {
	t.Parallel()

	object := []byte("canonical object")
	commit := CommitFrameV2{
		Version:      CommitFormatV2,
		Parents:      []CommitIdentity{LegacyCommitIdentity("legacy-parent")},
		SnapshotRoot: ContentID(object),
		Author:       "Ada",
		Message:      "v2 frame",
	}

	target, err := commit.Identity()
	if err != nil {
		t.Fatal(err)
	}
	frame := PackFrameV2{
		Version: PackFormatV2,
		Base:    LegacyCommitIdentity("legacy-parent"),
		Target:  target,
		Commits: []CommitFrameV2{commit},
		Objects: []PackObjectV2{{ID: ContentID(object), Data: object}},
	}
	data, err := MarshalPackFrameV2(frame)
	if err != nil {
		t.Fatalf("MarshalPackFrameV2() error = %v", err)
	}
	decoded, err := UnmarshalPackFrameV2(data)
	if err != nil {
		t.Fatalf("UnmarshalPackFrameV2() error = %v", err)
	}
	again, err := MarshalPackFrameV2(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, again) {
		t.Fatal("canonical pack frame changed after round trip")
	}

	noncanonical := append([]byte{0xb8, 0x07}, data[1:]...)
	if _, err := UnmarshalPackFrameV2(noncanonical); !errors.Is(err, ErrInvalidCanonicalFrame) {
		t.Fatalf("UnmarshalPackFrameV2(noncanonical) error = %v, want canonical error", err)
	}
}

func TestPackFrameV2RequiresObjectsForEveryCommitSnapshot(t *testing.T) {
	t.Parallel()

	snapshot := []byte("required snapshot")
	commit := CommitFrameV2{
		Version: CommitFormatV2, Parents: []CommitIdentity{},
		SnapshotRoot: ContentID(snapshot), Author: "Ada", Message: "root",
	}
	target, err := commit.Identity()
	if err != nil {
		t.Fatal(err)
	}
	_, err = MarshalPackFrameV2(PackFrameV2{
		Version: PackFormatV2, Target: target, Commits: []CommitFrameV2{commit},
		Objects: []PackObjectV2{},
	})
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("MarshalPackFrameV2() error = %v, want missing snapshot frame error", err)
	}
}

func TestPackFrameV3CanonicalRoundTrip(t *testing.T) {
	t.Parallel()

	object := []byte("canonical v3 object")
	commit := CommitFrameV2{
		Version:      CommitFormatV2,
		Parents:      []CommitIdentity{LegacyCommitIdentity("legacy-parent")},
		SnapshotRoot: ContentID(object),
		Author:       "Ada",
		Message:      "v3 frame",
	}

	target, err := commit.Identity()
	if err != nil {
		t.Fatal(err)
	}
	frame := PackFrameV3{
		Version: PackFormatV3,
		Base:    LegacyCommitIdentity("legacy-parent"),
		Target:  target,
		Commits: []CommitFrameV2{commit},
		Objects: []PackObjectV2{{ID: ContentID(object), Data: object}},
	}
	data, err := MarshalPackFrameV3(frame)
	if err != nil {
		t.Fatalf("MarshalPackFrameV3() error = %v", err)
	}
	decoded, err := UnmarshalPackFrameV3(data)
	if err != nil {
		t.Fatalf("UnmarshalPackFrameV3() error = %v", err)
	}
	again, err := MarshalPackFrameV3(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, again) {
		t.Fatal("canonical v3 pack frame changed after round trip")
	}

	noncanonical := append([]byte{0xb8, 0x07}, data[1:]...)
	if _, err := UnmarshalPackFrameV3(noncanonical); !errors.Is(err, ErrInvalidCanonicalFrame) {
		t.Fatalf("UnmarshalPackFrameV3(noncanonical) error = %v, want canonical error", err)
	}
}

func TestPackFrameV3SupportsDAG(t *testing.T) {
	t.Parallel()

	objRoot := []byte("root snapshot")
	objA := []byte("branch A snapshot")
	objB := []byte("branch B snapshot")
	objMerge := []byte("merge snapshot")

	rootCommit := CommitFrameV2{
		Version:      CommitFormatV2,
		Parents:      nil,
		SnapshotRoot: ContentID(objRoot),
		Author:       "Alice",
		Message:      "root",
	}
	rootID, err := rootCommit.Identity()
	if err != nil {
		t.Fatal(err)
	}

	commitA := CommitFrameV2{
		Version:      CommitFormatV2,
		Parents:      []CommitIdentity{rootID},
		SnapshotRoot: ContentID(objA),
		Author:       "Alice",
		Message:      "branch a",
	}
	idA, err := commitA.Identity()
	if err != nil {
		t.Fatal(err)
	}

	commitB := CommitFrameV2{
		Version:      CommitFormatV2,
		Parents:      []CommitIdentity{rootID},
		SnapshotRoot: ContentID(objB),
		Author:       "Bob",
		Message:      "branch b",
	}
	idB, err := commitB.Identity()
	if err != nil {
		t.Fatal(err)
	}

	mergeCommit := CommitFrameV2{
		Version:      CommitFormatV2,
		Parents:      []CommitIdentity{idA, idB},
		SnapshotRoot: ContentID(objMerge),
		Author:       "Alice",
		Message:      "merge",
	}
	targetID, err := mergeCommit.Identity()
	if err != nil {
		t.Fatal(err)
	}

	frameV3 := PackFrameV3{
		Version: PackFormatV3,
		Base:    CommitIdentity{},
		Target:  targetID,
		Commits: []CommitFrameV2{rootCommit, commitA, commitB, mergeCommit},
		Objects: []PackObjectV2{
			{ID: ContentID(objRoot), Data: objRoot},
			{ID: ContentID(objA), Data: objA},
			{ID: ContentID(objB), Data: objB},
			{ID: ContentID(objMerge), Data: objMerge},
		},
	}

	// PackFrameV3 must accept the DAG frame
	data, err := MarshalPackFrameV3(frameV3)
	if err != nil {
		t.Fatalf("MarshalPackFrameV3(DAG) error = %v, want success", err)
	}
	decoded, err := UnmarshalPackFrameV3(data)
	if err != nil {
		t.Fatalf("UnmarshalPackFrameV3(DAG) error = %v, want success", err)
	}
	if len(decoded.Commits) != 4 {
		t.Fatalf("commits = %d, want 4", len(decoded.Commits))
	}

	// PackFrameV2 must reject the same DAG frame because commitB.Parents[0] (root) != commitA (previous)
	frameV2 := PackFrameV2{
		Version: PackFormatV2,
		Base:    CommitIdentity{},
		Target:  targetID,
		Commits: []CommitFrameV2{rootCommit, commitA, commitB, mergeCommit},
		Objects: []PackObjectV2{
			{ID: ContentID(objRoot), Data: objRoot},
			{ID: ContentID(objA), Data: objA},
			{ID: ContentID(objB), Data: objB},
			{ID: ContentID(objMerge), Data: objMerge},
		},
	}
	if _, err := MarshalPackFrameV2(frameV2); err == nil {
		t.Fatal("MarshalPackFrameV2(DAG) want rejection, got nil")
	}
}
