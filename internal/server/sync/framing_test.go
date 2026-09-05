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
