package review

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/autonomous-bits/spool/graphcontract"
)

func TestSnapshotCBORCanonicalRoundTrip(t *testing.T) {
	t.Parallel()

	snapshot := testSnapshot(testNode("n", "Person", "external_id", graphcontract.FloatPropertyValue(1.25)))
	data, err := MarshalSnapshotCBOR(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeSnapshotCBOR(data)
	if err != nil {
		t.Fatal(err)
	}
	again, err := MarshalSnapshotCBOR(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, again) {
		t.Fatal("canonical encoding changed after round trip")
	}
}

func TestSnapshotCBORRejectsNonCanonicalOrLegacyEnvelope(t *testing.T) {
	t.Parallel()

	data, err := MarshalSnapshotCBOR(testSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	noncanonical := append([]byte{0xb8, 0x04}, data[1:]...)
	if _, err := DecodeSnapshotCBOR(noncanonical); !errors.Is(err, ErrInvalidCanonicalCBOR) {
		t.Fatalf("DecodeSnapshotCBOR() error = %v, want canonical error", err)
	}
	legacy, err := snapshotCanonicalCBOR.Marshal(map[uint64]uint32{1: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeSnapshotCBOR(legacy); !errors.Is(err, ErrUnsupportedSnapshotVersion) ||
		!strings.Contains(err.Error(), "migrate the repository") {
		t.Fatalf("DecodeSnapshotCBOR(v2) error = %v, want actionable legacy-version error", err)
	}
}
