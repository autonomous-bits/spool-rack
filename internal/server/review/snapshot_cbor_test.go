package review

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/sync"
)

func TestSnapshotCBORCanonicalRoundTripPreservesRackFields(t *testing.T) {
	t.Parallel()

	snapshot := Snapshot{
		Version: SnapshotVersion,
		Schema:  Schema{NodeLabels: []LabelRule{}, EdgeLabels: []LabelRule{}, Cardinalities: []CardinalityRule{}},
		Nodes: []Node{{
			ID: "m", Labels: []string{"Person"}, Properties: []Property{},
		}, {
			ID:     "n",
			Labels: []string{"Person"},
			Properties: []Property{
				{Name: "amount", Type: ValueTypeNumber, Value: json.RawMessage("1.2300e+04")},
				{Name: "name", Type: ValueTypeString, Value: json.RawMessage(`"Ada"`)},
			},
		}},
		Edges: []Edge{{
			ID: "e", From: "n", To: "m", Labels: []string{"FOLLOWS", "MENTIONS"},
			Properties: []Property{{Name: "weight", Type: ValueTypeNumber, Value: json.RawMessage("0.125")}},
		}},
	}

	data, err := MarshalSnapshotCBOR(snapshot)
	if err != nil {
		t.Fatalf("MarshalSnapshotCBOR() error = %v", err)
	}
	decoded, err := DecodeSnapshotCBOR(data)
	if err != nil {
		t.Fatalf("DecodeSnapshotCBOR() error = %v", err)
	}
	if got := string(decoded.Nodes[1].Properties[0].Value); got != "1.2300e+04" {
		t.Fatalf("number spelling = %q, want %q", got, "1.2300e+04")
	}
	if got := string(decoded.Edges[0].Properties[0].Value); got != "0.125" {
		t.Fatalf("decimal spelling = %q, want %q", got, "0.125")
	}
	if got := decoded.Edges[0].Labels; len(got) != 2 || got[0] != "FOLLOWS" || got[1] != "MENTIONS" {
		t.Fatalf("edge labels = %q, want both Rack labels", got)
	}
	again, err := MarshalSnapshotCBOR(decoded)
	if err != nil {
		t.Fatalf("MarshalSnapshotCBOR(decoded) error = %v", err)
	}
	if !bytes.Equal(data, again) {
		t.Fatal("canonical CBOR encoding changed after round trip")
	}
	if got := sync.ContentID(data); got != sync.ContentID(again) {
		t.Fatalf("content IDs differ after canonical round trip: %q", got)
	}

	jsonBridge, err := MarshalSnapshotJSON(decoded)
	if err != nil {
		t.Fatalf("MarshalSnapshotJSON() bridge error = %v", err)
	}
	bridged, err := DecodeSnapshotJSON(jsonBridge)
	if err != nil {
		t.Fatalf("DecodeSnapshotJSON() bridge error = %v", err)
	}
	if got := string(bridged.Nodes[1].Properties[0].Value); got != "1.2300e+04" {
		t.Fatalf("JSON bridge number spelling = %q, want %q", got, "1.2300e+04")
	}
}

func TestDecodeSnapshotCBORRejectsNonCanonicalAndMismatchedContract(t *testing.T) {
	t.Parallel()

	snapshot := Snapshot{
		Version: SnapshotVersion,
		Schema:  Schema{NodeLabels: []LabelRule{}, EdgeLabels: []LabelRule{}, Cardinalities: []CardinalityRule{}},
		Nodes:   []Node{{ID: "n", Labels: []string{"N"}, Properties: []Property{}}},
		Edges:   []Edge{},
	}
	data, err := MarshalSnapshotCBOR(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	noncanonical := append([]byte{0xb8, 0x04}, data[1:]...)
	if _, err := DecodeSnapshotCBOR(noncanonical); !errors.Is(err, ErrInvalidCanonicalCBOR) {
		t.Fatalf("DecodeSnapshotCBOR(noncanonical) error = %v, want canonical error", err)
	}

	envelope, err := newSnapshotEnvelope(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	envelope.Nodes[0].Title = "not-rack-owned"
	mismatched, err := snapshotCanonicalCBOR.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeSnapshotCBOR(mismatched); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("DecodeSnapshotCBOR(mismatched contract) error = %v, want invalid snapshot", err)
	}
}

func TestMarshalSnapshotCBORRejectsUnrepresentableNumber(t *testing.T) {
	t.Parallel()

	snapshot := Snapshot{
		Version: SnapshotVersion,
		Schema:  Schema{NodeLabels: []LabelRule{}, EdgeLabels: []LabelRule{}, Cardinalities: []CardinalityRule{}},
		Nodes: []Node{{
			ID: "n", Labels: []string{"N"},
			Properties: []Property{{Name: "tooLarge", Type: ValueTypeNumber, Value: json.RawMessage("1234567890123456789012345678901234567890")}},
		}},
		Edges: []Edge{},
	}
	_, err := MarshalSnapshotCBOR(snapshot)
	if !errors.Is(err, ErrUnsupportedSnapshotData) {
		t.Fatalf("MarshalSnapshotCBOR() error = %v, want unsupported snapshot data", err)
	}
}

func TestMarshalSnapshotCBORProjectsOnlyExactFloatValues(t *testing.T) {
	t.Parallel()

	snapshot := Snapshot{
		Version: SnapshotVersion,
		Schema:  Schema{NodeLabels: []LabelRule{}, EdgeLabels: []LabelRule{}, Cardinalities: []CardinalityRule{}},
		Nodes: []Node{{
			ID: "n", Labels: []string{"N"},
			Properties: []Property{
				{Name: "decimal", Type: ValueTypeNumber, Value: json.RawMessage("0.125")},
				{Name: "exponent", Type: ValueTypeNumber, Value: json.RawMessage("1.25e2")},
				{Name: "nested", Type: ValueTypeObject, Value: json.RawMessage(`{"decimal":0.5,"exponent":2.5e1}`)},
			},
		}},
		Edges: []Edge{},
	}

	envelope, err := newSnapshotEnvelope(snapshot)
	if err != nil {
		t.Fatalf("newSnapshotEnvelope() error = %v", err)
	}
	properties := envelope.Nodes[0].Properties
	if got := properties["decimal"].Float; got != 0.125 {
		t.Fatalf("decimal float = %v, want 0.125", got)
	}
	if got := properties["exponent"].Float; got != 125 {
		t.Fatalf("exponent float = %v, want 125", got)
	}
	if got := properties["nested"].Map["decimal"].Float; got != 0.5 {
		t.Fatalf("nested decimal float = %v, want 0.5", got)
	}
	if got := properties["nested"].Map["exponent"].Float; got != 25 {
		t.Fatalf("nested exponent float = %v, want 25", got)
	}

	data, err := MarshalSnapshotCBOR(snapshot)
	if err != nil {
		t.Fatalf("MarshalSnapshotCBOR() error = %v", err)
	}
	decoded, err := DecodeSnapshotCBOR(data)
	if err != nil {
		t.Fatalf("DecodeSnapshotCBOR() error = %v", err)
	}
	for _, property := range decoded.Nodes[0].Properties {
		switch property.Name {
		case "decimal":
			if got := string(property.Value); got != "0.125" {
				t.Fatalf("decimal JSON = %q, want %q", got, "0.125")
			}
		case "exponent":
			if got := string(property.Value); got != "1.25e2" {
				t.Fatalf("exponent JSON = %q, want %q", got, "1.25e2")
			}
		}
	}
}

func TestMarshalSnapshotCBORRejectsLossyFloatValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value json.RawMessage
		type_ ValueType
		want  string
	}{
		{name: "decimal", value: json.RawMessage("0.1"), type_: ValueTypeNumber, want: `number "0.1" cannot be represented exactly by graphcontract float64`},
		{name: "exponent", value: json.RawMessage("1e-1"), type_: ValueTypeNumber, want: `number "1e-1" cannot be represented exactly by graphcontract float64`},
		{name: "nested", value: json.RawMessage(`{"amount":[0.1]}`), type_: ValueTypeObject, want: `object key "amount": array item 0: number "0.1" cannot be represented exactly by graphcontract float64`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := Snapshot{
				Version: SnapshotVersion,
				Schema:  Schema{NodeLabels: []LabelRule{}, EdgeLabels: []LabelRule{}, Cardinalities: []CardinalityRule{}},
				Nodes: []Node{{
					ID: "n", Labels: []string{"N"},
					Properties: []Property{{Name: "value", Type: tc.type_, Value: tc.value}},
				}},
				Edges: []Edge{},
			}

			_, err := MarshalSnapshotCBOR(snapshot)
			if !errors.Is(err, ErrUnsupportedSnapshotData) {
				t.Fatalf("MarshalSnapshotCBOR() error = %v, want unsupported snapshot data", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("MarshalSnapshotCBOR() error = %q, want %q", err, tc.want)
			}
		})
	}
}

func TestSnapshotCBORRejectsNullGraphCollections(t *testing.T) {
	t.Parallel()

	snapshot := Snapshot{
		Version: SnapshotVersion,
		Schema:  Schema{NodeLabels: []LabelRule{}, EdgeLabels: []LabelRule{}, Cardinalities: []CardinalityRule{}},
	}
	if _, err := MarshalSnapshotCBOR(snapshot); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("MarshalSnapshotCBOR(nil collections) error = %v, want invalid snapshot", err)
	}
	data, err := snapshotCanonicalCBOR.Marshal(snapshotEnvelope{
		Version:  SnapshotEnvelopeVersion,
		Snapshot: snapshot,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeSnapshotCBOR(data); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("DecodeSnapshotCBOR(nil collections) error = %v, want invalid snapshot", err)
	}
}
