package review

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool/graphcontract"
	"github.com/fxamacker/cbor/v2"
)

// SnapshotVersion identifies Rack's canonical graphcontract snapshot envelope.
const SnapshotVersion uint32 = 3

// SnapshotEnvelopeVersion is retained as an explicit on-disk compatibility
// marker independent of the schema version embedded in each snapshot.
const SnapshotEnvelopeVersion uint32 = 3

var (
	ErrInvalidSnapshot            = errors.New("review: invalid graph snapshot")
	ErrUnsupportedSnapshotVersion = errors.New("review: unsupported graph snapshot version")
	ErrInvalidSnapshotRoot        = errors.New("review: invalid snapshot root")
	ErrInvalidCanonicalCBOR       = errors.New("review: invalid canonical snapshot CBOR")
)

var snapshotRootPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

var (
	snapshotCanonicalCBOR, _ = cbor.CanonicalEncOptions().EncMode()
	snapshotCBORDecoder, _   = cbor.DecOptions{
		DupMapKey:         cbor.DupMapKeyEnforcedAPF,
		IndefLength:       cbor.IndefLengthForbidden,
		TagsMd:            cbor.TagsForbidden,
		ExtraReturnErrors: cbor.ExtraDecErrorUnknownField,
	}.DecMode()
	snapshotVersionDecoder, _ = cbor.DecOptions{
		DupMapKey:   cbor.DupMapKeyEnforcedAPF,
		IndefLength: cbor.IndefLengthForbidden,
		TagsMd:      cbor.TagsForbidden,
	}.DecMode()
)

// Canonical graph entities are owned by the shared graph contract.
type Node = graphcontract.Node
type Edge = graphcontract.Edge
type Schema = graphcontract.SchemaSnapshot

// Snapshot is Rack's immutable review object. Schema and graph objects retain
// graphcontract's native representation; Rack does not translate schema or
// property semantics into a JSON-specific DTO.
type Snapshot struct {
	Version uint32
	Schema  graphcontract.SchemaSnapshot
	Nodes   map[string]graphcontract.Node
	Edges   map[string]graphcontract.Edge
}

// SnapshotObjectStore is the minimal CAS capability needed to decode a graph
// snapshot.
type SnapshotObjectStore interface {
	Get(ctx context.Context, scope cas.Scope, hash string) ([]byte, error)
}

// SnapshotDecoder loads and validates immutable graph snapshots from CAS.
type SnapshotDecoder struct{ objects SnapshotObjectStore }

func NewSnapshotDecoder(objects SnapshotObjectStore) *SnapshotDecoder {
	return &SnapshotDecoder{objects: objects}
}

func (d *SnapshotDecoder) Decode(ctx context.Context, scope cas.Scope, snapshotRoot string) (Snapshot, error) {
	if d == nil || d.objects == nil {
		return Snapshot{}, fmt.Errorf("%w: object store is required", ErrInvalidSnapshot)
	}
	return DecodeSnapshot(ctx, d.objects, scope, snapshotRoot)
}

func DecodeSnapshot(ctx context.Context, objects SnapshotObjectStore, scope cas.Scope, snapshotRoot string) (Snapshot, error) {
	if !snapshotRootPattern.MatchString(snapshotRoot) {
		return Snapshot{}, fmt.Errorf("%w: expected 64 lowercase hexadecimal characters", ErrInvalidSnapshotRoot)
	}
	if objects == nil {
		return Snapshot{}, fmt.Errorf("%w: object store is required", ErrInvalidSnapshot)
	}
	data, err := objects.Get(ctx, scope, snapshotRoot)
	if err != nil {
		return Snapshot{}, fmt.Errorf("review: load snapshot root %s: %w", snapshotRoot, err)
	}
	snapshot, err := DecodeSnapshotObject(data)
	if err != nil {
		return Snapshot{}, fmt.Errorf("review: decode snapshot root %s: %w", snapshotRoot, err)
	}
	return snapshot, nil
}

// DecodeSnapshotObject decodes a canonical graphcontract snapshot. Legacy JSON
// review payloads are deliberately rejected: their Rack-local schema cannot
// express the canonical graph-contract semantics.
func DecodeSnapshotObject(data []byte) (Snapshot, error) {
	if trimmed := bytes.TrimSpace(data); len(trimmed) != 0 && (trimmed[0] == '{' || trimmed[0] == '[') {
		return Snapshot{}, fmt.Errorf("%w: legacy JSON review snapshots are unsupported; publish a canonical graphcontract schema snapshot", ErrUnsupportedSnapshotVersion)
	}
	return DecodeSnapshotCBOR(data)
}

// DecodeSnapshotJSON is retained only to return an actionable compatibility
// error to callers of Rack's former JSON review payload API.
func DecodeSnapshotJSON([]byte) (Snapshot, error) {
	return Snapshot{}, fmt.Errorf("%w: JSON review snapshots are unsupported; use canonical graphcontract CBOR", ErrUnsupportedSnapshotVersion)
}

// MarshalSnapshotJSON is retained only to return an actionable compatibility
// error. Rack no longer serializes a lossy JSON review schema.
func MarshalSnapshotJSON(Snapshot) ([]byte, error) {
	return nil, fmt.Errorf("%w: JSON review snapshots are unsupported; use canonical graphcontract CBOR", ErrUnsupportedSnapshotVersion)
}

func MarshalSnapshotCBOR(snapshot Snapshot) ([]byte, error) {
	envelope, err := newSnapshotEnvelope(snapshot)
	if err != nil {
		return nil, err
	}
	data, err := snapshotCanonicalCBOR.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("%w: encode CBOR: %v", ErrInvalidSnapshot, err)
	}
	return data, nil
}

func DecodeSnapshotCBOR(data []byte) (Snapshot, error) {
	var header snapshotEnvelopeHeader
	if err := snapshotVersionDecoder.Unmarshal(data, &header); err != nil {
		return Snapshot{}, fmt.Errorf("%w: decode CBOR envelope header: %v", ErrInvalidCanonicalCBOR, err)
	}
	if header.Version == 2 {
		return Snapshot{}, fmt.Errorf("%w: Rack v2 CBOR snapshots contain the retired JSON review schema; migrate the repository to a canonical graphcontract schema snapshot", ErrUnsupportedSnapshotVersion)
	}
	if header.Version != SnapshotEnvelopeVersion {
		return Snapshot{}, fmt.Errorf("%w: envelope version %d", ErrUnsupportedSnapshotVersion, header.Version)
	}
	var envelope snapshotEnvelope
	if err := snapshotCBORDecoder.Unmarshal(data, &envelope); err != nil {
		return Snapshot{}, fmt.Errorf("%w: decode CBOR: %v", ErrInvalidCanonicalCBOR, err)
	}
	canonical, err := snapshotCanonicalCBOR.Marshal(envelope)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: re-encode CBOR: %v", ErrInvalidCanonicalCBOR, err)
	}
	if !bytes.Equal(data, canonical) {
		return Snapshot{}, fmt.Errorf("%w: object is not canonically encoded", ErrInvalidCanonicalCBOR)
	}
	snapshot := Snapshot{Version: SnapshotVersion, Schema: envelope.Schema, Nodes: envelope.Nodes, Edges: envelope.Edges}
	if err := snapshot.Validate(); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func MarshalSnapshot(snapshot Snapshot) ([]byte, error) { return MarshalSnapshotCBOR(snapshot) }

// snapshotEnvelope contains only canonical graphcontract values. It has no
// Rack-owned graph or schema mirror to drift from the validation authority.
type snapshotEnvelope struct {
	Version uint32                        `cbor:"1,keyasint"`
	Schema  graphcontract.SchemaSnapshot  `cbor:"2,keyasint"`
	Nodes   map[string]graphcontract.Node `cbor:"3,keyasint"`
	Edges   map[string]graphcontract.Edge `cbor:"4,keyasint"`
}

type snapshotEnvelopeHeader struct {
	Version uint32 `cbor:"1,keyasint"`
}

func newSnapshotEnvelope(snapshot Snapshot) (snapshotEnvelope, error) {
	if err := snapshot.Validate(); err != nil {
		return snapshotEnvelope{}, err
	}
	return snapshotEnvelope{
		Version: SnapshotEnvelopeVersion,
		Schema:  snapshot.Schema,
		Nodes:   snapshot.Nodes,
		Edges:   snapshot.Edges,
	}, nil
}

// Validate verifies the canonical schema and graph through graphcontract. Rack
// only wraps the error with storage-boundary context and never interprets
// individual schema rules or violations.
func (s Snapshot) Validate() error {
	if s.Version != SnapshotVersion {
		return fmt.Errorf("%w: version %d", ErrUnsupportedSnapshotVersion, s.Version)
	}
	if s.Nodes == nil || s.Edges == nil {
		return fmt.Errorf("%w: nodes and edges must be maps", ErrInvalidSnapshot)
	}
	normalized, err := s.Schema.Normalize()
	if err != nil {
		return fmt.Errorf("%w: schema: %w", ErrInvalidSnapshot, err)
	}
	if !reflect.DeepEqual(s.Schema, normalized) {
		return fmt.Errorf("%w: schema must be normalized", ErrInvalidSnapshot)
	}
	if err := graphcontract.ValidateSchemaSnapshot(s.Schema, s.Nodes, s.Edges); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSnapshot, err)
	}
	return nil
}
