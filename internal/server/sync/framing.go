package sync

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/autonomous-bits/spool/graphcontract"
	"github.com/fxamacker/cbor/v2"
	"lukechampine.com/blake3"
)

const (
	// MaxV2PackBytes bounds every canonical v2 frame accepted or produced by
	// Rack so a pack cannot force unbounded heap use.
	MaxV2PackBytes int64 = 64 << 20
	// CommitFormatLegacy identifies an opaque v1 commit ID. Rack must retain
	// these IDs exactly; their content is outside the v2 framing contract.
	CommitFormatLegacy uint32 = 1
	// CommitFormatV2 identifies a commit ID derived from CommitFrameV2.
	CommitFormatV2 uint32 = 2
	// PackFormatV2 identifies a canonical Rack pack frame.
	PackFormatV2 uint32 = 2
)

var (
	// ErrInvalidFrame indicates malformed, inconsistent, or unsupported Rack
	// v2 commit/pack framing.
	ErrInvalidFrame = errors.New("sync: invalid v2 frame")
	// ErrInvalidCanonicalFrame indicates a valid-but-noncanonical CBOR frame.
	ErrInvalidCanonicalFrame = errors.New("sync: invalid canonical v2 frame")
)

var (
	frameCanonicalCBOR, _ = cbor.CanonicalEncOptions().EncMode()
	frameCBORDecoder, _   = cbor.DecOptions{
		DupMapKey:         cbor.DupMapKeyEnforcedAPF,
		IndefLength:       cbor.IndefLengthForbidden,
		TagsMd:            cbor.TagsForbidden,
		ExtraReturnErrors: cbor.ExtraDecErrorUnknownField,
	}.DecMode()
)

// ObjectID is the canonical content-derived identifier for a graph object.
type ObjectID = graphcontract.ObjectID

// CommitIdentity retains the explicit object framing marker used by Rack's
// outer pack envelope. The canonical commit itself is graphcontract.Commit.
type CommitIdentity struct {
	Format uint32 `json:"format,omitempty" cbor:"1,keyasint"`
	ID     string `json:"id" cbor:"2,keyasint"`
}

func LegacyCommitIdentity(id string) CommitIdentity {
	return CommitIdentity{Format: CommitFormatLegacy, ID: id}
}

func V2CommitIdentity(id string) CommitIdentity {
	return CommitIdentity{Format: CommitFormatV2, ID: id}
}

func (id CommitIdentity) Validate() error {
	if id.ID == "" {
		return fmt.Errorf("%w: commit ID is required", ErrInvalidFrame)
	}
	if id.Format != CommitFormatLegacy && id.Format != CommitFormatV2 {
		return fmt.Errorf("%w: unsupported commit format %d", ErrInvalidFrame, id.Format)
	}
	if id.Format == CommitFormatV2 && !validContentID(id.ID) {
		return fmt.Errorf("%w: v2 commit ID must be a BLAKE3-256 content ID", ErrInvalidFrame)
	}
	return nil
}

// CommitFrameV2 carries the Rack pack envelope's explicit framing metadata.
// Its identity is always derived from the equivalent graphcontract.Commit.
type CommitFrameV2 struct {
	Version      uint32           `cbor:"1,keyasint"`
	Parents      []CommitIdentity `cbor:"2,keyasint"`
	SnapshotRoot string           `cbor:"3,keyasint"`
	Author       string           `cbor:"4,keyasint"`
	Message      string           `cbor:"5,keyasint"`
	Time         time.Time        `cbor:"6,keyasint"`
}

// Commit returns the authoritative graphcontract record without changing
// parent order.
func (f CommitFrameV2) Commit() (graphcontract.Commit, error) {
	parents := make([]graphcontract.ObjectID, len(f.Parents))
	for i, parent := range f.Parents {
		if err := parent.Validate(); err != nil {
			return graphcontract.Commit{}, fmt.Errorf("%w: parent %d: %v", ErrInvalidFrame, i, err)
		}
		parents[i] = graphcontract.ObjectID(parent.ID)
	}
	return graphcontract.NewCommit(
		graphcontract.ObjectID(f.SnapshotRoot), parents, f.Author, f.Message, f.Time,
	)
}

// CommitObjectID derives the Rack object identifier from Spool's canonical
// encoding, preserving graphcontract's parent order exactly.
func CommitObjectID(commit graphcontract.Commit) (graphcontract.ObjectID, error) {
	data, err := graphcontract.MarshalCommit(commit)
	if err != nil {
		return "", err
	}
	return graphcontract.ObjectID(ContentID(data)), nil
}

// Identity derives the v2 reference from the canonical graphcontract record.
func (f CommitFrameV2) Identity() (CommitIdentity, error) {
	commit, err := f.Commit()
	if err != nil {
		return CommitIdentity{}, err
	}
	id, err := CommitObjectID(commit)
	if err != nil {
		return CommitIdentity{}, err
	}
	return V2CommitIdentity(string(id)), nil
}

// PackObjectV2 is one immutable CAS object carried by a new v2 pack.
type PackObjectV2 struct {
	ID   string `cbor:"1,keyasint"`
	Data []byte `cbor:"2,keyasint"`
}

// PackFrameV2 is one bounded, canonical v2 slice of a commit DAG. Its
// first-parent chain advances Base to Target; merge parents outside that chain
// are supplied by separately ordered packs in a pull envelope.
type PackFrameV2 struct {
	Version uint32          `cbor:"1,keyasint"`
	Base    CommitIdentity  `cbor:"2,keyasint"`
	Target  CommitIdentity  `cbor:"3,keyasint"`
	Commits []CommitFrameV2 `cbor:"4,keyasint"`
	Objects []PackObjectV2  `cbor:"5,keyasint"`
}

// MarshalPackFrameV2 validates and returns the canonical encoding of frame.
func MarshalPackFrameV2(frame PackFrameV2) ([]byte, error) {
	if err := frame.validate(); err != nil {
		return nil, err
	}
	data, err := frameCanonicalCBOR.Marshal(frame)
	if err != nil {
		return nil, fmt.Errorf("%w: encode pack: %v", ErrInvalidFrame, err)
	}
	if int64(len(data)) > MaxV2PackBytes {
		return nil, fmt.Errorf("%w: pack exceeds %d byte limit", ErrInvalidFrame, MaxV2PackBytes)
	}
	return data, nil
}

// UnmarshalPackFrameV2 verifies that data is a canonical, self-consistent v2
// pack frame before allowing a caller to register its metadata.
func UnmarshalPackFrameV2(data []byte) (PackFrameV2, error) {
	if int64(len(data)) > MaxV2PackBytes {
		return PackFrameV2{}, fmt.Errorf("%w: pack exceeds %d byte limit", ErrInvalidCanonicalFrame, MaxV2PackBytes)
	}
	var frame PackFrameV2
	if err := frameCBORDecoder.Unmarshal(data, &frame); err != nil {
		return PackFrameV2{}, fmt.Errorf("%w: decode pack: %v", ErrInvalidCanonicalFrame, err)
	}
	canonical, err := MarshalPackFrameV2(frame)
	if err != nil {
		return PackFrameV2{}, err
	}
	if !bytes.Equal(data, canonical) {
		return PackFrameV2{}, fmt.Errorf("%w: object is not canonically encoded", ErrInvalidCanonicalFrame)
	}
	return frame, nil
}

func (f PackFrameV2) validate() error {
	if f.Version != PackFormatV2 {
		return fmt.Errorf("%w: unsupported pack frame version %d", ErrInvalidFrame, f.Version)
	}
	if f.Base.ID != "" || f.Base.Format != 0 {
		if err := f.Base.Validate(); err != nil {
			return fmt.Errorf("%w: base commit: %v", ErrInvalidFrame, err)
		}
	}
	if err := f.Target.Validate(); err != nil {
		return fmt.Errorf("%w: target commit: %v", ErrInvalidFrame, err)
	}
	if len(f.Commits) == 0 {
		return fmt.Errorf("%w: advancing pack must contain a commit frame", ErrInvalidFrame)
	}
	previous := f.Base
	for i, commit := range f.Commits {
		if _, err := commit.Commit(); err != nil {
			return fmt.Errorf("%w: commit %d: %v", ErrInvalidFrame, i, err)
		}
		if len(commit.Parents) == 0 {
			if i == 0 && previous.ID == "" && previous.Format == 0 {
				identity, err := commit.Identity()
				if err != nil {
					return err
				}
				previous = identity
				continue
			}
			return fmt.Errorf("%w: commit %d does not continue the first-parent pack range", ErrInvalidFrame, i)
		}
		if commit.Parents[0] != previous {
			return fmt.Errorf("%w: commit %d does not continue the first-parent pack range", ErrInvalidFrame, i)
		}
		identity, err := commit.Identity()
		if err != nil {
			return err
		}
		previous = identity
	}
	if previous != f.Target {
		return fmt.Errorf("%w: target does not name the final commit frame", ErrInvalidFrame)
	}
	seen := make(map[string]struct{}, len(f.Objects))
	for i, object := range f.Objects {
		if !validContentID(object.ID) {
			return fmt.Errorf("%w: object %d has invalid content ID", ErrInvalidFrame, i)
		}
		if ContentID(object.Data) != object.ID {
			return fmt.Errorf("%w: object %d content ID mismatch", ErrInvalidFrame, i)
		}
		if _, ok := seen[object.ID]; ok {
			return fmt.Errorf("%w: duplicate object %q", ErrInvalidFrame, object.ID)
		}
		seen[object.ID] = struct{}{}
	}
	for _, commit := range f.Commits {
		if _, found := seen[commit.SnapshotRoot]; !found {
			identity, err := commit.Identity()
			if err != nil {
				return err
			}
			return fmt.Errorf("%w: commit %s snapshot %s is not available in the pack", ErrInvalidFrame, identity.ID, commit.SnapshotRoot)
		}
	}
	return nil
}

// ContentID returns the lowercase BLAKE3-256 content ID used by Rack CAS.
func ContentID(data []byte) string {
	sum := blake3.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func validContentID(id string) bool {
	if len(id) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(id)
	return err == nil && hex.EncodeToString(decoded) == id
}

// SortPackObjects orders object frames by their immutable content IDs. It is
// useful for callers that build frames from map-backed object collections.
func SortPackObjects(objects []PackObjectV2) {
	sort.Slice(objects, func(i, j int) bool { return objects[i].ID < objects[j].ID })
}
