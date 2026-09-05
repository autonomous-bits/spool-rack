package sync

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

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
	// PackFormatLegacy identifies an opaque v1 pack payload.
	PackFormatLegacy uint32 = 1
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

// CommitIdentity is a universal reference to either an opaque legacy commit
// or a deterministic v2 commit frame. IDs are never inferred from their
// spelling: their format travels with the reference wherever a v2 frame
// needs to distinguish a legacy parent from a v2 one.
type CommitIdentity struct {
	Format uint32 `json:"format,omitempty" cbor:"1,keyasint"`
	ID     string `json:"id" cbor:"2,keyasint"`
}

// LegacyCommitIdentity names an opaque v1 commit without re-deriving it.
func LegacyCommitIdentity(id string) CommitIdentity {
	return CommitIdentity{Format: CommitFormatLegacy, ID: id}
}

// V2CommitIdentity names a deterministic v2 commit frame.
func V2CommitIdentity(id string) CommitIdentity {
	return CommitIdentity{Format: CommitFormatV2, ID: id}
}

// Validate verifies an explicitly framed commit reference.
func (id CommitIdentity) Validate() error {
	return id.validate()
}

func (id CommitIdentity) validate() error {
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

// CommitFrameV2 is the canonical preimage of a v2 commit ID. Parents retain
// their own explicit format, so a migrated DAG does not reinterpret v1 IDs.
type CommitFrameV2 struct {
	Version      uint32           `cbor:"1,keyasint"`
	Parents      []CommitIdentity `cbor:"2,keyasint"`
	SnapshotRoot string           `cbor:"3,keyasint"`
	Author       string           `cbor:"4,keyasint"`
	Message      string           `cbor:"5,keyasint"`
}

// CanonicalCommitFrameV2 returns the unique CBOR preimage for a v2 commit.
func CanonicalCommitFrameV2(frame CommitFrameV2) ([]byte, error) {
	if err := frame.validate(); err != nil {
		return nil, err
	}
	data, err := frameCanonicalCBOR.Marshal(frame)
	if err != nil {
		return nil, fmt.Errorf("%w: encode commit: %v", ErrInvalidFrame, err)
	}
	return data, nil
}

// Identity returns the deterministic v2 ID derived from this frame.
func (f CommitFrameV2) Identity() (CommitIdentity, error) {
	data, err := CanonicalCommitFrameV2(f)
	if err != nil {
		return CommitIdentity{}, err
	}
	return V2CommitIdentity(ContentID(data)), nil
}

func (f CommitFrameV2) validate() error {
	if f.Version != CommitFormatV2 {
		return fmt.Errorf("%w: unsupported commit frame version %d", ErrInvalidFrame, f.Version)
	}
	if !validContentID(f.SnapshotRoot) {
		return fmt.Errorf("%w: snapshot root must be a BLAKE3-256 content ID", ErrInvalidFrame)
	}
	if f.Author == "" || f.Message == "" {
		return fmt.Errorf("%w: commit author and message are required", ErrInvalidFrame)
	}
	if len(f.Parents) > 2 {
		return fmt.Errorf("%w: commits may have at most two parents", ErrInvalidFrame)
	}
	for i, parent := range f.Parents {
		if err := parent.validate(); err != nil {
			return fmt.Errorf("%w: parent %d: %v", ErrInvalidFrame, i, err)
		}
	}
	return nil
}

// PackObjectV2 is one immutable CAS object carried by a new v2 pack.
type PackObjectV2 struct {
	ID   string `cbor:"1,keyasint"`
	Data []byte `cbor:"2,keyasint"`
}

// PackFrameV2 is the explicit Rack-owned v2 pack format. New packs carry
// canonical v2 commit preimages and CAS objects. A migration may additionally
// carry an opaque legacy pack verbatim; it is never decoded or reinterpreted.
type PackFrameV2 struct {
	Version       uint32          `cbor:"1,keyasint"`
	Base          CommitIdentity  `cbor:"2,keyasint"`
	Target        CommitIdentity  `cbor:"3,keyasint"`
	Commits       []CommitFrameV2 `cbor:"4,keyasint"`
	Objects       []PackObjectV2  `cbor:"5,keyasint"`
	LegacyPackID  string          `cbor:"6,keyasint,omitempty"`
	LegacyPayload []byte          `cbor:"7,keyasint"`
	// SupplementalCommits supply non-first-parent ancestry required by an
	// exported merge. They never advance Base to Target, so Commits remains
	// the contiguous pack range recorded in PostgreSQL.
	SupplementalCommits []CommitFrameV2 `cbor:"8,keyasint,omitempty"`
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
		if err := f.Base.validate(); err != nil {
			return fmt.Errorf("%w: base commit: %v", ErrInvalidFrame, err)
		}
	}
	if err := f.Target.validate(); err != nil {
		return fmt.Errorf("%w: target commit: %v", ErrInvalidFrame, err)
	}
	if len(f.Commits) == 0 {
		return fmt.Errorf("%w: advancing pack must contain a commit frame", ErrInvalidFrame)
	}
	previous := f.Base
	for i, commit := range f.Commits {
		if err := commit.validate(); err != nil {
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
			return fmt.Errorf("%w: commit %d does not continue the declared pack range", ErrInvalidFrame, i)
		}
		if commit.Parents[0] != previous {
			return fmt.Errorf("%w: commit %d does not continue the declared pack range", ErrInvalidFrame, i)
		}
		identity, err := commit.Identity()
		if err != nil {
			return err
		}
		previous = identity
	}
	if len(f.Commits) > 0 {
		if previous != f.Target {
			return fmt.Errorf("%w: target does not name the final commit frame", ErrInvalidFrame)
		}
	}
	seenCommits := make(map[CommitIdentity]struct{}, len(f.Commits)+len(f.SupplementalCommits))
	for _, commit := range f.Commits {
		identity, err := commit.Identity()
		if err != nil {
			return err
		}
		seenCommits[identity] = struct{}{}
	}
	for i, commit := range f.SupplementalCommits {
		if err := commit.validate(); err != nil {
			return fmt.Errorf("%w: supplemental commit %d: %v", ErrInvalidFrame, i, err)
		}
		identity, err := commit.Identity()
		if err != nil {
			return err
		}
		if _, duplicate := seenCommits[identity]; duplicate {
			return fmt.Errorf("%w: duplicate supplemental commit %s", ErrInvalidFrame, identity.ID)
		}
		seenCommits[identity] = struct{}{}
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
	if (f.LegacyPackID == "") != (f.LegacyPayload == nil) {
		return fmt.Errorf("%w: legacy pack ID and payload must be supplied together", ErrInvalidFrame)
	}
	if f.LegacyPackID != "" {
		if !validContentID(f.LegacyPackID) || ContentID(f.LegacyPayload) != f.LegacyPackID {
			return fmt.Errorf("%w: opaque legacy pack hash mismatch", ErrInvalidFrame)
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
