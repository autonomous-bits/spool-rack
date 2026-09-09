package sync

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
)

func TestPullEnvelopeV2SeparatesPacksAndRejectsTampering(t *testing.T) {
	t.Parallel()

	newPack := func(label string) []byte {
		t.Helper()
		snapshot := []byte(label + " snapshot")
		commit := CommitFrameV2{
			Version: CommitFormatV2, Parents: []CommitIdentity{},
			SnapshotRoot: ContentID(snapshot), Author: "Ada", Message: label,
		}
		target, err := commit.Identity()
		if err != nil {
			t.Fatal(err)
		}
		pack, err := MarshalPackFrameV2(PackFrameV2{
			Version: PackFormatV2, Target: target, Commits: []CommitFrameV2{commit},
			Objects: []PackObjectV2{{ID: ContentID(snapshot), Data: snapshot}},
		})
		if err != nil {
			t.Fatal(err)
		}
		return pack
	}
	packs := [][]byte{newPack("root"), newPack("head")}
	headFrame, err := UnmarshalPackFrameV2(packs[len(packs)-1])
	if err != nil {
		t.Fatal(err)
	}
	manifest := PullManifestV2{
		Version: PullEnvelopeFormatV2,
		Head:    headFrame.Target.ID,
		Packs: []PullPackManifestV2{
			{Hash: ContentID(packs[0]), Format: PackFormatV2, Length: uint64(len(packs[0]))},
			{Hash: ContentID(packs[1]), Format: PackFormatV2, Length: uint64(len(packs[1]))},
		},
	}

	manifestData, err := MarshalPullManifestV2(manifest)
	if err != nil {
		t.Fatalf("MarshalPullManifestV2() error = %v", err)
	}
	envelope := make([]byte, pullEnvelopeHeaderSize, pullEnvelopeHeaderSize+len(manifestData)+len(packs[0])+len(packs[1]))
	copy(envelope[:4], pullEnvelopeMagic)
	binary.BigEndian.PutUint32(envelope[4:8], PullEnvelopeFormatV2)
	binary.BigEndian.PutUint64(envelope[8:], uint64(len(manifestData)))
	envelope = append(envelope, manifestData...)
	envelope = append(envelope, packs[0]...)
	envelope = append(envelope, packs[1]...)

	gotManifest, gotPacks, err := UnmarshalPullEnvelopeV2(envelope)
	if err != nil {
		t.Fatalf("UnmarshalPullEnvelopeV2() error = %v", err)
	}
	if !reflect.DeepEqual(gotManifest, manifest) {
		t.Fatalf("manifest = %+v, want %+v", gotManifest, manifest)
	}
	for i := range packs {
		if !bytes.Equal(gotPacks[i], packs[i]) {
			t.Fatalf("pack %d = %q, want %q", i, gotPacks[i], packs[i])
		}
	}

	tampered := append([]byte(nil), envelope...)
	tampered[len(tampered)-1] ^= 0xff
	if _, _, err := UnmarshalPullEnvelopeV2(tampered); !errors.Is(err, ErrInvalidPullEnvelope) {
		t.Fatalf("UnmarshalPullEnvelopeV2(tampered) error = %v, want invalid pull envelope", err)
	}
}

func TestPullEnvelopeV2RequiresEveryMergeParent(t *testing.T) {
	t.Parallel()

	target := V2CommitIdentity(ContentID([]byte("target commit")))
	source := V2CommitIdentity(ContentID([]byte("source commit")))
	snapshot := []byte("merge snapshot")
	merge := CommitFrameV2{
		Version:      CommitFormatV2,
		Parents:      []CommitIdentity{target, source},
		SnapshotRoot: ContentID(snapshot),
		Author:       "Ada",
		Message:      "merge",
	}
	mergeIdentity, err := merge.Identity()
	if err != nil {
		t.Fatal(err)
	}
	pack, err := MarshalPackFrameV2(PackFrameV2{
		Version: PackFormatV2,
		Base:    target,
		Target:  mergeIdentity,
		Commits: []CommitFrameV2{merge},
		Objects: []PackObjectV2{{ID: ContentID(snapshot), Data: snapshot}},
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest := PullManifestV2{
		Version: PullEnvelopeFormatV2,
		Head:    mergeIdentity.ID,
		Packs: []PullPackManifestV2{{
			Hash: ContentID(pack), Format: PackFormatV2, Length: uint64(len(pack)),
		}},
	}
	manifestData, err := MarshalPullManifestV2(manifest)
	if err != nil {
		t.Fatal(err)
	}
	envelope := make([]byte, pullEnvelopeHeaderSize, pullEnvelopeHeaderSize+len(manifestData)+len(pack))
	copy(envelope[:4], pullEnvelopeMagic)
	binary.BigEndian.PutUint32(envelope[4:8], PullEnvelopeFormatV2)
	binary.BigEndian.PutUint64(envelope[8:], uint64(len(manifestData)))
	envelope = append(envelope, manifestData...)
	envelope = append(envelope, pack...)

	if _, _, err := UnmarshalPullEnvelopeV2(envelope); !errors.Is(err, ErrInvalidPullEnvelope) {
		t.Fatalf("UnmarshalPullEnvelopeV2() error = %v, want missing merge parent error", err)
	}
}

func TestPullEnvelopeV2AllowsMultipleBoundedPacksBeyondOnePackLimit(t *testing.T) {
	t.Parallel()

	newBoundedPack := func(label string) []byte {
		t.Helper()
		snapshot := bytes.Repeat([]byte(label), int(MaxV2PackBytes/2/int64(len(label))))
		commit := CommitFrameV2{
			Version: CommitFormatV2, Parents: []CommitIdentity{},
			SnapshotRoot: ContentID(snapshot), Author: "Ada", Message: label,
		}
		target, err := commit.Identity()
		if err != nil {
			t.Fatal(err)
		}
		pack, err := MarshalPackFrameV2(PackFrameV2{
			Version: PackFormatV2, Target: target, Commits: []CommitFrameV2{commit},
			Objects: []PackObjectV2{{ID: ContentID(snapshot), Data: snapshot}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(pack)) > MaxV2PackBytes {
			t.Fatalf("pack length = %d, exceeds per-pack limit %d", len(pack), MaxV2PackBytes)
		}
		return pack
	}
	packs := [][]byte{newBoundedPack("a"), newBoundedPack("b")}
	if len(packs[0])+len(packs[1]) <= int(MaxV2PackBytes) {
		t.Fatal("test packs must exceed the per-pack limit in aggregate")
	}
	headFrame, err := UnmarshalPackFrameV2(packs[len(packs)-1])
	if err != nil {
		t.Fatal(err)
	}
	manifest := PullManifestV2{
		Version: PullEnvelopeFormatV2, Head: headFrame.Target.ID,
		Packs: []PullPackManifestV2{
			{Hash: ContentID(packs[0]), Format: PackFormatV2, Length: uint64(len(packs[0]))},
			{Hash: ContentID(packs[1]), Format: PackFormatV2, Length: uint64(len(packs[1]))},
		},
	}
	manifestData, err := MarshalPullManifestV2(manifest)
	if err != nil {
		t.Fatal(err)
	}
	envelope := make([]byte, pullEnvelopeHeaderSize, pullEnvelopeHeaderSize+len(manifestData)+len(packs[0])+len(packs[1]))
	copy(envelope[:4], pullEnvelopeMagic)
	binary.BigEndian.PutUint32(envelope[4:8], PullEnvelopeFormatV2)
	binary.BigEndian.PutUint64(envelope[8:], uint64(len(manifestData)))
	envelope = append(envelope, manifestData...)
	for _, pack := range packs {
		envelope = append(envelope, pack...)
	}
	if _, got, err := UnmarshalPullEnvelopeV2(envelope); err != nil || len(got) != len(packs) {
		t.Fatalf("UnmarshalPullEnvelopeV2(multi-pack) = %d packs, %v; want %d packs and nil", len(got), err, len(packs))
	}
}

func TestPullEnvelopeV2RejectsUnboundAdvertisedHead(t *testing.T) {
	t.Parallel()

	snapshot := []byte("snapshot")
	commit := CommitFrameV2{
		Version: CommitFormatV2, SnapshotRoot: ContentID(snapshot), Author: "Ada", Message: "commit",
	}
	target, err := commit.Identity()
	if err != nil {
		t.Fatal(err)
	}
	pack, err := MarshalPackFrameV2(PackFrameV2{
		Version: PackFormatV2, Target: target, Commits: []CommitFrameV2{commit},
		Objects: []PackObjectV2{{ID: ContentID(snapshot), Data: snapshot}},
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest := PullManifestV2{
		Version: PullEnvelopeFormatV2,
		Head:    ContentID([]byte("absent head")),
		Packs: []PullPackManifestV2{{
			Hash: ContentID(pack), Format: PackFormatV2, Length: uint64(len(pack)),
		}},
	}
	envelope := marshalPullEnvelope(t, manifest, pack)
	if _, _, err := UnmarshalPullEnvelopeV2(envelope); !errors.Is(err, ErrInvalidPullEnvelope) {
		t.Fatalf("UnmarshalPullEnvelopeV2() error = %v, want unbound head error", err)
	}

	manifest.Head = "not-a-v2-content-id"
	if _, err := MarshalPullManifestV2(manifest); !errors.Is(err, ErrInvalidPullEnvelope) {
		t.Fatalf("MarshalPullManifestV2() error = %v, want invalid head error", err)
	}
}

func TestPullEnvelopeV2RejectsDuplicateAdvertisedHead(t *testing.T) {
	t.Parallel()

	snapshot := []byte("snapshot")
	commit := CommitFrameV2{
		Version: CommitFormatV2, SnapshotRoot: ContentID(snapshot), Author: "Ada", Message: "commit",
	}
	target, err := commit.Identity()
	if err != nil {
		t.Fatal(err)
	}
	pack, err := MarshalPackFrameV2(PackFrameV2{
		Version: PackFormatV2, Target: target, Commits: []CommitFrameV2{commit},
		Objects: []PackObjectV2{{ID: ContentID(snapshot), Data: snapshot}},
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest := PullManifestV2{
		Version: PullEnvelopeFormatV2,
		Head:    target.ID,
		Packs: []PullPackManifestV2{
			{Hash: ContentID(pack), Format: PackFormatV2, Length: uint64(len(pack))},
			{Hash: ContentID(pack), Format: PackFormatV2, Length: uint64(len(pack))},
		},
	}
	envelope := marshalPullEnvelope(t, manifest, pack, pack)
	if _, _, err := UnmarshalPullEnvelopeV2(envelope); !errors.Is(err, ErrInvalidPullEnvelope) {
		t.Fatalf("UnmarshalPullEnvelopeV2() error = %v, want duplicate head error", err)
	}
}

func TestPullEnvelopeV2RejectsEarlierAdvertisedHead(t *testing.T) {
	t.Parallel()

	newPack := func(label string) ([]byte, CommitIdentity) {
		t.Helper()
		snapshot := []byte(label + " snapshot")
		commit := CommitFrameV2{
			Version: CommitFormatV2, SnapshotRoot: ContentID(snapshot), Author: "Ada", Message: label,
		}
		target, err := commit.Identity()
		if err != nil {
			t.Fatal(err)
		}
		pack, err := MarshalPackFrameV2(PackFrameV2{
			Version: PackFormatV2, Target: target, Commits: []CommitFrameV2{commit},
			Objects: []PackObjectV2{{ID: ContentID(snapshot), Data: snapshot}},
		})
		if err != nil {
			t.Fatal(err)
		}
		return pack, target
	}
	earlierPack, earlierTarget := newPack("earlier")
	terminalPack, _ := newPack("terminal")
	manifest := PullManifestV2{
		Version: PullEnvelopeFormatV2,
		Head:    earlierTarget.ID,
		Packs: []PullPackManifestV2{
			{Hash: ContentID(earlierPack), Format: PackFormatV2, Length: uint64(len(earlierPack))},
			{Hash: ContentID(terminalPack), Format: PackFormatV2, Length: uint64(len(terminalPack))},
		},
	}
	envelope := marshalPullEnvelope(t, manifest, earlierPack, terminalPack)
	if _, _, err := UnmarshalPullEnvelopeV2(envelope); !errors.Is(err, ErrInvalidPullEnvelope) {
		t.Fatalf("UnmarshalPullEnvelopeV2() error = %v, want nonterminal head error", err)
	}
}

func marshalPullEnvelope(t *testing.T, manifest PullManifestV2, packs ...[]byte) []byte {
	t.Helper()

	manifestData, err := MarshalPullManifestV2(manifest)
	if err != nil {
		t.Fatal(err)
	}
	envelope := make([]byte, pullEnvelopeHeaderSize, pullEnvelopeHeaderSize+len(manifestData))
	copy(envelope[:4], pullEnvelopeMagic)
	binary.BigEndian.PutUint32(envelope[4:8], PullEnvelopeFormatV2)
	binary.BigEndian.PutUint64(envelope[8:], uint64(len(manifestData)))
	envelope = append(envelope, manifestData...)
	for _, pack := range packs {
		envelope = append(envelope, pack...)
	}
	return envelope
}

func TestPullEnvelopeV2AcceptsPackFormatV3(t *testing.T) {
	t.Parallel()

	snapshot := []byte("v3 snapshot")
	commit := CommitFrameV2{
		Version: CommitFormatV2, Parents: []CommitIdentity{},
		SnapshotRoot: ContentID(snapshot), Author: "Ada", Message: "v3 commit",
	}
	target, err := commit.Identity()
	if err != nil {
		t.Fatal(err)
	}
	pack, err := MarshalPackFrameV3(PackFrameV3{
		Version: PackFormatV3, Target: target, Commits: []CommitFrameV2{commit},
		Objects: []PackObjectV2{{ID: ContentID(snapshot), Data: snapshot}},
	})
	if err != nil {
		t.Fatal(err)
	}

	manifest := PullManifestV2{
		Version: PullEnvelopeFormatV2,
		Head:    target.ID,
		Packs: []PullPackManifestV2{
			{Hash: ContentID(pack), Format: PackFormatV3, Length: uint64(len(pack))},
		},
	}
	envelope := marshalPullEnvelope(t, manifest, pack)
	gotManifest, gotPacks, err := UnmarshalPullEnvelopeV2(envelope)
	if err != nil {
		t.Fatalf("UnmarshalPullEnvelopeV2() error = %v", err)
	}
	if !reflect.DeepEqual(gotManifest, manifest) {
		t.Fatalf("got manifest %+v, want %+v", gotManifest, manifest)
	}
	if len(gotPacks) != 1 || !bytes.Equal(gotPacks[0], pack) {
		t.Fatalf("unexpected extracted pack data")
	}
}

func TestPullManifestMatchesPlanSupportsV3(t *testing.T) {
	t.Parallel()

	packHash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	headID := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	plan := PullPlan{
		Head: headID,
		Ranges: []postgres.PackRange{
			{PackHash: packHash, Format: PackFormatV3, TargetCommitID: headID},
		},
	}
	manifest := PullManifestV2{
		Version: PullEnvelopeFormatV2,
		Head:    headID,
		Packs: []PullPackManifestV2{
			{Hash: packHash, Format: PackFormatV3, Length: 100},
		},
	}
	if err := pullManifestMatchesPlan(manifest, plan); err != nil {
		t.Fatalf("pullManifestMatchesPlan() error = %v", err)
	}
}
