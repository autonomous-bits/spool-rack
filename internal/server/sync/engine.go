package sync

import (
	"context"
	"io"

	"github.com/autonomous-bits/spool/graphcontract"
)

// CommitRecord describes one commit to register in the remote metadata store
// as part of a push, keyed by its BLAKE3 content-addressed ID.
type CommitRecord struct {
	ID     graphcontract.ObjectID `json:"id"`
	Commit graphcontract.Commit   `json:"commit"`
}

// PushRequest contains parameters for an incoming branch push.
type PushRequest struct {
	TenantID     string
	RepoID       string
	Branch       string
	TargetCommit string
	BaseCommit   string
	// Commits are the commit rows to register in PostgreSQL before advancing
	// the branch ref. Callers should supply them oldest-ancestor-first so each
	// record's parents are already registered by the time its row is inserted.
	Commits []CommitRecord
	// PackHash is the content-addressed hash of the raw packfile bytes written
	// to CAS. It names and verifies the pack payload itself, unlike
	// TargetCommit/BaseCommit, which refer to existing commits.id rows in
	// PostgreSQL metadata.
	PackHash string
	// PackFormat defaults to canonical v2 framing so the JSON control plane
	// cannot create opaque legacy history.
	PackFormat uint32
	PackStream io.Reader
}

// SnapshotValidator verifies a v2 snapshot object before its referencing
// commit becomes visible in metadata. Rack supplies its canonical CBOR decoder.
type SnapshotValidator func([]byte) error

// PullRequest contains parameters for a branch pull request.
type PullRequest struct {
	TenantID    string
	RepoID      string
	Branch      string
	KnownCommit string
}

// Engine coordinates push and pull operations between local clients and remote storage.
type Engine interface {
	HandlePush(ctx context.Context, req PushRequest) error
	HandlePull(ctx context.Context, req PullRequest, w io.Writer) (string, error)
}
