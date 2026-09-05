package sync

import (
	"context"
	"io"
)

// CommitRecord describes one commit to register in the remote metadata store
// as part of a push, keyed by its BLAKE3 content-addressed ID.
type CommitRecord struct {
	ID           string `json:"id"`
	ParentID     string `json:"parentId,omitempty"`
	SnapshotRoot string `json:"snapshotRoot"`
	Author       string `json:"author"`
	Message      string `json:"message"`
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
	// record's parent is already registered by the time its row is inserted.
	Commits []CommitRecord
	// PackHash is the content-addressed hash of the raw packfile bytes written
	// to CAS. It names and verifies the pack payload itself, unlike
	// TargetCommit/BaseCommit, which refer to existing commits.id rows in
	// PostgreSQL metadata.
	PackHash   string
	PackStream io.Reader
}

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
