package sync

import (
	"context"
	"io"
)

// PushRequest contains parameters for an incoming branch push.
type PushRequest struct {
	TenantID     string
	RepoID       string
	Branch       string
	TargetCommit string
	BaseCommit   string
	PackStream   io.Reader
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
	HandlePull(ctx context.Context, req PullRequest, w io.Writer) error
}
