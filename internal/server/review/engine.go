package review

import (
	"context"
)

// MergePreview contains the simulated diff and any structural/schema/property conflicts.
type MergePreview struct {
	SourceBranch string   `json:"sourceBranch"`
	TargetBranch string   `json:"targetBranch"`
	BaseCommit   string   `json:"baseCommit"`
	HasConflicts bool     `json:"hasConflicts"`
	Conflicts    []string `json:"conflicts,omitempty"`
}

// Engine simulates three-way graph merges, evaluates conflicts, and executes merge commits.
type Engine interface {
	PreviewMerge(ctx context.Context, tenantID, repoID, sourceBranch, targetBranch string) (*MergePreview, error)
	ApplyMerge(ctx context.Context, tenantID, repoID, sourceBranch, targetBranch, leaseToken string) (string, error)
}
