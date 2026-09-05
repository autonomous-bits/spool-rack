package review

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	serversync "github.com/autonomous-bits/spool-rack/internal/server/sync"
)

const defaultMergeLeaseDuration = 5 * time.Minute

var (
	ErrInvalidMergeRequest = errors.New("review: invalid merge request")
	ErrMergeUnavailable    = errors.New("review: merge finalization dependencies are unavailable")
)

// Resolution selects the source or target value for one preview conflict, or
// supplies a typed JSON replacement for a manual selection.
type Resolution struct {
	Token  string          `json:"token"`
	Choice string          `json:"choice"`
	Value  json.RawMessage `json:"value,omitempty"`
}

// LeaseRequest identifies the immutable preview whose target branch is leased.
type LeaseRequest struct {
	SourceBranch string
	TargetBranch string
	Subject      string
}

// ApplyRequest finalizes a preview while holding an owned resolution lease.
type ApplyRequest struct {
	SourceBranch string
	TargetBranch string
	Subject      string
	LeaseToken   string
	Resolutions  []Resolution
	Author       string
	Message      string
}

// ApplyResult describes the advanced branch and, for a three-way merge, its
// synthesized immutable commit.
type ApplyResult struct {
	Branch       string `json:"branch"`
	HeadCommit   string `json:"headCommit"`
	FastForward  bool   `json:"fastForward"`
	SnapshotRoot string `json:"snapshotRoot,omitempty"`
	PackHash     string `json:"packHash,omitempty"`
}

// Finalizer is the write-side merge interface used by HTTP handlers.
type Finalizer interface {
	AcquireLease(context.Context, string, string, LeaseRequest) (postgres.MergeLease, error)
	ReleaseLease(context.Context, string, string, string, string, string) error
	Apply(context.Context, string, string, ApplyRequest) (ApplyResult, error)
}

// MergeStore contains the metadata capabilities required by write-side merge
// finalization. PostgreSQL owns the transactional lease/ref mutation.
type MergeStore interface {
	MetadataStore
	AcquireMergeLease(context.Context, postgres.MergeLeaseRequest) (postgres.MergeLease, error)
	ValidateMergeLease(context.Context, string, string, string, string) (postgres.MergeLease, error)
	ReleaseMergeLease(context.Context, string, string, string, string) error
	ApplyMerge(context.Context, postgres.ApplyMergeRequest) error
	CompareAndSwapBranchRef(context.Context, string, string, string, string) error
}

var _ MergeStore = (postgres.Store)(nil)

// FinalizeEngine is the write-capable merge service. Immutable CAS writes
// happen before its sole metadata mutation call.
type FinalizeEngine struct {
	objects cas.Driver
	store   MergeStore
	now     func() time.Time
}

var _ Finalizer = (*FinalizeEngine)(nil)

func NewFinalizeEngine(objects cas.Driver, store MergeStore) *FinalizeEngine {
	return &FinalizeEngine{objects: objects, store: store, now: time.Now}
}

func (e *FinalizeEngine) AcquireLease(ctx context.Context, tenantID, repoID string, request LeaseRequest) (postgres.MergeLease, error) {
	if e == nil || e.objects == nil || e.store == nil {
		return postgres.MergeLease{}, ErrMergeUnavailable
	}
	if ctx == nil || tenantID == "" || repoID == "" || request.SourceBranch == "" || request.TargetBranch == "" || request.Subject == "" {
		return postgres.MergeLease{}, fmt.Errorf("%w: tenant, repository, branches, and subject are required", ErrInvalidMergeRequest)
	}
	preview, err := e.preview(ctx, tenantID, repoID, request.SourceBranch, request.TargetBranch)
	if err != nil {
		return postgres.MergeLease{}, err
	}
	if preview.CanFastForward {
		return postgres.MergeLease{}, fmt.Errorf("%w: fast-forward merges do not require a lease", ErrInvalidMergeRequest)
	}
	tenantCtx, err := e.store.SetTenantContext(ctx, tenantID)
	if err != nil {
		return postgres.MergeLease{}, fmt.Errorf("review: set tenant context: %w", err)
	}
	lease, err := e.store.AcquireMergeLease(tenantCtx, postgres.MergeLeaseRequest{
		RepoID: repoID, TargetBranch: request.TargetBranch, Subject: request.Subject,
		SourceCommitID: preview.SourceCommit.ID, TargetCommitID: preview.TargetCommit.ID,
		BaseCommitID: preview.BaseCommit.ID, Duration: defaultMergeLeaseDuration,
	})
	if err != nil {
		return postgres.MergeLease{}, fmt.Errorf("review: acquire merge lease: %w", err)
	}
	return lease, nil
}

func (e *FinalizeEngine) ReleaseLease(ctx context.Context, tenantID, repoID, targetBranch, subject, token string) error {
	if e == nil || e.store == nil {
		return ErrMergeUnavailable
	}
	if ctx == nil || tenantID == "" || repoID == "" || targetBranch == "" || subject == "" || token == "" {
		return fmt.Errorf("%w: tenant, repository, target branch, subject, and lease token are required", ErrInvalidMergeRequest)
	}
	tenantCtx, err := e.store.SetTenantContext(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("review: set tenant context: %w", err)
	}
	if err := e.store.ReleaseMergeLease(tenantCtx, repoID, targetBranch, subject, token); err != nil {
		return fmt.Errorf("review: release merge lease: %w", err)
	}
	return nil
}

func (e *FinalizeEngine) Apply(ctx context.Context, tenantID, repoID string, request ApplyRequest) (ApplyResult, error) {
	if e == nil || e.objects == nil || e.store == nil {
		return ApplyResult{}, ErrMergeUnavailable
	}
	if ctx == nil || tenantID == "" || repoID == "" || request.SourceBranch == "" || request.TargetBranch == "" ||
		request.Subject == "" || request.Author == "" || request.Message == "" {
		return ApplyResult{}, fmt.Errorf("%w: tenant, repository, branches, subject, author, and message are required", ErrInvalidMergeRequest)
	}
	preview, err := e.preview(ctx, tenantID, repoID, request.SourceBranch, request.TargetBranch)
	if err != nil {
		return ApplyResult{}, err
	}
	tenantCtx, err := e.store.SetTenantContext(ctx, tenantID)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("review: set tenant context: %w", err)
	}
	if preview.CanFastForward {
		if len(request.Resolutions) != 0 || request.LeaseToken != "" {
			return ApplyResult{}, fmt.Errorf("%w: fast-forward apply must not include a lease or resolutions", ErrInvalidMergeRequest)
		}
		if err := e.store.CompareAndSwapBranchRef(tenantCtx, repoID, request.TargetBranch, preview.TargetCommit.ID, preview.SourceCommit.ID); err != nil {
			return ApplyResult{}, fmt.Errorf("review: fast-forward target branch: %w", err)
		}
		return ApplyResult{Branch: request.TargetBranch, HeadCommit: preview.SourceCommit.ID, FastForward: true}, nil
	}
	if request.LeaseToken == "" {
		return ApplyResult{}, fmt.Errorf("%w: leaseToken is required for a three-way merge", ErrInvalidMergeRequest)
	}
	lease, err := e.store.ValidateMergeLease(tenantCtx, repoID, request.TargetBranch, request.Subject, request.LeaseToken)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("review: validate merge lease: %w", err)
	}
	if lease.SourceCommitID != preview.SourceCommit.ID || lease.TargetCommitID != preview.TargetCommit.ID || lease.BaseCommitID != preview.BaseCommit.ID {
		return ApplyResult{}, fmt.Errorf("%w: lease does not match current merge preview", postgres.ErrMergeLeaseMismatch)
	}
	if err := validateResolutions(preview, request.Resolutions); err != nil {
		return ApplyResult{}, err
	}
	scope, err := cas.NewScope(tenantID, repoID)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("review: create CAS scope: %w", err)
	}
	base, source, target, err := e.loadSnapshots(tenantCtx, scope, preview)
	if err != nil {
		return ApplyResult{}, err
	}
	merged, err := materializeMerge(base, source, target, preview, request.Resolutions)
	if err != nil {
		return ApplyResult{}, err
	}
	snapshotData, err := MarshalSnapshot(merged)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("review: encode merged snapshot: %w", err)
	}
	snapshotRoot := serversync.ContentID(snapshotData)
	targetIdentity, err := frameCommitIdentity(preview.TargetCommit)
	if err != nil {
		return ApplyResult{}, err
	}
	sourceIdentity, err := frameCommitIdentity(preview.SourceCommit)
	if err != nil {
		return ApplyResult{}, err
	}
	commitFrame := serversync.CommitFrameV2{
		Version:      serversync.CommitFormatV2,
		Parents:      []serversync.CommitIdentity{targetIdentity, sourceIdentity},
		SnapshotRoot: snapshotRoot,
		Author:       request.Author,
		Message:      request.Message,
	}
	commitIdentity, err := commitFrame.Identity()
	if err != nil {
		return ApplyResult{}, fmt.Errorf("review: frame merged commit: %w", err)
	}
	packData, err := serversync.MarshalPackFrameV2(serversync.PackFrameV2{
		Version: serversync.PackFormatV2,
		Base:    targetIdentity,
		Target:  commitIdentity,
		Commits: []serversync.CommitFrameV2{commitFrame},
		Objects: []serversync.PackObjectV2{{ID: snapshotRoot, Data: snapshotData}},
	})
	if err != nil {
		return ApplyResult{}, fmt.Errorf("review: frame merged pack: %w", err)
	}
	if err := e.objects.Put(tenantCtx, scope, snapshotRoot, snapshotData); err != nil {
		return ApplyResult{}, fmt.Errorf("review: persist merged snapshot: %w", err)
	}
	packHash := serversync.ContentID(packData)
	if err := e.objects.WritePack(tenantCtx, scope, packHash, bytes.NewReader(packData)); err != nil {
		return ApplyResult{}, fmt.Errorf("review: persist merged pack: %w", err)
	}
	if err := e.store.ApplyMerge(tenantCtx, postgres.ApplyMergeRequest{
		RepoID: repoID, TargetBranch: request.TargetBranch, Subject: request.Subject, LeaseToken: request.LeaseToken,
		SourceCommitID: preview.SourceCommit.ID, TargetCommitID: preview.TargetCommit.ID, BaseCommitID: preview.BaseCommit.ID,
		ResultCommitID: commitIdentity.ID, SnapshotRoot: snapshotRoot, Author: request.Author, Message: request.Message, PackHash: packHash,
		CommitFormat: serversync.CommitFormatV2, PackFormat: serversync.PackFormatV2,
	}); err != nil {
		return ApplyResult{}, fmt.Errorf("review: apply merge: %w", err)
	}
	return ApplyResult{Branch: request.TargetBranch, HeadCommit: commitIdentity.ID, SnapshotRoot: snapshotRoot, PackHash: packHash}, nil
}

func frameCommitIdentity(commit CommitIdentity) (serversync.CommitIdentity, error) {
	switch commit.Format {
	case 0, serversync.CommitFormatLegacy:
		if commit.ID == "" {
			return serversync.CommitIdentity{}, fmt.Errorf("%w: missing legacy commit ID", ErrInvalidMergeRequest)
		}
		return serversync.LegacyCommitIdentity(commit.ID), nil
	case serversync.CommitFormatV2:
		identity := serversync.V2CommitIdentity(commit.ID)
		if err := identity.Validate(); err != nil {
			return serversync.CommitIdentity{}, fmt.Errorf("%w: invalid v2 commit identity: %v", ErrInvalidMergeRequest, err)
		}
		return identity, nil
	default:
		return serversync.CommitIdentity{}, fmt.Errorf("%w: unsupported commit format %d", ErrInvalidMergeRequest, commit.Format)
	}
}

func (e *FinalizeEngine) preview(ctx context.Context, tenantID, repoID, sourceBranch, targetBranch string) (*MergePreview, error) {
	return NewPreviewEngine(e.objects, e.store).PreviewMerge(ctx, tenantID, repoID, sourceBranch, targetBranch)
}

func (e *FinalizeEngine) loadSnapshots(ctx context.Context, scope cas.Scope, preview *MergePreview) (Snapshot, Snapshot, Snapshot, error) {
	base, err := DecodeSnapshot(ctx, e.objects, scope, preview.BaseCommit.SnapshotRoot)
	if err != nil {
		return Snapshot{}, Snapshot{}, Snapshot{}, fmt.Errorf("review: load base snapshot: %w", err)
	}
	source, err := DecodeSnapshot(ctx, e.objects, scope, preview.SourceCommit.SnapshotRoot)
	if err != nil {
		return Snapshot{}, Snapshot{}, Snapshot{}, fmt.Errorf("review: load source snapshot: %w", err)
	}
	target, err := DecodeSnapshot(ctx, e.objects, scope, preview.TargetCommit.SnapshotRoot)
	if err != nil {
		return Snapshot{}, Snapshot{}, Snapshot{}, fmt.Errorf("review: load target snapshot: %w", err)
	}
	return base, source, target, nil
}

func validateResolutions(preview *MergePreview, resolutions []Resolution) error {
	required := make(map[string]ConflictToken, len(preview.Conflicts))
	for _, conflict := range preview.Conflicts {
		required[conflict.Token] = conflict
	}
	if len(resolutions) != len(required) {
		return fmt.Errorf("%w: every preview conflict requires exactly one resolution", ErrInvalidMergeRequest)
	}
	for _, resolution := range resolutions {
		conflict, ok := required[resolution.Token]
		if !ok {
			return fmt.Errorf("%w: unknown or duplicate resolution token %q", ErrInvalidMergeRequest, resolution.Token)
		}
		delete(required, resolution.Token)
		if resolution.Choice != "source" && resolution.Choice != "target" && resolution.Choice != "manual" {
			return fmt.Errorf("%w: resolution %q has an invalid choice", ErrInvalidMergeRequest, resolution.Token)
		}
		if resolution.Choice == "manual" && len(resolution.Value) == 0 {
			return fmt.Errorf("%w: manual resolution %q requires a value", ErrInvalidMergeRequest, resolution.Token)
		}
		if resolution.Choice != "manual" && len(resolution.Value) != 0 {
			return fmt.Errorf("%w: non-manual resolution %q must not include a value", ErrInvalidMergeRequest, resolution.Token)
		}
		_ = conflict
	}
	if len(required) != 0 {
		return fmt.Errorf("%w: missing conflict resolutions", ErrInvalidMergeRequest)
	}
	return nil
}

func materializeMerge(base, source, target Snapshot, preview *MergePreview, resolutions []Resolution) (Snapshot, error) {
	if len(preview.Conflicts) == 1 && preview.Conflicts[0].Type == ConflictCardinality && len(resolutions) != 0 {
		resolution := resolutions[0]
		if resolution.Choice == "source" {
			return source, nil
		}
		if resolution.Choice == "target" {
			return target, nil
		}
		var snapshot Snapshot
		if err := decodeManual(resolution.Value, &snapshot); err != nil {
			return Snapshot{}, err
		}
		return snapshot, snapshot.Validate()
	}
	result := mergeSnapshots(base, source, target)
	merged := Snapshot{Version: SnapshotVersion, Schema: target.Schema, Nodes: mergeElements(base.Nodes, source.Nodes, target.Nodes, ElementNode, &MergePreview{}), Edges: mergeElements(base.Edges, source.Edges, target.Edges, ElementEdge, &MergePreview{})}
	merged.Schema, _, _ = mergeValue(base.Schema, source.Schema, target.Schema)
	_ = result
	orderedResolutions := append([]Resolution(nil), resolutions...)
	sort.SliceStable(orderedResolutions, func(i, j int) bool {
		left, right := conflictByToken(preview, orderedResolutions[i].Token), conflictByToken(preview, orderedResolutions[j].Token)
		leftRank, rightRank := resolutionRank(left), resolutionRank(right)
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		return left.Token < right.Token
	})
	for _, resolution := range orderedResolutions {
		conflict := conflictByToken(preview, resolution.Token)
		if err := applyResolution(&merged, source, target, conflict, resolution); err != nil {
			return Snapshot{}, err
		}
	}
	if err := merged.Validate(); err != nil {
		return Snapshot{}, fmt.Errorf("%w: resolved merge is invalid: %v", ErrInvalidMergeRequest, err)
	}
	return merged, nil
}

func resolutionRank(conflict ConflictToken) int {
	if conflict.Type == ConflictProperty {
		return 1
	}
	return 0
}

func conflictByToken(preview *MergePreview, token string) ConflictToken {
	for _, conflict := range preview.Conflicts {
		if conflict.Token == token {
			return conflict
		}
	}
	return ConflictToken{}
}

func applyResolution(merged *Snapshot, source, target Snapshot, conflict ConflictToken, resolution Resolution) error {
	if conflict.Type == ConflictSchema {
		var value Schema
		switch resolution.Choice {
		case "source":
			value = source.Schema
		case "target":
			value = target.Schema
		default:
			if err := decodeManual(resolution.Value, &value); err != nil {
				return err
			}
		}
		merged.Schema = value
		return nil
	}
	if conflict.Type == ConflictProperty {
		value, exists, err := resolutionProperty(source, target, conflict, resolution)
		if err != nil {
			return err
		}
		return setProperty(merged, conflict.Element, conflict.ElementID, conflict.Property, value, exists)
	}
	if conflict.Element == ElementNode {
		value, exists, err := resolutionNode(source, target, conflict.ElementID, resolution)
		if err != nil {
			return err
		}
		merged.Nodes = setNode(merged.Nodes, conflict.ElementID, value, exists)
		return nil
	}
	value, exists, err := resolutionEdge(source, target, conflict.ElementID, resolution)
	if err != nil {
		return err
	}
	merged.Edges = setEdge(merged.Edges, conflict.ElementID, value, exists)
	return nil
}

func resolutionProperty(source, target Snapshot, conflict ConflictToken, resolution Resolution) (Property, bool, error) {
	if resolution.Choice == "manual" {
		var value Property
		if err := decodeManual(resolution.Value, &value); err != nil {
			return Property{}, false, err
		}
		if value.Name != conflict.Property {
			return Property{}, false, fmt.Errorf("%w: manual property name does not match conflict", ErrInvalidMergeRequest)
		}
		return value, true, nil
	}
	snapshot := source
	if resolution.Choice == "target" {
		snapshot = target
	}
	properties, ok := propertiesFor(snapshot, conflict.Element, conflict.ElementID)
	if !ok {
		return Property{}, false, nil
	}
	value, ok := propertiesByName(properties)[conflict.Property]
	return value, ok, nil
}

func resolutionNode(source, target Snapshot, id string, resolution Resolution) (Node, bool, error) {
	if resolution.Choice == "manual" {
		if bytes.Equal(resolution.Value, []byte("null")) {
			return Node{}, false, nil
		}
		var value Node
		if err := decodeManual(resolution.Value, &value); err != nil {
			return Node{}, false, err
		}
		if value.ID != id {
			return Node{}, false, fmt.Errorf("%w: manual node ID does not match conflict", ErrInvalidMergeRequest)
		}
		return value, true, nil
	}
	snapshot := source
	if resolution.Choice == "target" {
		snapshot = target
	}
	for _, node := range snapshot.Nodes {
		if node.ID == id {
			return node, true, nil
		}
	}
	return Node{}, false, nil
}

func resolutionEdge(source, target Snapshot, id string, resolution Resolution) (Edge, bool, error) {
	if resolution.Choice == "manual" {
		if bytes.Equal(resolution.Value, []byte("null")) {
			return Edge{}, false, nil
		}
		var value Edge
		if err := decodeManual(resolution.Value, &value); err != nil {
			return Edge{}, false, err
		}
		if value.ID != id {
			return Edge{}, false, fmt.Errorf("%w: manual edge ID does not match conflict", ErrInvalidMergeRequest)
		}
		return value, true, nil
	}
	snapshot := source
	if resolution.Choice == "target" {
		snapshot = target
	}
	for _, edge := range snapshot.Edges {
		if edge.ID == id {
			return edge, true, nil
		}
	}
	return Edge{}, false, nil
}

func decodeManual(data json.RawMessage, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("%w: invalid manual value: %v", ErrInvalidMergeRequest, err)
	}
	if err := ensureEOF(decoder); err != nil {
		return fmt.Errorf("%w: invalid manual value: %v", ErrInvalidMergeRequest, err)
	}
	return nil
}

func propertiesFor(snapshot Snapshot, kind ElementKind, id string) ([]Property, bool) {
	if kind == ElementNode {
		for _, node := range snapshot.Nodes {
			if node.ID == id {
				return node.Properties, true
			}
		}
		return nil, false
	}
	for _, edge := range snapshot.Edges {
		if edge.ID == id {
			return edge.Properties, true
		}
	}
	return nil, false
}

func setProperty(snapshot *Snapshot, kind ElementKind, id, name string, value Property, exists bool) error {
	if kind == ElementNode {
		for i := range snapshot.Nodes {
			if snapshot.Nodes[i].ID == id {
				snapshot.Nodes[i].Properties = replaceProperty(snapshot.Nodes[i].Properties, name, value, exists)
				return nil
			}
		}
	} else {
		for i := range snapshot.Edges {
			if snapshot.Edges[i].ID == id {
				snapshot.Edges[i].Properties = replaceProperty(snapshot.Edges[i].Properties, name, value, exists)
				return nil
			}
		}
	}
	return fmt.Errorf("%w: conflicted element %q no longer exists", ErrInvalidMergeRequest, id)
}

func replaceProperty(properties []Property, name string, value Property, exists bool) []Property {
	result := make([]Property, 0, len(properties)+1)
	replaced := false
	for _, property := range properties {
		if property.Name != name {
			result = append(result, property)
			continue
		}
		replaced = true
		if exists {
			result = append(result, value)
		}
	}
	if exists && !replaced {
		result = append(result, value)
	}
	sortProperties(result)
	return result
}

func setNode(nodes []Node, id string, value Node, exists bool) []Node {
	result := make([]Node, 0, len(nodes)+1)
	replaced := false
	for _, node := range nodes {
		if node.ID != id {
			result = append(result, node)
			continue
		}
		replaced = true
		if exists {
			result = append(result, value)
		}
	}
	if exists && !replaced {
		result = append(result, value)
	}
	sortNodes(result)
	return result
}

func setEdge(edges []Edge, id string, value Edge, exists bool) []Edge {
	result := make([]Edge, 0, len(edges)+1)
	replaced := false
	for _, edge := range edges {
		if edge.ID == id {
			replaced = true
			if exists {
				result = append(result, value)
			}
			continue
		}
		result = append(result, edge)
	}
	if exists && !replaced {
		result = append(result, value)
	}
	sortEdges(result)
	return result
}

func sortProperties(values []Property) {
	sort.Slice(values, func(i, j int) bool { return values[i].Name < values[j].Name })
}
func sortNodes(values []Node) {
	sort.Slice(values, func(i, j int) bool { return values[i].ID < values[j].ID })
}
func sortEdges(values []Edge) {
	sort.Slice(values, func(i, j int) bool { return values[i].ID < values[j].ID })
}
func hashBytes(data []byte) string { return serversync.ContentID(data) }
