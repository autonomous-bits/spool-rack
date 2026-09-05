// Package review provides read-only, deterministic graph merge previews.
package review

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
)

var (
	// ErrInvalidPreviewRequest indicates missing or malformed preview inputs.
	ErrInvalidPreviewRequest = errors.New("review: invalid merge preview request")
	// ErrPreviewUnavailable indicates an Engine was constructed without its
	// read-only metadata or object-store dependencies.
	ErrPreviewUnavailable = errors.New("review: merge preview dependencies are unavailable")
)

// MetadataStore is the read-only metadata capability required by PreviewEngine.
// It deliberately excludes branch compare-and-swap and commit-writing methods:
// generating a preview must not mutate repository state.
type MetadataStore interface {
	SetTenantContext(ctx context.Context, tenantID string) (context.Context, error)
	GetBranchRef(ctx context.Context, repoID, branch string) (string, error)
	IsAncestor(ctx context.Context, repoID, ancestorCommit, commit string) (bool, error)
	FindLowestCommonAncestor(ctx context.Context, repoID, sourceCommitID, targetCommitID string) (string, error)
	GetCommitSnapshotRoot(ctx context.Context, repoID, commitID string) (string, error)
}

var _ MetadataStore = (postgres.Store)(nil)

type commitMetadataStore interface {
	GetCommitMetadata(ctx context.Context, repoID, commitID string) (postgres.CommitMetadata, error)
}

// CommitIdentity identifies an immutable commit and its immutable graph root.
type CommitIdentity struct {
	ID           string `json:"id"`
	SnapshotRoot string `json:"snapshotRoot"`
	// Format is intentionally not exposed in existing HTTP JSON responses.
	// It is only needed inside Rack's v2 commit framing.
	Format uint32 `json:"-"`
}

// ChangeKind identifies the portion of a graph affected by a clean change.
type ChangeKind string

const (
	ChangeKindElement  ChangeKind = "element"
	ChangeKindProperty ChangeKind = "property"
	ChangeKindSchema   ChangeKind = "schema"
)

// ChangeOperation identifies how a clean item differs from the merge base.
type ChangeOperation string

const (
	ChangeAdded   ChangeOperation = "added"
	ChangeUpdated ChangeOperation = "updated"
	ChangeDeleted ChangeOperation = "deleted"
)

// ElementKind identifies a graph element.
type ElementKind string

const (
	ElementNode   ElementKind = "node"
	ElementEdge   ElementKind = "edge"
	ElementSchema ElementKind = "schema"
)

// Change is a deterministic, non-conflicting input to the simulated merge.
// Branch is "source", "target", or "both" when both sides made the same change.
type Change struct {
	Kind      ChangeKind      `json:"kind"`
	Operation ChangeOperation `json:"operation"`
	Element   ElementKind     `json:"element"`
	ElementID string          `json:"elementId,omitempty"`
	Property  string          `json:"property,omitempty"`
	Branch    string          `json:"branch"`
}

// ConflictType classifies a merge decision which needs user input.
type ConflictType string

const (
	ConflictProperty     ConflictType = "property"
	ConflictDeleteUpdate ConflictType = "delete_update"
	ConflictStructural   ConflictType = "structural"
	ConflictSchema       ConflictType = "schema"
	ConflictCardinality  ConflictType = "cardinality"
)

// ConflictToken is a stable, typed conflict location. Token can be retained by
// clients when submitting a later resolution; it is not a capability token.
type ConflictToken struct {
	Token     string       `json:"token"`
	Type      ConflictType `json:"type"`
	Element   ElementKind  `json:"element"`
	ElementID string       `json:"elementId,omitempty"`
	Property  string       `json:"property,omitempty"`
}

// ResolutionRequirement states what must happen before this preview can be
// applied by a separate, write-capable merge operation.
type ResolutionRequirement struct {
	Token    string   `json:"token"`
	Required bool     `json:"required"`
	Options  []string `json:"options"`
}

// MergePreview contains a complete read-only three-way merge decision.
type MergePreview struct {
	SourceBranch string `json:"sourceBranch"`
	TargetBranch string `json:"targetBranch"`

	BaseCommit   CommitIdentity `json:"baseCommit"`
	SourceCommit CommitIdentity `json:"sourceCommit"`
	TargetCommit CommitIdentity `json:"targetCommit"`

	CanFastForward bool                    `json:"canFastForward"`
	CleanChanges   []Change                `json:"cleanChanges"`
	Conflicts      []ConflictToken         `json:"conflicts"`
	HasConflicts   bool                    `json:"hasConflicts"`
	Resolutions    []ResolutionRequirement `json:"resolutionRequirements"`
}

// Engine is the read-only merge-preview interface used by request handlers.
type Engine interface {
	PreviewMerge(ctx context.Context, tenantID, repoID, sourceBranch, targetBranch string) (*MergePreview, error)
}

// PreviewEngine resolves branch heads and immutable snapshots, then simulates
// a merge. It holds no request state and has no write-capable dependency.
type PreviewEngine struct {
	objects SnapshotObjectStore
	store   MetadataStore
}

var _ Engine = (*PreviewEngine)(nil)

// NewPreviewEngine constructs a stateless, read-only merge preview engine.
func NewPreviewEngine(objects SnapshotObjectStore, store MetadataStore) *PreviewEngine {
	return &PreviewEngine{objects: objects, store: store}
}

// NewEngine is an alias for NewPreviewEngine.
func NewEngine(objects SnapshotObjectStore, store MetadataStore) *PreviewEngine {
	return NewPreviewEngine(objects, store)
}

// PreviewMerge resolves both heads under tenant context and computes a
// deterministic merge preview. It invokes only read methods on metadata and
// CAS; it never creates objects, commits, or branch-ref updates.
func (e *PreviewEngine) PreviewMerge(ctx context.Context, tenantID, repoID, sourceBranch, targetBranch string) (*MergePreview, error) {
	if e == nil || e.objects == nil || e.store == nil {
		return nil, ErrPreviewUnavailable
	}
	if ctx == nil || tenantID == "" || repoID == "" || sourceBranch == "" || targetBranch == "" {
		return nil, fmt.Errorf("%w: tenant, repository, source branch, and target branch are required", ErrInvalidPreviewRequest)
	}

	tenantCtx, err := e.store.SetTenantContext(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("review: set tenant context: %w", err)
	}
	scope, err := cas.NewScope(tenantID, repoID)
	if err != nil {
		return nil, fmt.Errorf("review: create CAS scope: %w", err)
	}
	sourceID, err := e.store.GetBranchRef(tenantCtx, repoID, sourceBranch)
	if err != nil {
		return nil, fmt.Errorf("review: resolve source branch %q: %w", sourceBranch, err)
	}
	targetID, err := e.store.GetBranchRef(tenantCtx, repoID, targetBranch)
	if err != nil {
		return nil, fmt.Errorf("review: resolve target branch %q: %w", targetBranch, err)
	}

	canFastForward, err := e.store.IsAncestor(tenantCtx, repoID, targetID, sourceID)
	if err != nil {
		return nil, fmt.Errorf("review: test fast-forward ancestry: %w", err)
	}
	baseID := targetID
	if !canFastForward {
		baseID, err = e.store.FindLowestCommonAncestor(tenantCtx, repoID, sourceID, targetID)
		if err != nil {
			return nil, fmt.Errorf("review: find merge base: %w", err)
		}
	}

	baseCommit, err := e.resolveCommitIdentity(tenantCtx, repoID, baseID)
	if err != nil {
		return nil, fmt.Errorf("review: resolve base snapshot %s: %w", baseID, err)
	}
	sourceCommit, err := e.resolveCommitIdentity(tenantCtx, repoID, sourceID)
	if err != nil {
		return nil, fmt.Errorf("review: resolve source snapshot %s: %w", sourceID, err)
	}
	targetCommit, err := e.resolveCommitIdentity(tenantCtx, repoID, targetID)
	if err != nil {
		return nil, fmt.Errorf("review: resolve target snapshot %s: %w", targetID, err)
	}

	base, err := DecodeSnapshot(tenantCtx, e.objects, scope, baseCommit.SnapshotRoot)
	if err != nil {
		return nil, fmt.Errorf("review: load base snapshot: %w", err)
	}
	source, err := DecodeSnapshot(tenantCtx, e.objects, scope, sourceCommit.SnapshotRoot)
	if err != nil {
		return nil, fmt.Errorf("review: load source snapshot: %w", err)
	}
	target, err := DecodeSnapshot(tenantCtx, e.objects, scope, targetCommit.SnapshotRoot)
	if err != nil {
		return nil, fmt.Errorf("review: load target snapshot: %w", err)
	}

	preview := mergeSnapshots(base, source, target)
	preview.SourceBranch = sourceBranch
	preview.TargetBranch = targetBranch
	preview.BaseCommit = baseCommit
	preview.SourceCommit = sourceCommit
	preview.TargetCommit = targetCommit
	preview.CanFastForward = canFastForward
	return &preview, nil
}

func (e *PreviewEngine) resolveCommitIdentity(ctx context.Context, repoID, commitID string) (CommitIdentity, error) {
	if metadataStore, ok := e.store.(commitMetadataStore); ok {
		metadata, err := metadataStore.GetCommitMetadata(ctx, repoID, commitID)
		if err != nil {
			return CommitIdentity{}, err
		}
		return CommitIdentity{ID: metadata.ID, SnapshotRoot: metadata.SnapshotRoot, Format: metadata.Format}, nil
	}
	root, err := e.store.GetCommitSnapshotRoot(ctx, repoID, commitID)
	if err != nil {
		return CommitIdentity{}, err
	}
	return CommitIdentity{ID: commitID, SnapshotRoot: root}, nil
}

func mergeSnapshots(base, source, target Snapshot) MergePreview {
	result := MergePreview{CleanChanges: []Change{}, Conflicts: []ConflictToken{}, Resolutions: []ResolutionRequirement{}}
	mergedSchema, schemaBranch, schemaConflict := mergeValue(base.Schema, source.Schema, target.Schema)
	if schemaConflict {
		result.addConflict(ConflictSchema, ElementSchema, "", "")
		mergedSchema = target.Schema
	} else if !reflect.DeepEqual(base.Schema, mergedSchema) {
		result.CleanChanges = append(result.CleanChanges, Change{Kind: ChangeKindSchema, Operation: ChangeUpdated, Element: ElementSchema, Branch: schemaBranch})
	}

	nodes := mergeElements(base.Nodes, source.Nodes, target.Nodes, ElementNode, &result)
	edges := mergeElements(base.Edges, source.Edges, target.Edges, ElementEdge, &result)
	merged := Snapshot{Version: SnapshotVersion, Schema: mergedSchema, Nodes: nodes, Edges: edges}
	if !result.HasConflicts {
		validateMergedSchema(merged, &result)
	}
	sortChanges(result.CleanChanges)
	sortConflicts(result.Conflicts)
	sort.Slice(result.Resolutions, func(i, j int) bool { return result.Resolutions[i].Token < result.Resolutions[j].Token })
	return result
}

func validateMergedSchema(snapshot Snapshot, result *MergePreview) {
	if err := validatePropertyRules(snapshot.Schema, snapshot.Nodes, snapshot.Edges); err != nil {
		result.addConflict(ConflictSchema, ElementSchema, "", "")
		return
	}
	byID := make(map[string]Node, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		byID[node.ID] = node
	}
	if err := validateCardinalities(snapshot.Schema.Cardinalities, snapshot.Nodes, snapshot.Edges, byID); err != nil {
		result.addConflict(ConflictCardinality, ElementSchema, "", "")
	}
}

func mergeElements[T Node | Edge](base, source, target []T, kind ElementKind, result *MergePreview) []T {
	baseByID, sourceByID, targetByID := elementsByID(base), elementsByID(source), elementsByID(target)
	ids := elementIDs(baseByID, sourceByID, targetByID)
	merged := make([]T, 0, len(ids))
	for _, id := range ids {
		b, bok := baseByID[id]
		s, sok := sourceByID[id]
		t, tok := targetByID[id]
		value, include := mergeElement(b, bok, s, sok, t, tok, kind, result)
		if include {
			merged = append(merged, value)
		}
	}
	return merged
}

func mergeElement[T Node | Edge](base T, baseOK bool, source T, sourceOK bool, target T, targetOK bool, kind ElementKind, result *MergePreview) (T, bool) {
	var zero T
	id := elementID(elementForID(base, baseOK, source, sourceOK, target))
	if baseOK && !sourceOK && targetOK && !reflect.DeepEqual(base, target) {
		result.addConflict(ConflictDeleteUpdate, kind, id, "")
		return target, true
	}
	if baseOK && sourceOK && !targetOK && !reflect.DeepEqual(base, source) {
		result.addConflict(ConflictDeleteUpdate, kind, id, "")
		return zero, false
	}
	if !baseOK && sourceOK && targetOK && !sameStructure(source, target) {
		result.addConflict(ConflictStructural, kind, id, "")
		return target, true
	}

	switch {
	case !baseOK && !sourceOK && !targetOK:
		return zero, false
	case !baseOK && sourceOK && !targetOK:
		result.addElementChange(kind, id, ChangeAdded, "source")
		return source, true
	case !baseOK && !sourceOK && targetOK:
		result.addElementChange(kind, id, ChangeAdded, "target")
		return target, true
	case baseOK && !sourceOK && !targetOK:
		result.addElementChange(kind, id, ChangeDeleted, "both")
		return zero, false
	case baseOK && !sourceOK:
		result.addElementChange(kind, id, ChangeDeleted, "source")
		return zero, false
	case baseOK && !targetOK:
		result.addElementChange(kind, id, ChangeDeleted, "target")
		return zero, false
	}

	structure, branch, conflict := mergeStructure(base, source, target)
	if conflict {
		result.addConflict(ConflictStructural, kind, id, "")
		structure = target
	} else if !sameStructure(base, structure) {
		result.addElementChange(kind, id, ChangeUpdated, branch)
	}
	properties := mergeProperties(elementProperties(base), elementProperties(source), elementProperties(target), kind, id, result)
	return withProperties(structure, properties), true
}

func mergeProperties(base, source, target []Property, kind ElementKind, id string, result *MergePreview) []Property {
	b, s, t := propertiesByName(base), propertiesByName(source), propertiesByName(target)
	names := propertyNames(b, s, t)
	merged := make([]Property, 0, len(names))
	for _, name := range names {
		baseValue, baseOK := b[name]
		sourceValue, sourceOK := s[name]
		targetValue, targetOK := t[name]
		value, exists, branch, conflict := mergeOptional(baseValue, baseOK, sourceValue, sourceOK, targetValue, targetOK)
		if conflict {
			result.addConflict(ConflictProperty, kind, id, name)
			value, exists = targetValue, targetOK
		} else if !optionalEqual(baseValue, baseOK, value, exists) {
			result.CleanChanges = append(result.CleanChanges, Change{
				Kind: ChangeKindProperty, Operation: propertyOperation(baseOK, exists), Element: kind, ElementID: id, Property: name, Branch: branch,
			})
		}
		if exists {
			merged = append(merged, value)
		}
	}
	return merged
}

func mergeValue[T any](base, source, target T) (T, string, bool) {
	if reflect.DeepEqual(source, base) {
		return target, "target", false
	}
	if reflect.DeepEqual(target, base) {
		return source, "source", false
	}
	if reflect.DeepEqual(source, target) {
		return source, "both", false
	}
	return target, "", true
}

func mergeOptional[T any](base T, baseOK bool, source T, sourceOK bool, target T, targetOK bool) (T, bool, string, bool) {
	if optionalEqual(source, sourceOK, base, baseOK) {
		return target, targetOK, "target", false
	}
	if optionalEqual(target, targetOK, base, baseOK) {
		return source, sourceOK, "source", false
	}
	if optionalEqual(source, sourceOK, target, targetOK) {
		return source, sourceOK, "both", false
	}
	return target, targetOK, "", true
}

func optionalEqual[T any](left T, leftOK bool, right T, rightOK bool) bool {
	return leftOK == rightOK && (!leftOK || reflect.DeepEqual(left, right))
}

func propertyOperation(before, after bool) ChangeOperation {
	if !before {
		return ChangeAdded
	}
	if !after {
		return ChangeDeleted
	}
	return ChangeUpdated
}

func (p *MergePreview) addElementChange(kind ElementKind, id string, operation ChangeOperation, branch string) {
	p.CleanChanges = append(p.CleanChanges, Change{Kind: ChangeKindElement, Operation: operation, Element: kind, ElementID: id, Branch: branch})
}

func (p *MergePreview) addConflict(kind ConflictType, element ElementKind, id, property string) {
	token := string(kind) + ":" + string(element)
	if id != "" {
		token += ":" + id
	}
	if property != "" {
		token += ":" + property
	}
	for _, existing := range p.Conflicts {
		if existing.Token == token {
			return
		}
	}
	p.HasConflicts = true
	p.Conflicts = append(p.Conflicts, ConflictToken{Token: token, Type: kind, Element: element, ElementID: id, Property: property})
	p.Resolutions = append(p.Resolutions, ResolutionRequirement{Token: token, Required: true, Options: []string{"source", "target", "manual"}})
}

func sortChanges(changes []Change) {
	sort.Slice(changes, func(i, j int) bool {
		left, right := changes[i], changes[j]
		return strings.Join([]string{string(left.Kind), string(left.Element), left.ElementID, left.Property, string(left.Operation), left.Branch}, "\x00") <
			strings.Join([]string{string(right.Kind), string(right.Element), right.ElementID, right.Property, string(right.Operation), right.Branch}, "\x00")
	})
}

func sortConflicts(conflicts []ConflictToken) {
	sort.Slice(conflicts, func(i, j int) bool { return conflicts[i].Token < conflicts[j].Token })
}

func elementsByID[T Node | Edge](elements []T) map[string]T {
	result := make(map[string]T, len(elements))
	for _, element := range elements {
		result[elementID(element)] = element
	}
	return result
}

func elementIDs[T Node | Edge](maps ...map[string]T) []string {
	seen := make(map[string]struct{})
	for _, values := range maps {
		for id := range values {
			seen[id] = struct{}{}
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func propertyNames(maps ...map[string]Property) []string {
	seen := make(map[string]struct{})
	for _, values := range maps {
		for name := range values {
			seen[name] = struct{}{}
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func propertiesByName(properties []Property) map[string]Property {
	result := make(map[string]Property, len(properties))
	for _, property := range properties {
		result[property.Name] = property
	}
	return result
}

func elementForID[T Node | Edge](base T, baseOK bool, source T, sourceOK bool, target T) T {
	if baseOK {
		return base
	}
	if sourceOK {
		return source
	}
	return target
}

func elementID[T Node | Edge](element T) string {
	switch value := any(element).(type) {
	case Node:
		return value.ID
	case Edge:
		return value.ID
	default:
		return ""
	}
}

func elementProperties[T Node | Edge](element T) []Property {
	switch value := any(element).(type) {
	case Node:
		return value.Properties
	case Edge:
		return value.Properties
	default:
		return nil
	}
}

func withProperties[T Node | Edge](element T, properties []Property) T {
	switch value := any(element).(type) {
	case Node:
		value.Properties = properties
		return any(value).(T)
	case Edge:
		value.Properties = properties
		return any(value).(T)
	default:
		return element
	}
}

func sameStructure[T Node | Edge](left, right T) bool {
	switch leftValue := any(left).(type) {
	case Node:
		rightValue := any(right).(Node)
		return leftValue.ID == rightValue.ID && reflect.DeepEqual(leftValue.Labels, rightValue.Labels)
	case Edge:
		rightValue := any(right).(Edge)
		return leftValue.ID == rightValue.ID && leftValue.From == rightValue.From && leftValue.To == rightValue.To && reflect.DeepEqual(leftValue.Labels, rightValue.Labels)
	default:
		return false
	}
}

func mergeStructure[T Node | Edge](base, source, target T) (T, string, bool) {
	baseProperties := elementProperties(base)
	base = withProperties(base, nil)
	source = withProperties(source, nil)
	target = withProperties(target, nil)
	value, branch, conflict := mergeValue(base, source, target)
	return withProperties(value, baseProperties), branch, conflict
}
