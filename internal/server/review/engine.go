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
	"github.com/autonomous-bits/spool/graphcontract"
)

var (
	ErrInvalidPreviewRequest = errors.New("review: invalid merge preview request")
	ErrPreviewUnavailable    = errors.New("review: merge preview dependencies are unavailable")
)

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

type CommitIdentity struct {
	ID           string `json:"id"`
	SnapshotRoot string `json:"snapshotRoot"`
	Format       uint32 `json:"-"`
}

type ChangeKind string

const (
	ChangeKindElement  ChangeKind = "element"
	ChangeKindProperty ChangeKind = "property"
	ChangeKindSchema   ChangeKind = "schema"
)

type ChangeOperation string

const (
	ChangeAdded   ChangeOperation = "added"
	ChangeUpdated ChangeOperation = "updated"
	ChangeDeleted ChangeOperation = "deleted"
)

type ElementKind string

const (
	ElementNode   ElementKind = "node"
	ElementEdge   ElementKind = "edge"
	ElementSchema ElementKind = "schema"
)

type Change struct {
	Kind      ChangeKind      `json:"kind"`
	Operation ChangeOperation `json:"operation"`
	Element   ElementKind     `json:"element"`
	ElementID string          `json:"elementId,omitempty"`
	Property  string          `json:"property,omitempty"`
	Branch    string          `json:"branch"`
}

type ConflictType string

const (
	ConflictProperty     ConflictType = "property"
	ConflictDeleteUpdate ConflictType = "delete_update"
	ConflictStructural   ConflictType = "structural"
	ConflictSchema       ConflictType = "schema"
	ConflictCardinality  ConflictType = "cardinality"
)

type ConflictToken struct {
	Token     string       `json:"token"`
	Type      ConflictType `json:"type"`
	Element   ElementKind  `json:"element"`
	ElementID string       `json:"elementId,omitempty"`
	Property  string       `json:"property,omitempty"`
}

type ResolutionRequirement struct {
	Token    string   `json:"token"`
	Required bool     `json:"required"`
	Options  []string `json:"options"`
}

// MergePreview contains Rack-specific merge mechanics and graphcontract's
// normalized semantic failures, unmodified and in canonical order.
type MergePreview struct {
	SourceBranch string `json:"sourceBranch"`
	TargetBranch string `json:"targetBranch"`

	BaseCommit   CommitIdentity `json:"baseCommit"`
	SourceCommit CommitIdentity `json:"sourceCommit"`
	TargetCommit CommitIdentity `json:"targetCommit"`

	CanFastForward bool                            `json:"canFastForward"`
	CleanChanges   []Change                        `json:"cleanChanges"`
	Conflicts      []ConflictToken                 `json:"conflicts"`
	Violations     []graphcontract.SchemaViolation `json:"violations"`
	HasConflicts   bool                            `json:"hasConflicts"`
	Resolutions    []ResolutionRequirement         `json:"resolutionRequirements"`
}

type Engine interface {
	PreviewMerge(ctx context.Context, tenantID, repoID, sourceBranch, targetBranch string) (*MergePreview, error)
}

type PreviewEngine struct {
	objects SnapshotObjectStore
	store   MetadataStore
}

var _ Engine = (*PreviewEngine)(nil)

func NewPreviewEngine(objects SnapshotObjectStore, store MetadataStore) *PreviewEngine {
	return &PreviewEngine{objects: objects, store: store}
}

func NewEngine(objects SnapshotObjectStore, store MetadataStore) *PreviewEngine {
	return NewPreviewEngine(objects, store)
}

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
	preview.SourceBranch, preview.TargetBranch = sourceBranch, targetBranch
	preview.BaseCommit, preview.SourceCommit, preview.TargetCommit = baseCommit, sourceCommit, targetCommit
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
	result := MergePreview{CleanChanges: []Change{}, Conflicts: []ConflictToken{}, Violations: []graphcontract.SchemaViolation{}, Resolutions: []ResolutionRequirement{}}
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
	err := graphcontract.ValidateSchemaSnapshot(snapshot.Schema, snapshot.Nodes, snapshot.Edges)
	if err == nil {
		return
	}
	var validation *graphcontract.SchemaValidationError
	if !errors.As(err, &validation) {
		result.addConflict(ConflictSchema, ElementSchema, "", "")
		return
	}
	result.Violations = append(result.Violations, validation.Violations...)
	conflict := ConflictCardinality
	for _, violation := range validation.Violations {
		if !strings.Contains(string(violation.Code), "cardinality") {
			conflict = ConflictSchema
			break
		}
	}
	result.addConflict(conflict, ElementSchema, "", "")
}

func mergeElements[T Node | Edge](base, source, target map[string]T, kind ElementKind, result *MergePreview) map[string]T {
	merged := make(map[string]T)
	for _, id := range elementIDs(base, source, target) {
		b, bok := base[id]
		s, sok := source[id]
		t, tok := target[id]
		value, include := mergeElement(id, b, bok, s, sok, t, tok, kind, result)
		if include {
			merged[id] = value
		}
	}
	return merged
}

func mergeElement[T Node | Edge](id string, base T, baseOK bool, source T, sourceOK bool, target T, targetOK bool, kind ElementKind, result *MergePreview) (T, bool) {
	var zero T
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
	return withProperties(structure, mergeProperties(elementProperties(base), elementProperties(source), elementProperties(target), kind, id, result)), true
}

func mergeProperties(base, source, target map[string]graphcontract.PropertyValue, kind ElementKind, id string, result *MergePreview) map[string]graphcontract.PropertyValue {
	merged := make(map[string]graphcontract.PropertyValue)
	for _, name := range propertyNames(base, source, target) {
		b, bok := base[name]
		s, sok := source[name]
		t, tok := target[name]
		value, exists, branch, conflict := mergeOptional(b, bok, s, sok, t, tok)
		if conflict {
			result.addConflict(ConflictProperty, kind, id, name)
			value, exists = t, tok
		} else if !optionalEqual(b, bok, value, exists) {
			result.CleanChanges = append(result.CleanChanges, Change{Kind: ChangeKindProperty, Operation: propertyOperation(bok, exists), Element: kind, ElementID: id, Property: name, Branch: branch})
		}
		if exists {
			merged[name] = value
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

func propertyNames(maps ...map[string]graphcontract.PropertyValue) []string {
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

func elementProperties[T Node | Edge](element T) map[string]graphcontract.PropertyValue {
	switch value := any(element).(type) {
	case Node:
		return value.Properties
	case Edge:
		return value.Properties
	default:
		return nil
	}
}

func withProperties[T Node | Edge](element T, properties map[string]graphcontract.PropertyValue) T {
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
		return leftValue.ID == rightValue.ID && leftValue.Title == rightValue.Title && reflect.DeepEqual(leftValue.Labels, rightValue.Labels)
	case Edge:
		rightValue := any(right).(Edge)
		return leftValue.ID == rightValue.ID && leftValue.Source == rightValue.Source && leftValue.Target == rightValue.Target && leftValue.Type == rightValue.Type
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
