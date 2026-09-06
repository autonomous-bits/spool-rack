// Package nativepush implements Rack's server-side push validation pipeline
// for native Spool packs (graphcontract's binary pack container format,
// verified via internal/server/nativeindex). It is the atomic
// validate-then-publish boundary for native pushes: a pack, and every commit
// and graph snapshot it carries, must be fully verified — pack integrity,
// commit DAG connectivity, and tenant schema conformance — before any commit
// metadata is registered or a branch ref is advanced. A failure at any stage
// leaves the target branch ref, and every commit row a successful push would
// have registered, completely unchanged.
//
// Wire contract for native commit/graph objects: this pipeline is the first
// server-side consumer of native pushes, so it establishes (and documents
// here) the object-type conventions the CLI-side producer must follow:
//
//   - A pushed commit is packed with object type ObjectTypeCommit: its bytes
//     are the canonical CBOR encoding produced by graphcontract.MarshalCommit,
//     and its object ID is
//     graphcontract.ObjectIDForEncoded(ObjectTypeCommit, bytes).
//   - Each commit's Snapshot field names an object packed with object type
//     ObjectTypeSnapshot: its bytes are the canonical envelope produced by
//     review.MarshalSnapshotCBOR — the same fully materialized
//     schema+nodes+edges representation Rack's legacy merge/review pipeline
//     already uses — and its object ID is
//     graphcontract.ObjectIDForEncoded(ObjectTypeSnapshot, bytes). Reusing
//     this envelope lets HandlePush reuse review's existing schema-validating
//     decoder instead of re-implementing schema checks.
package nativepush

import (
	"context"
	"errors"
	"fmt"

	"github.com/autonomous-bits/spool-rack/internal/server/nativeindex"
	"github.com/autonomous-bits/spool-rack/internal/server/review"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
	serversync "github.com/autonomous-bits/spool-rack/internal/server/sync"
	"github.com/autonomous-bits/spool/graphcontract"
)

const (
	// ObjectTypeCommit identifies a packed graphcontract.Commit object.
	ObjectTypeCommit = "commit"
	// ObjectTypeSnapshot identifies a packed, fully materialized graph
	// snapshot object encoded with review.MarshalSnapshotCBOR.
	ObjectTypeSnapshot = "snapshot"

	// CommitFormatNative identifies a commit registered from a verified
	// native Spool pack, distinct from Rack's legacy opaque format (1) and
	// its own v2 CBOR pack framing (2).
	CommitFormatNative uint32 = 3
)

var (
	// ErrInvalidNativePush indicates a native push failed structural or
	// cross-consistency validation (malformed request, a commit record that
	// does not match its packed object, an unsupported base commit format,
	// or a snapshot that fails schema validation). No commit metadata is
	// registered and no branch ref is advanced when this error is returned.
	ErrInvalidNativePush = errors.New("nativepush: invalid native push")
	// ErrDisconnectedPush indicates a pushed commit references a parent or
	// snapshot object that is not present in the verified pack and not
	// already known to the repository. No commit metadata is registered and
	// no branch ref is advanced when this error is returned.
	ErrDisconnectedPush = errors.New("nativepush: push references a disconnected or missing object")
)

// CommitRecord names one native commit included in a push, supplied
// oldest-ancestor-first. Commit is cross-checked against the actual packed
// object at ID before it is trusted; a caller cannot register commit
// metadata that does not match what was actually verified in the pack.
type CommitRecord struct {
	ID     graphcontract.ObjectID
	Commit graphcontract.Commit
}

// PushRequest contains parameters for an incoming native branch push.
type PushRequest struct {
	TenantID     string
	RepoID       string
	Branch       string
	BaseCommit   string
	TargetCommit string

	// PackID, PackManifest, PackData, and PackEntries describe the native
	// pack exactly as required by nativeindex.Indexer.IndexPack.
	PackID       graphcontract.PackID
	PackManifest graphcontract.PackManifest
	PackData     []byte
	PackEntries  []graphcontract.PackIndexEntry

	// Commits are the commit records this push wants registered and made
	// reachable from Branch, oldest-ancestor-first. Each one's packed
	// "commit" object must continue the pushed history: the first pushed
	// commit's first parent must equal BaseCommit, and every subsequent
	// commit's first parent must equal the previous pushed commit's ID.
	Commits []CommitRecord
}

// packedObject is one decoded object from a verified native pack.
type packedObject struct {
	objectType string
	data       []byte
}

// Store is the storage capability Engine depends on. It embeds
// serversync.BranchStore for the read-side operations HandlePush relies on
// to verify a push (resolving commit metadata, checking ancestry, reading
// pack ranges), and adds the durable, transactional publish and
// retention-anchoring operations that make native push retry-safe: see
// PublishNativePush for the idempotency guarantee, and HandlePush for how
// PutActiveTransaction/DeleteActiveTransaction protect a verified pack's
// objects from concurrent retention collection while a push is in flight.
type Store interface {
	serversync.BranchStore
	// PublishNativePush durably registers this push's commits and advances
	// Branch in a single transaction keyed by an idempotency token, so a
	// retried push recognizes and returns its own prior success instead of
	// re-publishing or misdiagnosing a non-fast-forward conflict.
	PublishNativePush(ctx context.Context, pub postgres.NativePushPublication) (postgres.NativePushResult, error)
	// PutActiveTransaction anchors retention for a verified pack's objects
	// while HandlePush is validating and publishing it, so a concurrent
	// retention pass cannot collect objects an in-flight (or crashed,
	// resumable) push has already durably indexed but not yet made
	// reachable through any commit or branch ref.
	PutActiveTransaction(ctx context.Context, repoID, transactionID, commitID, objectID string) error
	// DeleteActiveTransaction releases the anchor registered by
	// PutActiveTransaction once HandlePush's outcome (success or rejection)
	// is final.
	DeleteActiveTransaction(ctx context.Context, repoID, transactionID string) error
}

// Engine persists verified native packs and advances branch refs only after
// the pack, its commits, and their graph snapshots are all proven valid.
type Engine struct {
	indexer *nativeindex.Indexer
	store   Store
}

// NewEngine constructs an Engine backed by a native pack indexer and branch
// metadata storage.
func NewEngine(indexer *nativeindex.Indexer, store Store) *Engine {
	return &Engine{indexer: indexer, store: store}
}

// HandlePush fully verifies req's pack, the commit(s) it carries, and their
// referenced graph snapshots before registering any commit metadata or
// advancing Branch. It enforces fast-forward: Branch only advances when
// BaseCommit names its current head. Any failure — corrupt pack,
// schema-invalid graph state, a disconnected/missing reference, or a
// non-fast-forward attempt — leaves Branch and all commit metadata
// completely unchanged.
//
// HandlePush is retry-safe (goal-rack-upload-idempotency): the verified
// pack's own PackID is used, unmodified, as an idempotency token. A client
// that retries a push it never saw the response for (the pack, and every
// commit and ref update it produced, already fully applied) receives the
// same successful outcome instead of a confusing non-fast-forward rejection
// — see postgres.PGStore.PublishNativePush for how the durable upload
// transaction recognizes and short-circuits such a retry.
func (e *Engine) HandlePush(ctx context.Context, req PushRequest) error {
	if err := validatePushRequest(req); err != nil {
		return err
	}

	// Step 1: fully verify the pack (header, manifest, entries, and every
	// object's CRC32/decompression/envelope/hash) and persist it only once
	// proven valid. This write is idempotent and content-addressed, so
	// performing it before the checks below (which may still reject the
	// push) cannot leave any branch ref or commit row in a partial state.
	if err := e.indexer.IndexPack(ctx, req.TenantID, req.RepoID, req.PackID, req.PackManifest, req.PackData, req.PackEntries, req.TargetCommit); err != nil {
		return fmt.Errorf("nativepush: verify pack: %w", err)
	}

	tenantCtx, err := e.store.SetTenantContext(ctx, req.TenantID)
	if err != nil {
		return fmt.Errorf("nativepush: set tenant context: %w", err)
	}

	// Step 2: anchor retention on the verified pack's target commit object
	// for the remainder of this push. The pack's objects are now durably
	// indexed but not yet reachable from any commit or branch ref; without
	// this anchor a concurrent retention pass — or a crash that leaves this
	// push interrupted and later resumed by a retry — could collect them
	// out from under the in-flight publish. The transaction ID is the
	// pack's own idempotency token, so re-registering it on a retry is a
	// harmless no-op, and it is always released once this call's outcome
	// (success or rejection) is final.
	transactionID := string(req.PackID)
	if err := e.store.PutActiveTransaction(tenantCtx, req.RepoID, transactionID, "", req.TargetCommit); err != nil {
		return fmt.Errorf("nativepush: anchor retention for pack %s: %w", req.PackID, err)
	}
	defer func() {
		if delErr := e.store.DeleteActiveTransaction(tenantCtx, req.RepoID, transactionID); delErr != nil && !errors.Is(delErr, postgres.ErrActiveTransactionNotFound) {
			// Best-effort cleanup: leaving the anchor behind only over-retains
			// a verified, harmless pack until a future retention pass or
			// manual cleanup removes it. It never affects correctness of
			// readable committed history, so a cleanup failure must not
			// override this call's actual result.
			_ = delErr
		}
	}()

	packed, err := decodePackedObjects(req.PackData, req.PackEntries)
	if err != nil {
		return err
	}

	commits, err := decodeAndVerifyCommitRecords(req.Commits, packed)
	if err != nil {
		return err
	}

	baseMetadata, err := e.store.GetCommitMetadata(tenantCtx, req.RepoID, req.BaseCommit)
	if err != nil {
		if errors.Is(err, postgres.ErrCommitNotFound) {
			return fmt.Errorf("%w: base commit %s is not known to this repository", ErrDisconnectedPush, req.BaseCommit)
		}
		return fmt.Errorf("nativepush: resolve base commit: %w", err)
	}
	if baseMetadata.Format != CommitFormatNative {
		return fmt.Errorf("%w: base commit %s has format %d, want native format %d", ErrInvalidNativePush, req.BaseCommit, baseMetadata.Format, CommitFormatNative)
	}

	if err := e.verifyCommitDAG(ctx, tenantCtx, req, commits, packed); err != nil {
		return err
	}

	// Step 3: all pack, DAG, schema, and fast-forward verification has
	// succeeded (fast-forward is verified, and the branch ref advanced,
	// inside PublishNativePush's single durable transaction — see its
	// doc comment for why the check cannot safely happen as a separate,
	// earlier step). Publish every commit and advance the branch ref
	// atomically, keyed by this push's idempotency token so a retry of an
	// already-applied push is recognized instead of re-executed.
	publication := postgres.NativePushPublication{
		RepoID:         req.RepoID,
		Branch:         req.Branch,
		IdempotencyKey: transactionID,
		BaseCommit:     req.BaseCommit,
		TargetCommit:   req.TargetCommit,
		Commits:        make([]postgres.NativePushCommit, len(req.Commits)),
	}
	for i, record := range req.Commits {
		publication.Commits[i] = postgres.NativePushCommit{ID: record.ID, Commit: record.Commit, Format: CommitFormatNative}
	}

	if _, err := e.store.PublishNativePush(tenantCtx, publication); err != nil {
		var nffErr *postgres.NonFastForwardHeadError
		if errors.As(err, &nffErr) {
			return e.diagnoseNonFastForward(tenantCtx, req, nffErr.ActualHead)
		}
		return fmt.Errorf("nativepush: publish push: %w", err)
	}

	return nil
}

func validatePushRequest(req PushRequest) error {
	switch {
	case req.TenantID == "":
		return fmt.Errorf("%w: tenant ID is required", ErrInvalidNativePush)
	case req.RepoID == "":
		return fmt.Errorf("%w: repo ID is required", ErrInvalidNativePush)
	case req.Branch == "":
		return fmt.Errorf("%w: branch is required", ErrInvalidNativePush)
	case req.BaseCommit == "":
		return fmt.Errorf("%w: base commit is required", ErrInvalidNativePush)
	case req.TargetCommit == "":
		return fmt.Errorf("%w: target commit is required", ErrInvalidNativePush)
	case len(req.Commits) == 0:
		return fmt.Errorf("%w: a native push must include at least one commit", ErrInvalidNativePush)
	}
	for i, record := range req.Commits {
		if record.ID == "" {
			return fmt.Errorf("%w: commit %d has no ID", ErrInvalidNativePush, i)
		}
	}
	if string(req.Commits[len(req.Commits)-1].ID) != req.TargetCommit {
		return fmt.Errorf("%w: target commit does not name the last commit in the push", ErrInvalidNativePush)
	}
	return nil
}

// decodePackedObjects decodes every entry of an already pack-verified pack,
// keyed by object ID, so HandlePush can repeatedly resolve pushed commits and
// their snapshots without re-fetching from CAS.
func decodePackedObjects(packData []byte, entries []graphcontract.PackIndexEntry) (map[graphcontract.ObjectID]packedObject, error) {
	objects := make(map[graphcontract.ObjectID]packedObject, len(entries))
	for _, entry := range entries {
		if entry.Offset+entry.CompressedSize > uint64(len(packData)) {
			return nil, fmt.Errorf("%w: object %s entry extends beyond pack data", ErrInvalidNativePush, entry.Object)
		}
		compressed := packData[entry.Offset : entry.Offset+entry.CompressedSize]
		objectType, data, err := graphcontract.DecompressPackedObject(entry, compressed)
		if err != nil {
			return nil, fmt.Errorf("%w: decode object %s: %v", ErrInvalidNativePush, entry.Object, err)
		}
		objects[entry.Object] = packedObject{objectType: objectType, data: data}
	}
	return objects, nil
}

// decodeAndVerifyCommitRecords cross-checks every supplied CommitRecord
// against the actual verified packed object at its ID, rejecting the push if
// the record's ID is absent from the pack, names an object of the wrong
// type, or disagrees with the commit that was actually verified there. This
// makes it impossible for a caller to register commit metadata that was
// never proven valid by the pack verification step.
func decodeAndVerifyCommitRecords(records []CommitRecord, packed map[graphcontract.ObjectID]packedObject) ([]graphcontract.Commit, error) {
	commits := make([]graphcontract.Commit, len(records))
	for i, record := range records {
		object, ok := packed[record.ID]
		if !ok {
			return nil, fmt.Errorf("%w: commit %s is not present in the verified pack", ErrDisconnectedPush, record.ID)
		}
		if object.objectType != ObjectTypeCommit {
			return nil, fmt.Errorf("%w: object %s has type %q, want %q", ErrInvalidNativePush, record.ID, object.objectType, ObjectTypeCommit)
		}
		commit, err := graphcontract.UnmarshalCommit(object.data)
		if err != nil {
			return nil, fmt.Errorf("%w: decode commit %s: %v", ErrInvalidNativePush, record.ID, err)
		}
		if !commit.Equal(record.Commit) {
			return nil, fmt.Errorf("%w: commit %s metadata does not match its packed object", ErrInvalidNativePush, record.ID)
		}
		commits[i] = commit
	}
	return commits, nil
}

// verifyCommitDAG walks the pushed commits oldest-first, verifying that each
// one's first parent continues the pushed history (from BaseCommit, then from
// the previous pushed commit), that every additional (merge) parent is
// already known, and that each commit's snapshot object is present and
// schema-valid. It performs no writes; HandlePush only registers metadata
// once every commit here has passed.
func (e *Engine) verifyCommitDAG(ctx, tenantCtx context.Context, req PushRequest, commits []graphcontract.Commit, packed map[graphcontract.ObjectID]packedObject) error {
	previous := req.BaseCommit
	known := make(map[string]struct{}, len(req.Commits))

	for i, commit := range commits {
		record := req.Commits[i]

		if len(commit.Parents) == 0 || string(commit.Parents[0]) != previous {
			return fmt.Errorf("%w: commit %s does not continue the pushed history from %s", ErrInvalidNativePush, record.ID, previous)
		}
		for _, parent := range commit.Parents[1:] {
			if _, ok := known[string(parent)]; ok {
				continue
			}
			if _, err := e.store.GetCommitMetadata(tenantCtx, req.RepoID, string(parent)); err != nil {
				if errors.Is(err, postgres.ErrCommitNotFound) {
					return fmt.Errorf("%w: commit %s references unknown parent %s", ErrDisconnectedPush, record.ID, parent)
				}
				return fmt.Errorf("nativepush: resolve parent %s: %w", parent, err)
			}
		}

		if err := e.verifyCommitSnapshot(ctx, req, record, commit, packed); err != nil {
			return err
		}

		known[string(record.ID)] = struct{}{}
		previous = string(record.ID)
	}

	return nil
}

// verifyCommitSnapshot resolves commit's snapshot object — preferring the
// pack just verified, falling back to previously indexed objects for a
// snapshot reused unchanged from an ancestor commit — and fully schema
// validates it via review.DecodeSnapshotObject.
func (e *Engine) verifyCommitSnapshot(ctx context.Context, req PushRequest, record CommitRecord, commit graphcontract.Commit, packed map[graphcontract.ObjectID]packedObject) error {
	object, ok := packed[commit.Snapshot]
	var (
		objectType string
		data       []byte
	)
	if ok {
		objectType, data = object.objectType, object.data
	} else {
		resolvedType, resolvedData, err := e.indexer.ResolveObject(ctx, req.TenantID, req.RepoID, commit.Snapshot)
		if err != nil {
			if errors.Is(err, nativeindex.ErrObjectNotFound) {
				return fmt.Errorf("%w: commit %s snapshot %s is not available", ErrDisconnectedPush, record.ID, commit.Snapshot)
			}
			return fmt.Errorf("nativepush: resolve snapshot %s: %w", commit.Snapshot, err)
		}
		objectType, data = resolvedType, resolvedData
	}
	if objectType != ObjectTypeSnapshot {
		return fmt.Errorf("%w: object %s has type %q, want %q", ErrInvalidNativePush, commit.Snapshot, objectType, ObjectTypeSnapshot)
	}
	if _, err := review.DecodeSnapshotObject(data); err != nil {
		return fmt.Errorf("%w: commit %s snapshot %s: %w", ErrInvalidNativePush, record.ID, commit.Snapshot, err)
	}
	return nil
}

func (e *Engine) diagnoseNonFastForward(ctx context.Context, req PushRequest, actualHead string) error {
	isAncestor, err := e.store.IsAncestor(ctx, req.RepoID, req.BaseCommit, actualHead)
	if err != nil {
		return fmt.Errorf("nativepush: check branch ancestry: %w", err)
	}
	if isAncestor {
		return &serversync.NonFastForwardError{
			ActualHead: actualHead,
			Guidance:   "remote branch has advanced since your base commit; pull the latest changes and reapply your commit before pushing again",
		}
	}
	return &serversync.NonFastForwardError{
		ActualHead: actualHead,
		Guidance:   "base commit not found in the remote branch's history; branches have diverged — pull and manually reconcile before pushing",
	}
}
