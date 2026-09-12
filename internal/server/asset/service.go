package asset

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/autonomous-bits/spool-rack/internal/server/storage/cas"
	"github.com/autonomous-bits/spool-rack/internal/server/storage/postgres"
)

var (
	// ErrInvalidHash reports that an asset hash is not well-formed.
	ErrInvalidHash = errors.New("asset: invalid hash")
	// ErrMissingHash reports that a required asset hash was empty.
	ErrMissingHash = errors.New("asset: hash is required")
	// ErrNotFound reports that the requested asset was not found.
	ErrNotFound = errors.New("asset: not found")
	// ErrQuotaExceeded reports that an asset upload exceeds the tenant storage quota.
	ErrQuotaExceeded = postgres.ErrQuotaExceeded
)

// Store defines the metadata persistence interface required by the Asset service.
type Store interface {
	AdmitAssetUpload(ctx context.Context, repoID, hash string, sizeBytes int64) error
	RegisterAsset(ctx context.Context, repoID, hash string, sizeBytes int64, mimeType string) error
	GetAssetMetadata(ctx context.Context, repoID, hash string) (postgres.AssetRecord, error)
	ListMissingAssets(ctx context.Context, repoID string, candidateHashes []string) ([]string, error)
}

// NegotiationRequest specifies candidate asset hashes and optional sizes for pre-flight check.
type NegotiationRequest struct {
	Hashes []string         `json:"hashes"`
	Sizes  map[string]int64 `json:"sizes,omitempty"`
}

// NegotiationResult contains the outcome of pre-flight asset negotiation.
type NegotiationResult struct {
	Missing          []string          `json:"missing"`
	Existing         []string          `json:"existing"`
	UploadAuthorized bool              `json:"uploadAuthorized"`
	Tokens           map[string]string `json:"tokens,omitempty"`
}

// Service manages remote asset storage, pre-push negotiation, streaming upload,
// and retrieval.
type Service struct {
	casDriver cas.Driver
	store     Store
}

// NewService constructs a Service backed by a CAS driver and Postgres metadata store.
func NewService(casDriver cas.Driver, store Store) *Service {
	return &Service{
		casDriver: casDriver,
		store:     store,
	}
}

// Negotiate checks candidate asset hashes against existing metadata and storage quotas,
// returning the list of missing hashes that must be uploaded.
func (s *Service) Negotiate(ctx context.Context, scope cas.Scope, req NegotiationRequest) (NegotiationResult, error) {
	if s.store == nil {
		return NegotiationResult{}, errors.New("asset service: store is not configured")
	}

	normalized := make([]string, 0, len(req.Hashes))
	for _, h := range req.Hashes {
		trimmed := strings.ToLower(strings.TrimSpace(h))
		if trimmed != "" {
			normalized = append(normalized, trimmed)
		}
	}

	missing, err := s.store.ListMissingAssets(ctx, scope.RepoID(), normalized)
	if err != nil {
		return NegotiationResult{}, fmt.Errorf("negotiate assets: %w", err)
	}

	existingMap := make(map[string]struct{})
	for _, h := range normalized {
		existingMap[h] = struct{}{}
	}
	for _, m := range missing {
		delete(existingMap, m)
	}

	existing := make([]string, 0, len(existingMap))
	for h := range existingMap {
		existing = append(existing, h)
	}

	// Check quota admission for missing assets if sizes were provided
	if req.Sizes != nil {
		for _, m := range missing {
			if sz, ok := req.Sizes[m]; ok && sz > 0 {
				if err := s.store.AdmitAssetUpload(ctx, scope.RepoID(), m, sz); err != nil {
					return NegotiationResult{}, fmt.Errorf("admit asset %s: %w", m, err)
				}
			}
		}
	}

	if missing == nil {
		missing = []string{}
	}
	if existing == nil {
		existing = []string{}
	}

	return NegotiationResult{
		Missing:          missing,
		Existing:         existing,
		UploadAuthorized: true,
	}, nil
}

// Upload admits, stores, and registers an asset blob stream under scope.
func (s *Service) Upload(ctx context.Context, scope cas.Scope, hash string, size int64, mimeType string, r io.Reader) (int64, error) {
	if s.casDriver == nil || s.store == nil {
		return 0, errors.New("asset service: not configured")
	}
	cleanHash := strings.ToLower(strings.TrimSpace(hash))
	if cleanHash == "" {
		return 0, ErrMissingHash
	}

	// 1. Quota admission check
	if size > 0 {
		if err := s.store.AdmitAssetUpload(ctx, scope.RepoID(), cleanHash, size); err != nil {
			return 0, fmt.Errorf("admit asset upload: %w", err)
		}
	}

	// 2. Stream blob into tenant-partitioned CAS
	written, err := s.casDriver.WriteAsset(ctx, scope, cleanHash, r)
	if err != nil {
		return 0, fmt.Errorf("write asset to cas: %w", err)
	}

	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	// 3. Register asset metadata in Postgres
	if err := s.store.RegisterAsset(ctx, scope.RepoID(), cleanHash, written, mimeType); err != nil {
		return written, fmt.Errorf("register asset metadata: %w", err)
	}

	return written, nil
}

// Open retrieves an asset blob reader, its byte size, and MIME type.
func (s *Service) Open(ctx context.Context, scope cas.Scope, hash string) (io.ReadCloser, int64, string, error) {
	if s.casDriver == nil {
		return nil, 0, "", errors.New("asset service: cas driver not configured")
	}
	cleanHash := strings.ToLower(strings.TrimSpace(hash))
	if cleanHash == "" {
		return nil, 0, "", ErrMissingHash
	}

	var mimeType string = "application/octet-stream"
	if s.store != nil {
		if rec, err := s.store.GetAssetMetadata(ctx, scope.RepoID(), cleanHash); err == nil {
			if rec.MIMEType != "" {
				mimeType = rec.MIMEType
			}
		}
	}

	rc, size, err := s.casDriver.OpenAsset(ctx, scope, cleanHash)
	if err != nil {
		if errors.Is(err, cas.ErrNotFound) {
			return nil, 0, "", ErrNotFound
		}
		return nil, 0, "", fmt.Errorf("open asset: %w", err)
	}

	return rc, size, mimeType, nil
}
