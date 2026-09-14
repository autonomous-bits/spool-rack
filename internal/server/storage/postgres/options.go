package postgres

import (
	"time"
)

// Option configures an Open call for PGStore.
type Option func(*openOptions)

type openOptions struct {
	tokenProvider   TokenProvider
	user            string
	maxConnLifetime time.Duration
}

// WithTokenProvider configures dynamic token acquisition (e.g. for Azure Managed Identity).
func WithTokenProvider(tp TokenProvider) Option {
	return func(o *openOptions) {
		o.tokenProvider = tp
	}
}

// WithUser overrides or explicitly sets the database username for connections.
func WithUser(user string) Option {
	return func(o *openOptions) {
		o.user = user
	}
}

// WithMaxConnLifetime sets the maximum connection lifetime for pooled connections.
func WithMaxConnLifetime(d time.Duration) Option {
	return func(o *openOptions) {
		o.maxConnLifetime = d
	}
}

// MigrateOption configures a Migrate call.
type MigrateOption func(*migrateOptions)

type migrateOptions struct {
	tokenProvider TokenProvider
	user          string
}

// WithMigrateTokenProvider configures dynamic token acquisition for migrations.
func WithMigrateTokenProvider(tp TokenProvider) MigrateOption {
	return func(o *migrateOptions) {
		o.tokenProvider = tp
	}
}

// WithMigrateUser overrides or explicitly sets the database username for migrations.
func WithMigrateUser(user string) MigrateOption {
	return func(o *migrateOptions) {
		o.user = user
	}
}
