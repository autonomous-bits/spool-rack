package gateway

import (
	"os"
	"strconv"

	"github.com/autonomous-bits/spool/graphcontract"
)

// VersionWindow describes the inclusive range of a graphcontract wire-format
// version a Rack deployment currently accepts. Max is always the version this
// binary implements; Min defaults to Max (no widening) but can be lowered for
// the duration of a rolling deploy so the immediately-prior compatible client
// version keeps working until operators confirm it has been retired — see
// spec-cli-rack-contract-and-metadata-migrations and
// std-non-breaking-evolution-and-version-compatibility.
type VersionWindow struct {
	Min uint32
	Max uint32
}

// Accepts reports whether v falls inside the accepted window. A zero version
// is treated as "unspecified" and always accepted, preserving compatibility
// with callers that predate version negotiation entirely.
func (w VersionWindow) Accepts(v uint32) bool {
	if v == 0 {
		return true
	}
	return v >= w.Min && v <= w.Max
}

// envVersionWindow builds a VersionWindow whose Max is the current
// graphcontract format version and whose Min is read from the named
// environment variable (falling back to Max, i.e. no widening, when unset or
// invalid). This lets operators widen the accepted window for a rolling
// deploy purely through configuration, without a code change or rebuild.
func envVersionWindow(minEnvVar string, current uint32) VersionWindow {
	window := VersionWindow{Min: current, Max: current}
	raw := os.Getenv(minEnvVar)
	if raw == "" {
		return window
	}
	parsed, err := strconv.ParseUint(raw, 10, 32)
	if err != nil || uint32(parsed) > current {
		return window
	}
	window.Min = uint32(parsed)
	return window
}

// WithPackFormatWindow overrides the accepted graphcontract pack format
// version window (defaults to the env-configured window; see
// envVersionWindow). Primarily useful for tests that want a deterministic
// window without setting environment variables.
func WithPackFormatWindow(window VersionWindow) Option {
	return func(g *Gateway) { g.packFormatWindow = window }
}

// WithPackIndexFormatWindow overrides the accepted graphcontract pack index
// format version window.
func WithPackIndexFormatWindow(window VersionWindow) Option {
	return func(g *Gateway) { g.packIndexFormatWindow = window }
}

// WithPackManifestFormatWindow overrides the accepted graphcontract pack
// manifest format version window.
func WithPackManifestFormatWindow(window VersionWindow) Option {
	return func(g *Gateway) { g.packManifestFormatWindow = window }
}

func defaultPackFormatWindow() VersionWindow {
	return envVersionWindow("RACK_MIN_PACK_FORMAT_VERSION", graphcontract.PackFormatVersion)
}

func defaultPackIndexFormatWindow() VersionWindow {
	return envVersionWindow("RACK_MIN_PACK_INDEX_FORMAT_VERSION", graphcontract.PackIndexFormatVersion)
}

func defaultPackManifestFormatWindow() VersionWindow {
	return envVersionWindow("RACK_MIN_PACK_MANIFEST_FORMAT_VERSION", graphcontract.PackManifestFormatVersion)
}
