// Package observability provides JSON logging and Prometheus metrics.
package observability

import (
	"io"
	"log/slog"
	"strings"
)

// NewLogger builds a JSON slog logger. Handlers never receive credentials or
// full financial payloads: call sites log identifiers only.
func NewLogger(w io.Writer, level string, instanceID string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})
	return slog.New(h).With("instanceId", instanceID)
}
