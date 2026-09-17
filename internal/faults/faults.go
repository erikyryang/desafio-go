// Package faults implements opt-in crash points used to demonstrate recovery
// (FAULT_INJECT=consumer-after-commit,outbox-after-publish). A triggered
// point terminates the process immediately, simulating a crash between two
// steps that must be resilient to interruption.
package faults

import (
	"log/slog"
	"os"
	"strings"
)

// Known crash points.
const (
	ConsumerAfterCommit = "consumer-after-commit" // after the inbox/domain commit, before DeleteMessage
	OutboxAfterPublish  = "outbox-after-publish"  // after SendMessage, before marking the row published
)

// Injector holds the enabled crash points.
type Injector struct {
	points map[string]bool
	log    *slog.Logger
	exit   func(int)
}

// New parses a comma-separated list of crash points.
func New(spec string, log *slog.Logger) *Injector {
	inj := &Injector{points: map[string]bool{}, log: log, exit: os.Exit}
	for _, p := range strings.Split(spec, ",") {
		if p = strings.TrimSpace(p); p != "" {
			inj.points[p] = true
		}
	}
	return inj
}

// Enabled reports whether any point is configured.
func (i *Injector) Enabled() bool { return i != nil && len(i.points) > 0 }

// Crash terminates the process when point is enabled.
func (i *Injector) Crash(point string) {
	if i == nil || !i.points[point] {
		return
	}
	i.log.Error("fault injection: crashing process", "point", point)
	i.exit(3)
}

// NewWithExit is New with a custom termination function (tests).
func NewWithExit(spec string, log *slog.Logger, exit func(int)) *Injector {
	inj := New(spec, log)
	inj.exit = exit
	return inj
}
