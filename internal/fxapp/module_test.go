package fxapp

import (
	"testing"

	"go.uber.org/fx"

	"github.com/erikyryan/desafio-go/internal/config"
)

// TestGraphIsComplete validates the dependency graph without starting
// anything (no infrastructure required). Start/stop against real
// dependencies is covered by tests/integration.
func TestGraphIsComplete(t *testing.T) {
	cfg := config.Config{DatabaseURL: "postgres://x", OIDCIssuer: "http://issuer", OIDCJWKSURL: "http://jwks"}
	if err := fx.ValidateApp(fx.Supply(cfg), Module); err != nil {
		t.Fatalf("invalid graph: %v", err)
	}
}
