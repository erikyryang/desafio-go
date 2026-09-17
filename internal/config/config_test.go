package config

import "testing"

func TestLoadValidates(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("OIDC_ISSUER", "")
	if _, err := Load(); err == nil {
		t.Fatal("missing required values must fail")
	}
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("OIDC_ISSUER", "http://i")
	t.Setenv("OIDC_JWKS_URL", "http://j")
	t.Setenv("PENDING_TTL", "nope")
	if _, err := Load(); err == nil {
		t.Fatal("bad duration must fail")
	}
	t.Setenv("PENDING_TTL", "5m")
	cfg, err := Load()
	if err != nil || cfg.PendingTTL.Minutes() != 5 || cfg.HTTPAddr != ":8081" {
		t.Fatalf("load: %v %+v", err, cfg)
	}
}
