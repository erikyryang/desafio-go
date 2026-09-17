// Package auth validates OAuth 2.0 / OIDC access tokens issued by an external
// IdP (Keycloak in the local environment) and derives the caller's identity.
package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"

	"github.com/erikyryan/desafio-go/internal/app"
)

// Roles are realm roles carried in realm_access.roles.
const (
	RoleProvider = "wager:provider"
	RoleInternal = "wager:internal"
)

var (
	ErrMissingToken = errors.New("auth: missing bearer token")
	ErrInvalidToken = errors.New("auth: invalid token")
)

// Identity is the authenticated principal.
type Identity struct {
	Subject    string
	ClientID   string
	ProviderID string // from the providerId claim, only for provider clients
	Roles      []string
}

// HasRole reports whether the identity carries a realm role.
func (i Identity) HasRole(role string) bool {
	for _, r := range i.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// IsInternal reports whether the identity is the internal service.
func (i Identity) IsInternal() bool { return i.HasRole(RoleInternal) }

// IsProvider reports whether the identity is a game provider with a providerId.
func (i Identity) IsProvider() bool { return i.HasRole(RoleProvider) && i.ProviderID != "" }

// Actor converts the identity into the application authorization view.
func (i Identity) Actor() app.Actor {
	a := app.Actor{Internal: i.IsInternal()}
	if i.IsProvider() {
		a.ProviderID = i.ProviderID
	}
	return a
}

// Config of the verifier.
type Config struct {
	// Issuer expected in the "iss" claim, e.g. http://localhost:8080/realms/wager.
	Issuer string
	// JWKSURL is fetched to obtain signing keys; it may differ from the issuer
	// host when the service reaches the IdP through an internal network.
	JWKSURL string
	// Audience expected in "aud"; empty disables the check.
	Audience string
}

// Verifier validates tokens against the IdP keys (cached, auto-refreshed).
type Verifier struct {
	verifier *oidc.IDTokenVerifier
}

// NewVerifier builds the verifier. Keys are fetched lazily on first use and
// refreshed when an unknown key id is seen.
func NewVerifier(ctx context.Context, cfg Config, client *http.Client) (*Verifier, error) {
	if cfg.Issuer == "" || cfg.JWKSURL == "" {
		return nil, errors.New("auth: issuer and jwks url are required")
	}
	if client != nil {
		ctx = oidc.ClientContext(ctx, client)
	}
	keySet := oidc.NewRemoteKeySet(ctx, cfg.JWKSURL)
	oc := &oidc.Config{ClientID: cfg.Audience, SkipClientIDCheck: cfg.Audience == ""}
	return &Verifier{verifier: oidc.NewVerifier(cfg.Issuer, keySet, oc)}, nil
}

type claims struct {
	Subject         string `json:"sub"`
	AuthorizedParty string `json:"azp"`
	ClientID        string `json:"client_id"`
	ProviderID      string `json:"providerId"`
	RealmAccess     struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

// Verify checks signature, issuer, audience and expiry and extracts the identity.
func (v *Verifier) Verify(ctx context.Context, rawToken string) (Identity, error) {
	if rawToken == "" {
		return Identity{}, ErrMissingToken
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	token, err := v.verifier.Verify(ctx, rawToken)
	if err != nil {
		return Identity{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	var c claims
	if err := token.Claims(&c); err != nil {
		return Identity{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	clientID := c.AuthorizedParty
	if clientID == "" {
		clientID = c.ClientID
	}
	return Identity{Subject: c.Subject, ClientID: clientID, ProviderID: c.ProviderID, Roles: c.RealmAccess.Roles}, nil
}
