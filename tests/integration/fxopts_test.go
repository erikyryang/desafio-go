//go:build integration

package integration

import (
	"go.uber.org/fx"

	"github.com/erikyryan/desafio-go/internal/config"
	"github.com/erikyryan/desafio-go/internal/fxapp"
)

func fxappOptions(cfg config.Config) []fx.Option {
	return []fx.Option{fx.Supply(cfg), fxapp.Module}
}
