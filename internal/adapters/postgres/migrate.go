package postgres

import (
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // driver registration
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func newMigrator(databaseURL string) (*migrate.Migrate, error) {
	src, err := iofs.New(migrationFiles, "migrations")
	if err != nil {
		return nil, fmt.Errorf("load migrations: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open migrator: %w", err)
	}
	return m, nil
}

// MigrateUp applies every pending migration.
func MigrateUp(databaseURL string) error {
	m, err := newMigrator(databaseURL)
	if err != nil {
		return err
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}

// MigrateDown reverts every migration (steps <= 0) or the last n.
func MigrateDown(databaseURL string, steps int) error {
	m, err := newMigrator(databaseURL)
	if err != nil {
		return err
	}
	defer m.Close()
	if steps <= 0 {
		err = m.Down()
	} else {
		err = m.Steps(-steps)
	}
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate down: %w", err)
	}
	return nil
}

// MigrationVersion returns the current schema version and dirty flag.
func MigrationVersion(databaseURL string) (uint, bool, error) {
	m, err := newMigrator(databaseURL)
	if err != nil {
		return 0, false, err
	}
	defer m.Close()
	v, dirty, err := m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		return 0, false, nil
	}
	return v, dirty, err
}
