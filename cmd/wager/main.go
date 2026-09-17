// Command wager runs the service (default) or applies migrations.
//
//	wager                 # serve
//	wager migrate up      # apply all migrations
//	wager migrate down    # revert all migrations
//	wager migrate down 1  # revert the last migration
//	wager migrate version # print schema version
package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/erikyryan/desafio-go/internal/adapters/postgres"
	"github.com/erikyryan/desafio-go/internal/config"
	"github.com/erikyryan/desafio-go/internal/fxapp"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		os.Exit(runMigrate(os.Args[2:]))
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		os.Exit(2)
	}
	fxapp.New(cfg).Run()
}

func runMigrate(args []string) int {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		fmt.Fprintln(os.Stderr, "DATABASE_URL is required")
		return 2
	}
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: wager migrate up|down [steps]|version")
		return 2
	}
	var err error
	switch args[0] {
	case "up":
		err = postgres.MigrateUp(url)
	case "down":
		steps := 0
		if len(args) > 1 {
			if steps, err = strconv.Atoi(args[1]); err != nil {
				fmt.Fprintln(os.Stderr, "steps must be an integer")
				return 2
			}
		}
		err = postgres.MigrateDown(url, steps)
	case "version":
		v, dirty, verr := postgres.MigrationVersion(url)
		if verr != nil {
			err = verr
			break
		}
		fmt.Printf("version=%d dirty=%v\n", v, dirty)
	default:
		fmt.Fprintln(os.Stderr, "usage: wager migrate up|down [steps]|version")
		return 2
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Println("ok")
	return 0
}
