package internal

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

var (
	//go:embed migrations/*.sql
	filesSql embed.FS
)

func RunMigrations(ctx context.Context, db *sql.DB) error {
	sourceDriver, err := iofs.New(filesSql, "migrations")
	if err != nil {
		return fmt.Errorf("error creating sourceDriver for DB migrations: %w", err)
	}

	dbDriver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("error creating dbDriver for DB migrations: %w", err)
	}

	migrator, err := migrate.NewWithInstance("iofs", sourceDriver, "postgres", dbDriver)
	if err != nil {
		return fmt.Errorf("error initializing DB migrations: %w", err)
	}

	err = migrator.Up()
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}

	return nil
}

func MustRunMigrations(ctx context.Context, db *sql.DB) {
	err := RunMigrations(ctx, db)
	if err != nil {
		panic(err)
	}
}
