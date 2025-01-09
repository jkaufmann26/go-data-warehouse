package main

import (
	"context"
	"go-data-warehouse/internal"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func main() {
	container := mustDockerize(context.Background())
	port, err := container.Container.MappedPort(context.Background(), "5432")
	if err != nil {
		panic(err)
	}
	postgresUri := "postgres://postgres:postgres@localhost:" + port.Port() + "/datawarehouse?sslmode=disable"
	db := internal.MustOpen(postgresUri)
	_, err = db.Query("SELECT * FROM test")
	if err != nil {
		panic("Wow big fail")
	}
	container.Terminate(context.Background())
}

func mustDockerize(ctx context.Context) postgres.PostgresContainer {
	pgContainer, err := postgres.Run(ctx,
		"postgres:latest",
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithDatabase("datawarehouse"),
		postgres.WithInitScripts(
			"/Users/jkaufmann/Desktop/sbc-microservices/go-data-warehouse/init/1_init.up.sql",
		),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)

	if err != nil {
		panic(err)
	}
	return *pgContainer
}
