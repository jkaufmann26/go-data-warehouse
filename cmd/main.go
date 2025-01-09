package main

import (
	"context"
	"fmt"
	"go-data-warehouse/internal/ecommerce"
	"go-data-warehouse/internal/psql"
	"os"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func main() {
	// initializing docker container and fetching port it booted on
	container := mustDockerize(context.Background())
	port, err := container.Container.MappedPort(context.Background(), "5432")
	if err != nil {
		panic(err)
	}

	// connecting to db
	postgresUri := "postgres://postgres:postgres@localhost:" + port.Port() + "/datawarehouse?sslmode=disable"
	db := psql.MustOpen(postgresUri)

	store := ecommerce.NewDataStore(db)

	// initializng a new data ingestion service with a db
	dataIngestion := ecommerce.NewDataIngestionService(*store)

	file, err := os.Open("../data/data.csv")
	if err != nil {
		fmt.Print(" an error occurred while attempting to open the file")
	}
	defer file.Close()

	dataIngestion.IngestFile(file)

	container.Terminate(context.Background())
}

func mustDockerize(ctx context.Context) postgres.PostgresContainer {
	pgContainer, err := postgres.Run(ctx,
		"postgres:latest",
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithDatabase("datawarehouse"),
		postgres.WithInitScripts(
			"/Users/jkaufmann/Desktop/sbc-microservices/go-data-warehouse/migrations/1_init.up.sql",
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
