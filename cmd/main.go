package main

import (
	"context"
	"fmt"
	"go-data-warehouse/internal"
	"go-data-warehouse/internal/ecommerce"
	"go-data-warehouse/internal/psql"
	"os"
	"time"
)

func main() {
	// initializing docker container and fetching port it booted on

	// connecting to db
	postgresUri := "postgres://postgres:postgres@localhost:" + "5432" + "/datawarehouse?sslmode=disable"
	db := psql.MustOpen(postgresUri)

	db.SetMaxIdleConns(2)
	db.SetMaxOpenConns(100)
	// run migrations
	internal.MustRunMigrations(context.Background(), db)

	store := ecommerce.NewDataStore(db)

	// initializng a new data ingestion service with a db
	dataIngestion := ecommerce.NewDataIngestionService(*store)

	file, err := os.Open("../data/data.csv")
	if err != nil {
		fmt.Print(" an error occurred while attempting to open the file")
	}
	defer file.Close()

	start := time.Now()
	dataIngestion.IngestFile(file)
	jobLength := time.Since(start)
	fmt.Println("Jobs Done. Time taken: " + jobLength.String())
}
