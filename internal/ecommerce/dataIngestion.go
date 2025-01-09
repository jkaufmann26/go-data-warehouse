package ecommerce

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type DataIngestionService struct {
	db *sqlx.DB
}

func NewDataIngestionService(db *sql.DB) *DataIngestionService {
	return &DataIngestionService{
		db: sqlx.NewDb(db, "postgres"),
	}
}
